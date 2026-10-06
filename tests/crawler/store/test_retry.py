"""
Tests for BaseCrawler's request pacing, retries and block detection.

No network and no fixtures: requests are served by httpx.MockTransport and
time.sleep is replaced with a recorder, so a test that asserts a 24-second
backoff still runs instantly.
"""

import datetime
from tempfile import NamedTemporaryFile
from urllib.parse import quote

import httpx
import pytest

from crawler.store import base
from crawler.store.base import BaseCrawler, CrawlerBlocked
from crawler.store.konzum import KonzumCrawler
from crawler.store.lidl import LidlCrawler


class _Crawler(BaseCrawler):
    """Minimal concrete crawler; the field maps are unused by these tests."""

    CHAIN = "test"
    BASE_URL = "https://example.test"
    PRICE_MAP = {}
    FIELD_MAP = {}
    REQUIRED_COLUMNS = []

    RETRY_BACKOFF_BASE = 1.0


@pytest.fixture
def sleeps(monkeypatch) -> list[float]:
    """
    Record sleep durations instead of waiting, for base and konzum alike.

    The monotonic clock is faked alongside sleep and advances only when the code
    sleeps. Without that, pacing cannot be tested at all: a mocked sleep leaves
    the real clock where it was, so _throttle always believes no time has passed
    and charges the full delay even when backoff has already covered it.
    """
    recorded: list[float] = []
    now = [1000.0]

    def fake_sleep(seconds: float) -> None:
        recorded.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(base, "sleep", fake_sleep)
    monkeypatch.setattr(base, "monotonic", lambda: now[0])
    monkeypatch.setattr("crawler.store.konzum.time.sleep", fake_sleep)
    return recorded


@pytest.fixture(autouse=True)
def no_jitter(monkeypatch):
    """Pin the jitter multiplier so backoff values are exact."""
    monkeypatch.setattr(base, "uniform", lambda _lo, _hi: 1.0)


def make_crawler(
    responses: list[httpx.Response] | dict[str, list[httpx.Response]],
    crawler_class: type[BaseCrawler] = _Crawler,
    **attrs,
) -> tuple[BaseCrawler, list[str]]:
    """
    Build a crawler whose client replays canned responses, plus a request log.

    Args:
        responses: Responses in order, or per-path queues keyed by URL path.
        crawler_class: Crawler to instantiate.
        attrs: Class attributes to override on the instance, e.g. REQUEST_DELAY.
    """
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if isinstance(responses, dict):
            queue = responses[request.url.path]
        else:
            queue = responses
        if not queue:
            raise AssertionError(f"Unexpected extra request to {request.url}")
        return queue.pop(0)

    crawler = crawler_class()
    crawler.client = httpx.Client(transport=httpx.MockTransport(handler))
    for name, value in attrs.items():
        setattr(crawler, name, value)
    return crawler, requested


def test_retries_a_transient_status_then_succeeds(sleeps):
    crawler, requested = make_crawler(
        [httpx.Response(503), httpx.Response(200, text="price;list")]
    )

    assert crawler.fetch_text("https://example.test/a.csv") == "price;list"
    assert len(requested) == 2
    assert sleeps == [1.0]


def test_does_not_retry_404_by_default(sleeps):
    """For most chains a 404 means "not published yet"; retrying buys nothing."""
    crawler, requested = make_crawler([httpx.Response(404)])

    with pytest.raises(httpx.HTTPStatusError):
        crawler.fetch_text("https://example.test/a.csv")

    assert len(requested) == 1
    assert sleeps == []


def test_konzum_opts_into_retrying_404(sleeps):
    """Konzum 404s price lists its own index is advertising; a retry recovers."""
    crawler, requested = make_crawler(
        [httpx.Response(404), httpx.Response(200, text="price;list")],
        crawler_class=KonzumCrawler,
    )

    assert crawler.fetch_text("https://example.test/a.csv") == "price;list"
    assert len(requested) == 2


def test_gives_up_after_max_retries(sleeps):
    crawler, requested = make_crawler([httpx.Response(503) for _ in range(6)])

    with pytest.raises(httpx.HTTPStatusError):
        crawler.fetch_text("https://example.test/a.csv")

    assert len(requested) == crawler.MAX_RETRIES == 6
    # One sleep fewer than attempts: nothing waits after the last failure.
    assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_backoff_is_capped(sleeps):
    crawler, _ = make_crawler(
        [httpx.Response(500) for _ in range(8)], MAX_RETRIES=8, RETRY_BACKOFF_MAX=10.0
    )

    with pytest.raises(httpx.HTTPStatusError):
        crawler.fetch_text("https://example.test/a.csv")

    assert sleeps == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0, 10.0]


def test_attempts_override_wins_over_max_retries(sleeps):
    """Konzum relies on this to keep per-URL retries shallow."""
    crawler, requested = make_crawler([httpx.Response(503), httpx.Response(503)])

    with pytest.raises(httpx.HTTPStatusError):
        crawler.fetch_text("https://example.test/a.csv", attempts=2)

    assert len(requested) == 2


def test_does_not_retry_other_client_errors(sleeps):
    """A 400 is our fault; repeating it just wastes the pacing budget."""
    crawler, requested = make_crawler([httpx.Response(400)])

    with pytest.raises(httpx.HTTPStatusError):
        crawler.fetch_text("https://example.test/a.csv")

    assert len(requested) == 1
    assert sleeps == []


def test_retries_transport_errors(sleeps):
    """Connection drops are the failure Lidl's deleted loop existed for."""
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise httpx.ConnectError("connection reset", request=request)
        return httpx.Response(200, text="ok")

    crawler = _Crawler()
    crawler.client = httpx.Client(transport=httpx.MockTransport(handler))

    assert crawler.fetch_text("https://example.test/a.csv") == "ok"
    assert attempts["n"] == 2


class TestBlocked:
    def test_403_raises_immediately_without_retrying(self, sleeps):
        crawler, requested = make_crawler([httpx.Response(403)])

        with pytest.raises(CrawlerBlocked):
            crawler.fetch_text("https://example.test/a.csv")

        assert len(requested) == 1
        assert sleeps == []
        assert crawler.blocked is True

    def test_no_further_request_is_made_once_blocked(self, sleeps):
        """
        The flag is what bounds the damage: crawlers catch Exception per store,
        so without it a ban would cost one 403 per remaining store.
        """
        crawler, requested = make_crawler([httpx.Response(403)])

        with pytest.raises(CrawlerBlocked):
            crawler.fetch_text("https://example.test/a.csv")

        for _ in range(3):
            with pytest.raises(CrawlerBlocked):
                crawler.fetch_text("https://example.test/b.csv")

        assert len(requested) == 1


class TestRetryAfter:
    def test_honours_retry_after_seconds(self, sleeps):
        crawler, _ = make_crawler(
            [
                httpx.Response(429, headers={"Retry-After": "5"}),
                httpx.Response(200, text="ok"),
            ]
        )

        assert crawler.fetch_text("https://example.test/a.csv") == "ok"
        assert sleeps == [5.0]

    def test_retry_after_never_shortens_the_backoff(self, sleeps):
        """
        Obeying `Retry-After: 0` literally would fire the remaining attempts back
        to back at a server that has just rate-limited us.
        """
        crawler, _ = make_crawler(
            [
                httpx.Response(429, headers={"Retry-After": "0"}),
                httpx.Response(200, text="ok"),
            ]
        )

        assert crawler.fetch_text("https://example.test/a.csv") == "ok"
        assert sleeps == [1.0]

    def test_honours_retry_after_http_date(self, sleeps):
        target = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(
            seconds=30
        )
        stamp = target.strftime("%a, %d %b %Y %H:%M:%S GMT")
        crawler, _ = make_crawler(
            [
                httpx.Response(429, headers={"Retry-After": stamp}),
                httpx.Response(200, text="ok"),
            ]
        )

        assert crawler.fetch_text("https://example.test/a.csv") == "ok"
        assert len(sleeps) == 1
        assert 25 <= sleeps[0] <= 30

    def test_abandons_the_chain_when_retry_after_exceeds_cap(self, sleeps):
        """
        Giving up on the one request would let the caller move to the next store
        and hit the same endpoint a REQUEST_DELAY later, ignoring the header.
        """
        crawler, requested = make_crawler(
            [httpx.Response(429, headers={"Retry-After": "9999"})]
        )

        with pytest.raises(CrawlerBlocked):
            crawler.fetch_text("https://example.test/a.csv")

        assert len(requested) == 1
        assert sleeps == []
        assert crawler.blocked is True

    def test_cooldown_outlives_the_fetch_that_earned_it(self, sleeps):
        """
        The bug this covers: a Retry-After honoured only inside one _with_retry
        call is nearly useless, because crawlers catch per store and move on.
        """
        crawler, requested = make_crawler(
            {
                "/a.csv": [httpx.Response(429, headers={"Retry-After": "60"})],
                "/b.csv": [httpx.Response(200, text="ok")],
            },
            MAX_RETRIES=1,  # give up immediately, so nothing is slept in-call
            REQUEST_DELAY=2.0,
        )

        with pytest.raises(httpx.HTTPStatusError):
            crawler.fetch_text("https://example.test/a.csv")
        assert sleeps == []

        # A different URL on the same host must still wait out the 60s, not the
        # 2s pacing interval.
        assert crawler.fetch_text("https://example.test/b.csv") == "ok"
        assert sleeps == [60.0]
        assert len(requested) == 2

    def test_cooldown_is_not_waited_twice(self, sleeps):
        """The in-call sleep already satisfies the cooldown it recorded."""
        crawler, _ = make_crawler(
            {
                "/a.csv": [
                    httpx.Response(429, headers={"Retry-After": "10"}),
                    httpx.Response(200, text="ok"),
                ]
            }
        )

        assert crawler.fetch_text("https://example.test/a.csv") == "ok"
        assert sleeps == [10.0]

    def test_cooldown_is_per_host(self, sleeps):
        crawler, _ = make_crawler(
            {
                "/a.csv": [httpx.Response(429, headers={"Retry-After": "60"})],
                "/b.csv": [httpx.Response(200, text="ok")],
            },
            MAX_RETRIES=1,
        )

        with pytest.raises(httpx.HTTPStatusError):
            crawler.fetch_text("https://one.test/a.csv")

        assert crawler.fetch_text("https://two.test/b.csv") == "ok"
        assert sleeps == []

    def test_falls_back_to_backoff_when_unparseable(self, sleeps):
        """A junk header should not be treated as "give up"."""
        crawler, requested = make_crawler(
            [
                httpx.Response(429, headers={"Retry-After": "soon"}),
                httpx.Response(200, text="ok"),
            ]
        )

        assert crawler.fetch_text("https://example.test/a.csv") == "ok"
        assert len(requested) == 2
        assert sleeps == [1.0]

    def test_ignores_retry_after_on_other_statuses(self, sleeps):
        """Only 429 carries a Retry-After we act on."""
        crawler, _ = make_crawler(
            [
                httpx.Response(503, headers={"Retry-After": "42"}),
                httpx.Response(200, text="ok"),
            ]
        )

        assert crawler.fetch_text("https://example.test/a.csv") == "ok"
        assert sleeps == [1.0]


class TestPacing:
    def test_no_delay_by_default(self, sleeps):
        """The default must not slow down the chains that work today."""
        crawler, _ = make_crawler([httpx.Response(200, text="ok") for _ in range(3)])

        for _ in range(3):
            crawler.fetch_text("https://example.test/a.csv")

        assert crawler.REQUEST_DELAY == 0.0
        assert sleeps == []

    def test_paces_requests_to_the_same_host(self, sleeps):
        crawler, _ = make_crawler(
            [httpx.Response(200, text="ok") for _ in range(3)], REQUEST_DELAY=0.5
        )

        for _ in range(3):
            crawler.fetch_text("https://example.test/a.csv")

        # Nothing to wait for before the first request.
        assert sleeps == [0.5, 0.5]

    def test_does_not_pace_across_hosts(self, sleeps):
        """
        Several chains read the index from one host and price lists from
        another; a chain-wide clock would charge them twice.
        """
        crawler, _ = make_crawler(
            {"/a.csv": [httpx.Response(200, text="ok")] for _ in range(1)}
            | {"/b.csv": [httpx.Response(200, text="ok")]},
            REQUEST_DELAY=0.5,
        )

        crawler.fetch_text("https://one.test/a.csv")
        crawler.fetch_text("https://two.test/b.csv")

        assert sleeps == []

    def test_retry_wait_is_max_not_sum(self, sleeps):
        """
        Backoff and pacing must not stack. The throttle measures from the last
        request, so a 4s backoff already satisfies a 2s pacing interval.
        """
        crawler, _ = make_crawler(
            [httpx.Response(500), httpx.Response(500), httpx.Response(200, text="ok")],
            REQUEST_DELAY=2.0,
        )

        assert crawler.fetch_text("https://example.test/a.csv") == "ok"

        # Attempt 2: 1.0s backoff, then the throttle tops it up to the 2.0s
        # pacing interval. Attempt 3: 2.0s backoff already covers it, so the
        # throttle adds nothing and does not sleep at all.
        assert sleeps == [1.0, 1.0, 2.0]

        # Waited 4s in total. max() per attempt is 2 + 2; additive would have
        # been (2 + 1) + (2 + 2) = 7.
        assert sum(sleeps) == 4.0


def test_fetch_binary_truncates_between_attempts(sleeps):
    """
    A dropped download leaves partial bytes in the caller's file. Without a
    reset the retry appends to them and yields a corrupt archive.
    """
    responses = [
        httpx.Response(200, content=b"partial-junk"),
        httpx.Response(200, content=b"good-content"),
    ]
    first = {"done": False}

    def handler(request: httpx.Request) -> httpx.Response:
        if not first["done"]:
            first["done"] = True
            raise httpx.ReadError("connection reset", request=request)
        return responses[1]

    crawler = _Crawler()
    crawler.client = httpx.Client(transport=httpx.MockTransport(handler))

    with NamedTemporaryFile(mode="w+b") as fp:
        fp.write(b"stale-bytes-from-a-previous-attempt")
        crawler.fetch_binary("https://example.test/a.zip", fp)
        fp.seek(0)
        assert fp.read() == b"good-content"


def test_requests_made_counts_every_attempt(sleeps):
    """Konzum's request budget is spent against this counter."""
    crawler, _ = make_crawler(
        [httpx.Response(503), httpx.Response(503), httpx.Response(200, text="ok")]
    )

    crawler.fetch_text("https://example.test/a.csv")
    assert crawler.requests_made == 3


class TestKonzumIndex:
    """
    One unreadable index page used to raise out of get_all_products and publish
    the day with zero Konzum stores.
    """

    @staticmethod
    def _page(n: int) -> str:
        links = "".join(
            f"<a format='csv' href='/cjenici/download?title=S{n}-{i}'>x</a>"
            for i in range(2)
        )
        return f"<html>{links}</html>"

    def _crawler(self, pages: dict[int, list[httpx.Response]], attempts: int = 6):
        def handler(request: httpx.Request) -> httpx.Response:
            page = int(request.url.params["page"])
            return pages[page].pop(0)

        crawler = KonzumCrawler()
        crawler.client = httpx.Client(transport=httpx.MockTransport(handler))
        crawler.REQUEST_DELAY = 0.0
        crawler.MAX_RETRIES = attempts
        return crawler

    def test_a_failed_page_does_not_end_pagination(self, sleeps):
        """
        The loop stops on an empty page, so a failure must not be confused with
        one -- otherwise the chain truncates at the first bad page.
        """
        crawler = self._crawler(
            {
                1: [httpx.Response(200, text=self._page(1))],
                2: [httpx.Response(500) for _ in range(6)],
                3: [httpx.Response(200, text=self._page(3))],
                4: [httpx.Response(200, text="<html></html>")],
            }
        )

        urls = crawler.get_index(datetime.date(2026, 9, 25))

        assert len(urls) == 4
        assert all("S2-" not in url for url in urls)

    def test_raises_when_no_page_can_be_read(self, sleeps):
        crawler = self._crawler(
            {page: [httpx.Response(500)] for page in range(1, 10)}, attempts=1
        )

        with pytest.raises(ValueError, match="No index page could be read"):
            crawler.get_index(datetime.date(2026, 9, 25))

    def test_a_block_is_not_swallowed(self, sleeps):
        """Continuing past a 403 would just extend the ban."""
        crawler = self._crawler({1: [httpx.Response(403)]})

        with pytest.raises(CrawlerBlocked):
            crawler.get_index(datetime.date(2026, 9, 25))


class TestKonzumSweep:
    """
    Konzum's price lists fail at random per request, so recovery comes from
    repeated passes over what is missing rather than deeper per-URL retries.
    """

    STORE_TITLE = "SUPERMARKET,ILICA 298A 10000 ZAGREB,0603,97102,25.09.2026, 05-22.CSV"

    CSV = (
        "NAZIV PROIZVODA,ŠIFRA PROIZVODA,MARKA PROIZVODA,NETO KOLIČINA,"
        "JEDINICA MJERE,BARKOD,KATEGORIJA PROIZVODA,MALOPRODAJNA CIJENA,"
        "CIJENA ZA JEDINICU MJERE,MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE,"
        "NAJNIŽA CIJENA U POSLJEDNIH 30 DANA,SIDRENA CIJENA NA 2.5.2025\n"
        "Mlijeko,123,Dukat,1,l,3850123456789,Mlijeko,1.99,1.99,,1.89,1.79\n"
    )

    def _crawler(self, csv_responses: list[httpx.Response], stores: int = 1):
        # parse_index dedupes through a set, so which of several stores is served
        # first is not fixed. Tests here must not depend on the order.
        links = "".join(
            f"<a format='csv' href='/cjenici/download?title="
            f"{self.STORE_TITLE.replace('0603', f'060{n}').replace(' ', '+').replace(',', '%2C')}"
            f"'>x</a>"
            for n in range(stores)
        )
        index = f"<html>{links}</html>"

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/cjenici":
                page = int(request.url.params["page"])
                if page == 1:
                    return httpx.Response(200, text=index)
                return httpx.Response(200, text="<html></html>")
            if not csv_responses:
                raise AssertionError("More price list requests than expected")
            return csv_responses.pop(0)

        crawler = KonzumCrawler()
        crawler.client = httpx.Client(transport=httpx.MockTransport(handler))
        crawler.REQUEST_DELAY = 0.0
        return crawler

    def test_sweep_recovers_a_price_list_the_first_pass_lost(self, sleeps):
        # Two failures exhaust CSV_ATTEMPTS in round 0; the sweep succeeds.
        crawler = self._crawler(
            [
                httpx.Response(404),
                httpx.Response(404),
                httpx.Response(200, text=self.CSV),
            ]
        )

        stores = crawler.get_all_products(datetime.date(2026, 9, 25))

        assert len(stores) == 1
        assert len(stores[0].items) == 1
        assert crawler.SWEEP_PAUSE in sleeps

    def test_stops_after_the_last_sweep(self, sleeps):
        crawler = self._crawler(
            [httpx.Response(404)]
            * (crawler_attempts := KonzumCrawler.CSV_ATTEMPTS)
            * (KonzumCrawler.SWEEP_ROUNDS + 1)
        )
        assert crawler_attempts == 2

        stores = crawler.get_all_products(datetime.date(2026, 9, 25))

        assert stores == []

    def test_a_block_stops_the_sweep_without_losing_stores(self, sleeps):
        """
        A 403 at store 180 of 188 must not throw away the 179 already collected.
        Two stores here: the first succeeds, the second is blocked.
        """
        crawler = self._crawler(
            [httpx.Response(200, text=self.CSV), httpx.Response(403)], stores=2
        )

        stores = crawler.get_all_products(datetime.date(2026, 9, 25))

        assert len(stores) == 1
        assert crawler.blocked is True
        assert crawler.SWEEP_PAUSE not in sleeps

    def test_a_block_during_the_first_pass_reports_no_sweeps(self, sleeps, caplog):
        """The count is the number of sweeps run, not the round about to start."""
        crawler = self._crawler([httpx.Response(403)], stores=2)

        with caplog.at_level("ERROR"):
            crawler.get_all_products(datetime.date(2026, 9, 25))

        gave_up = [r.message for r in caplog.records if "Gave up on" in r.message]
        assert len(gave_up) == 1
        assert "after 0 sweep(s)" in gave_up[0]

    def test_running_out_of_sweeps_reports_them_all(self, sleeps, caplog):
        crawler = self._crawler(
            [httpx.Response(404)]
            * KonzumCrawler.CSV_ATTEMPTS
            * (KonzumCrawler.SWEEP_ROUNDS + 1)
        )

        with caplog.at_level("ERROR"):
            crawler.get_all_products(datetime.date(2026, 9, 25))

        gave_up = [r.message for r in caplog.records if "Gave up on" in r.message]
        assert len(gave_up) == 1
        assert f"after {KonzumCrawler.SWEEP_ROUNDS} sweep(s)" in gave_up[0]


class TestPermanentFailures:
    """
    Konzum requeues what it could not collect, so it must not requeue failures
    that cannot improve: those would spend the request budget the recoverable
    404s need.
    """

    @staticmethod
    def _ours(caplog) -> list[str]:
        """Levels of our own records; httpx logs every request at INFO."""
        return [r.levelname for r in caplog.records if r.name.startswith("crawler.")]

    def _run(self, response: httpx.Response, caplog, level: str = "INFO"):
        """Crawl one store whose price list always answers `response`."""
        crawler = TestKonzumSweep._crawler(TestKonzumSweep(), [response] * 64, stores=1)
        with caplog.at_level(level):
            stores = crawler.get_all_products(datetime.date(2026, 9, 28))
        return crawler, stores

    def test_retryable_404_is_swept_and_stays_quiet(self, sleeps, caplog):
        crawler, stores = self._run(httpx.Response(404), caplog)

        assert stores == []
        expected = (crawler.SWEEP_ROUNDS + 1) * crawler.CSV_ATTEMPTS
        assert crawler.requests_made - 2 == expected
        assert "WARNING" not in self._ours(caplog)

    def test_non_retryable_status_is_dropped_after_one_round(self, sleeps, caplog):
        """A 400 used to be re-downloaded 14 times."""
        crawler, stores = self._run(httpx.Response(400), caplog)

        assert stores == []
        # One attempt, not CSV_ATTEMPTS: the base class does not retry a 400.
        assert crawler.requests_made - 2 == 1
        assert self._ours(caplog).count("WARNING") == 1

    def test_unparseable_file_is_dropped_after_one_round(self, sleeps, caplog):
        """A format change used to produce one traceback per round."""
        crawler, stores = self._run(
            httpx.Response(200, text="WRONG;HEADERS\n1;2\n"), caplog
        )

        assert stores == []
        assert crawler.requests_made - 2 == 1
        errors = [r for r in caplog.records if r.levelname == "ERROR" and r.exc_info]
        assert len(errors) == 1

    def test_base_does_not_log_terminal_failures(self, sleeps, caplog):
        """
        The helper cannot know whether the caller will retry in a sweep, so the
        caller owns the severity and the helper stays silent on giving up.
        """
        crawler, _ = make_crawler([httpx.Response(503), httpx.Response(503)])

        with caplog.at_level("INFO"):
            with pytest.raises(httpx.HTTPStatusError):
                crawler.fetch_text("https://example.test/a.csv", attempts=2)

        assert self._ours(caplog) == []


class TestIsRetryable:
    """One definition, shared by the retry loop and by Konzum's sweep."""

    def _err(self, status: int) -> httpx.HTTPStatusError:
        request = httpx.Request("GET", "https://example.test/a.csv")
        return httpx.HTTPStatusError(
            "boom", request=request, response=httpx.Response(status, request=request)
        )

    def test_retryable_statuses(self):
        crawler = _Crawler()
        for status in (408, 425, 429, 500, 502, 503, 522):
            assert crawler.is_retryable(self._err(status)) is True, status

    def test_permanent_statuses(self):
        crawler = _Crawler()
        for status in (400, 401, 403, 404, 410, 451):
            assert crawler.is_retryable(self._err(status)) is False, status

    def test_a_chain_can_add_a_status(self):
        """Konzum's 404 override must reach the sweep as well as the retry loop."""
        crawler = KonzumCrawler()
        assert crawler.is_retryable(self._err(404)) is True
        assert crawler.is_retryable(self._err(400)) is False

    def test_transport_errors_are_retryable(self):
        crawler = _Crawler()
        request = httpx.Request("GET", "https://example.test/a.csv")
        assert crawler.is_retryable(httpx.ConnectError("x", request=request)) is True

    def test_permanent_transport_errors_are_not_retryable(self):
        """An identical request cannot get past any of these."""
        crawler = _Crawler()
        request = httpx.Request("GET", "https://example.test/a.csv")
        for err in (
            httpx.UnsupportedProtocol("x", request=request),
            httpx.LocalProtocolError("x"),
            httpx.TooManyRedirects("x", request=request),
        ):
            assert crawler.is_retryable(err) is False, type(err).__name__

    def test_a_permanent_transport_error_is_not_retried(self, sleeps):
        """The classification has to reach _handle_request_error, not just callers."""

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.LocalProtocolError("malformed request")

        crawler = _Crawler()
        crawler.client = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(httpx.LocalProtocolError):
            crawler.fetch_text("https://example.test/a.csv")

        assert crawler.requests_made == 1
        assert sleeps == []


class TestOneBadStoreDoesNotKillTheChain:
    """
    A parse failure in one store used to raise out of get_all_products and throw
    away every store already downloaded.
    """

    DATE = datetime.date(2026, 9, 24)

    HEADER = (
        "NAZIV;ŠIFRA;MARKA;NETO_KOLIČINA;JEDINICA_MJERE;MALOPRODAJNA_CIJENA;"
        "CIJENA_ZA_JEDINICU_MJERE;MPC_ZA_VRIJEME_POSEBNOG_OBLIKA_PRODAJE;"
        "NAJNIZA_CIJENA_U_POSLJ._30_DANA;BARKOD;KATEGORIJA_PROIZVODA;"
        "Sidrena_cijena_na_02.05.2025"
    )
    ROW = "Mlijeko;1234;Dukat;1;L;1,29;1,29;;1,19;3850123456789;Mliječni;1,25"

    def _filename(self, store_id: int) -> str:
        return (
            f"Supermarket {store_id}_Ulica Dr. Franje Tudmana_30_10450_"
            f"Jastrebarsko_1_24.09.2026_7.15h.csv"
        )

    def _crawler(self, first_csv: str) -> LidlCrawler:
        """Two stores for the date; the first serves `first_csv`."""
        names = [self._filename(1), self._filename(2)]
        index = "".join(f'<a href="/{quote(n)}"></a>' for n in names)
        # httpx decodes URL.path, so key the bodies by the raw filename.
        bodies = {
            f"/{names[0]}": first_csv,
            f"/{names[1]}": f"{self.HEADER}\n{self.ROW}\n",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path in bodies:
                return httpx.Response(200, text=bodies[request.url.path])
            return httpx.Response(200, text=index)

        crawler = LidlCrawler()
        crawler.client = httpx.Client(transport=httpx.MockTransport(handler))
        return crawler

    def test_a_healthy_store_is_kept_when_another_has_a_bad_header(self, sleeps):
        crawler = self._crawler("WRONG;HEADERS\n1;2\n")

        stores = crawler.get_all_products(self.DATE)

        assert [s.store_id for s in stores] == ["2"]
        assert len(stores[0].items) == 1

    def test_both_stores_are_kept_when_both_parse(self, sleeps):
        crawler = self._crawler(f"{self.HEADER}\n{self.ROW}\n")

        stores = crawler.get_all_products(self.DATE)

        assert sorted(s.store_id for s in stores) == ["1", "2"]
