#!/bin/sh
python -m crawler.cli.crawl \
  --chain "$CRAWLER_CHAIN" \
  --create-zip "$CRAWLER_CREATE_ZIP" \
  --skip-existing "$CRAWLER_SKIP_EXISTING" \
  --verbose "$CRAWLER_VERBOSE" \
  "$CRAWLER_OUTPUT"