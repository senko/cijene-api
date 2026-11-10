import os
import time
import logging
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from service.config import settings
from service.db.models import User

logger = logging.getLogger(__name__)

# Record which user makes each request.
# Off unless API_KEY_LOG is set to a file path.
# Line format (space-separated):
#   <timestamp> <user id> <method> <path> <client host> <user agent...>
_key_log: logging.Logger | None = None
if _key_log_path := os.getenv("API_KEY_LOG"):
    _key_log = logging.getLogger("api_key_log")
    _key_log.propagate = False
    _key_log.setLevel(logging.INFO)
    _key_log_handler = logging.FileHandler(_key_log_path)
    _key_log_handler.setFormatter(
        logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z")
    )
    _key_log.addHandler(_key_log_handler)

# Security scheme for OpenAPI documentation
security_scheme = HTTPBearer(scheme_name="HTTPBearer")

# Simple in-memory cache for authentication results
# Structure: {api_key: (user_or_none, timestamp)}
_auth_cache: dict[str, tuple[User | None, float]] = {}

# Cache durations in seconds
CACHE_HIT_TTL = 3600  # 60 minutes for valid users
CACHE_MISS_TTL = 60  # 60 seconds for invalid tokens
CACHE_MAX_SIZE = 10000  # Maximum cache size before cleanup

db = settings.get_db()


async def _lookup_user_by_token(api_key: str) -> User | None:
    """
    Lookup user by API key with caching.

    Args:
        api_key: The API key to look up.

    Returns:
        User object if found and active, None otherwise.
    """
    now = time.time()

    # Check cache first
    if api_key in _auth_cache:
        cached_user, timestamp = _auth_cache[api_key]
        age = now - timestamp

        if (cached_user and age < CACHE_HIT_TTL) or (
            not cached_user and age < CACHE_MISS_TTL
        ):
            # Cache hit and still valid
            _auth_cache[api_key] = (cached_user, now)  # Update timestamp
            return cached_user
        else:
            # Need to refresh
            del _auth_cache[api_key]

    user = await db.get_user_by_api_key(api_key)
    _auth_cache[api_key] = (user, now)

    if len(_auth_cache) > CACHE_MAX_SIZE:
        # Remove all miss entries from cache to prevent memory bloat
        _to_remove = [k for k, v in _auth_cache.items() if v[0] is not None]
        for k in _to_remove:
            del _auth_cache[k]
    return user


async def verify_authentication(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security_scheme),
) -> User:
    """
    Verify bearer token authentication.

    Args:
        request: The incoming request (used for the optional key log).
        credentials: The HTTP authorization credentials containing the bearer token.

    Returns:
        The authenticated user object.
    """
    api_key = credentials.credentials
    user = await _lookup_user_by_token(api_key)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Unknown API key",
        )

    logger.debug(f"Authenticated access for user: {user.name} (id={user.id})")
    if _key_log:
        _key_log.info(
            "%s %s %s %s %s",
            user.id,
            request.method,
            request.url.path,
            request.client.host if request.client else "-",
            request.headers.get("user-agent", "-"),
        )
    return user


# Dependency for protecting routes
RequireAuth = Depends(verify_authentication)
