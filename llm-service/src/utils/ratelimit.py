import os
import time
import logging
from typing import Optional, Tuple

try:
    import redis.asyncio as redis  # redis>=4
except Exception:  # pragma: no cover
    redis = None

# env_bool, not a local copy: the naive form reads compose's empty ${VAR:-} as
# False and silently flips every default-True flag. That bug was fixed once, in
# openai_client; this module carried the unfixed copy.
from src.utils.openai_client import env_bool as _env_bool

logger = logging.getLogger("ratelimit")


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _default_redis_url() -> Optional[str]:
    url = (os.getenv("REDIS_URL") or "").strip()
    if url:
        return url

    host = (os.getenv("REDIS_HOST") or "").strip() or "redis"
    port = (os.getenv("REDIS_PORT") or "").strip() or "6379"
    db = (os.getenv("REDIS_DB") or "").strip() or "0"
    password = (os.getenv("REDIS_PASSWORD") or "").strip()

    if password:
        return f"redis://:{password}@{host}:{port}/{db}"
    return f"redis://{host}:{port}/{db}"


def _redis_target_for_logs() -> str:
    """Where we are pointing, with no credential in it.

    ``_default_redis_url`` embeds REDIS_PASSWORD, and the startup line used to
    log that URL verbatim at INFO — putting the Redis password into container
    logs, log shipping, and any bug report that pastes them.
    """
    url = (os.getenv("REDIS_URL") or "").strip()
    if url:
        # A caller-supplied URL may carry user:pass@; keep only what follows.
        return url.rsplit("@", 1)[-1] if "@" in url else url
    host = (os.getenv("REDIS_HOST") or "").strip() or "redis"
    port = (os.getenv("REDIS_PORT") or "").strip() or "6379"
    db = (os.getenv("REDIS_DB") or "").strip() or "0"
    return f"{host}:{port}/{db}"


class FixedWindowLimiter:
    async def hit(self, key: str, limit: int, window_seconds: int) -> Tuple[bool, int]:
        raise NotImplementedError


class InMemoryFixedWindowLimiter(FixedWindowLimiter):
    def __init__(self) -> None:
        # key -> (window_id, count, expires_at)
        self._store: dict[str, tuple[int, int, float]] = {}

    async def hit(self, key: str, limit: int, window_seconds: int) -> Tuple[bool, int]:
        now = time.time()
        window_id = int(now // window_seconds)
        expires_at = (window_id + 1) * window_seconds

        cur = self._store.get(key)
        if cur is None or cur[0] != window_id or cur[2] <= now:
            self._store[key] = (window_id, 1, expires_at)
            return True, int(max(0.0, expires_at - now))

        _, count, _ = cur
        count += 1
        self._store[key] = (window_id, count, expires_at)
        return count <= limit, int(max(0.0, expires_at - now))


class RedisFixedWindowLimiter(FixedWindowLimiter):
    """Shared-window limiter backed by Redis, degrading to in-memory on failure.

    ``redis.from_url`` connects lazily, so an unreachable or password-protected
    Redis raises nothing at construction — ``get_limiter`` logged
    "Rate limiting enabled (Redis)" and the first real failure arrived at the
    first request. This class used to re-raise it, and every call site wraps
    ``hit`` in ``except Exception`` and fails *open*, so the outcome was: no rate
    limiting at all, one warning line per request, and a startup log claiming
    the opposite. That is the difference between "the limiter degraded" and
    "the limiter is not running", and only the second one was true.

    ``get_limiter`` already documents the intended behaviour — fall back to
    in-memory when Redis is unavailable. It just could not happen at
    construction time. It happens here instead, at the first point the failure
    is observable, and the warning is logged once rather than per request.
    """

    def __init__(self, client: "redis.Redis") -> None:
        self._redis = client
        self._fallback = InMemoryFixedWindowLimiter()
        self._degraded = False

    async def hit(self, key: str, limit: int, window_seconds: int) -> Tuple[bool, int]:
        if self._degraded:
            return await self._fallback.hit(key, limit, window_seconds)

        now = int(time.time())
        window_id = now // window_seconds
        # Keep key stable per window
        redis_key = f"{key}:{window_id}"

        try:
            # INCR + EXPIRE (only on first hit)
            pipe = self._redis.pipeline()
            pipe.incr(redis_key, 1)
            pipe.ttl(redis_key)
            count, ttl = await pipe.execute()
            if ttl is None or ttl < 0:
                # expire slightly beyond the window boundary
                await self._redis.expire(redis_key, window_seconds + 1)
                ttl = window_seconds
            allowed = int(count) <= int(limit)
            return allowed, int(ttl if ttl is not None else window_seconds)
        except Exception as e:
            self._degraded = True
            logger.warning(
                "Redis rate limiter unreachable at %s (%s: %s); degrading to "
                "in-memory limits for this process. Limits are now per-replica, "
                "not shared — check REDIS_HOST/REDIS_PORT/REDIS_PASSWORD.",
                _redis_target_for_logs(),
                type(e).__name__,
                e,
            )
            return await self._fallback.hit(key, limit, window_seconds)


_limiter: Optional[FixedWindowLimiter] = None


def get_limiter() -> FixedWindowLimiter:
    """
    Best-effort rate limiter.
    - Prefers Redis (shared across replicas) when available.
    - Falls back to in-memory (single-process) if Redis is unavailable.
    """
    global _limiter
    if _limiter is not None:
        return _limiter

    use_redis = _env_bool("RSYNC_RATE_LIMIT_USE_REDIS", True)
    if use_redis and redis is not None:
        try:
            url = _default_redis_url()
            client = redis.from_url(url, encoding="utf-8", decode_responses=True)
            _limiter = RedisFixedWindowLimiter(client)
            # from_url() opens no socket, so this line reports configuration, not
            # reachability, and must not claim otherwise. The limiter says so
            # itself (once) if the first request cannot reach the server. The
            # target is logged without its password.
            logger.info(
                f"✅ Rate limiting configured (Redis): {_redis_target_for_logs()} "
                f"(connection verified on first request)"
            )
            return _limiter
        except Exception as e:  # pragma: no cover
            logger.warning(f"⚠️  Redis rate limiter unavailable, falling back to in-memory: {e}")

    _limiter = InMemoryFixedWindowLimiter()
    logger.info("✅ Rate limiting enabled (in-memory)")
    return _limiter

