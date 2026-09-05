import os
import time
import logging
from typing import Optional, Tuple

try:
    import redis.asyncio as redis  # redis>=4
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger("ratelimit")


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "true" if default else "false") or "").strip().lower()
    return v in ("1", "true", "yes", "y", "on")


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
    def __init__(self, client: "redis.Redis") -> None:
        self._redis = client

    async def hit(self, key: str, limit: int, window_seconds: int) -> Tuple[bool, int]:
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
        except Exception as e:  # pragma: no cover
            # Fail open to in-memory behavior at caller level
            raise e


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
            logger.info(f"✅ Rate limiting enabled (Redis): {url}")
            return _limiter
        except Exception as e:  # pragma: no cover
            logger.warning(f"⚠️  Redis rate limiter unavailable, falling back to in-memory: {e}")

    _limiter = InMemoryFixedWindowLimiter()
    logger.info("✅ Rate limiting enabled (in-memory)")
    return _limiter

