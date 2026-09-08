"""The Redis rate limiter must degrade, not disappear, and never log its password.

Two defects, both silent, both in the same startup path:

1. ``redis.from_url`` opens no socket, so an unreachable or password-protected
   Redis raised nothing at construction. Startup logged
   "Rate limiting enabled (Redis)" and the first request then hit an exception
   that ``RedisFixedWindowLimiter.hit`` re-raised. Every call site wraps ``hit``
   in ``except Exception`` and fails *open*, so the real state was: no rate
   limiting at all, one warning line per request, and a startup log asserting the
   opposite. ``get_limiter``'s own docstring promises an in-memory fallback; it
   could not happen at construction time, so it now happens at the first request.

2. That same startup line logged ``_default_redis_url()``, which embeds
   REDIS_PASSWORD — putting the Redis password into container logs and every bug
   report that pastes them.
"""

import asyncio
import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.utils import ratelimit as rl  # noqa: E402


PW = "s3cr3t-redis-pw"


class _Boom:
    """A redis client whose every command fails, the way an unreachable one does."""

    def pipeline(self):
        return self

    def incr(self, *a, **k):
        return self

    def ttl(self, *a, **k):
        return self

    async def execute(self):
        raise ConnectionError("Error 111 connecting to redis:6379. Connection refused.")

    async def expire(self, *a, **k):
        raise ConnectionError("Error 111 connecting to redis:6379. Connection refused.")


@pytest.fixture(autouse=True)
def _reset_memoized_limiter():
    rl._limiter = None
    yield
    rl._limiter = None


def test_startup_log_never_carries_the_redis_password(monkeypatch, caplog):
    monkeypatch.setenv("REDIS_HOST", "redis")
    monkeypatch.setenv("REDIS_PORT", "6379")
    monkeypatch.setenv("REDIS_PASSWORD", PW)
    monkeypatch.delenv("REDIS_URL", raising=False)

    # The URL handed to the client must still authenticate...
    assert PW in rl._default_redis_url()
    # ...but nothing log-bound may repeat it.
    assert PW not in rl._redis_target_for_logs()

    class _FakeRedisModule:
        @staticmethod
        def from_url(url, **kwargs):
            assert PW in url, "the client must still be given the password"
            return _Boom()

    monkeypatch.setattr(rl, "redis", _FakeRedisModule)
    with caplog.at_level(logging.INFO, logger="ratelimit"):
        rl.get_limiter()

    assert caplog.records, "expected a startup line"
    for record in caplog.records:
        assert PW not in record.getMessage(), f"password leaked into logs: {record.getMessage()}"


def test_a_url_supplied_with_credentials_is_also_stripped_for_logs(monkeypatch):
    monkeypatch.setenv("REDIS_URL", f"redis://someuser:{PW}@cache.internal:6380/2")
    target = rl._redis_target_for_logs()
    assert PW not in target
    assert "someuser" not in target
    assert "cache.internal:6380/2" in target


def test_an_unreachable_redis_still_rate_limits_instead_of_failing_open():
    limiter = rl.RedisFixedWindowLimiter(_Boom())

    async def run():
        # Limit of 2 in the window: the first two pass, the third must be refused.
        results = [await limiter.hit("k", 2, 60) for _ in range(3)]
        return [ok for ok, _ in results]

    assert asyncio.run(run()) == [True, True, False], (
        "an unreachable Redis left the limiter passing every request; "
        "hit() re-raised and every call site fails open"
    )


def test_the_degrade_warning_is_logged_once_not_per_request(caplog):
    limiter = rl.RedisFixedWindowLimiter(_Boom())

    async def run():
        for _ in range(5):
            await limiter.hit("k", 100, 60)

    with caplog.at_level(logging.WARNING, logger="ratelimit"):
        asyncio.run(run())

    degraded = [r for r in caplog.records if "degrading to" in r.getMessage()]
    assert len(degraded) == 1, f"expected one degrade warning, got {len(degraded)}"
    assert PW not in degraded[0].getMessage()


def test_redis_toggle_treats_an_empty_value_as_unset(monkeypatch):
    """Compose renders an unset ${VAR:-} as "". The local _env_bool copy this
    module carried read that as False and silently turned Redis off."""
    monkeypatch.setenv("RSYNC_RATE_LIMIT_USE_REDIS", "")
    assert rl._env_bool("RSYNC_RATE_LIMIT_USE_REDIS", True) is True
    monkeypatch.setenv("RSYNC_RATE_LIMIT_USE_REDIS", "false")
    assert rl._env_bool("RSYNC_RATE_LIMIT_USE_REDIS", True) is False
