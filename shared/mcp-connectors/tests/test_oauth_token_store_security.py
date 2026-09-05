#!/usr/bin/env python3
"""Guards on the OAuth token store's credentials-at-rest behaviour.

Two classes of bug live here, and they are guarded differently.

1. *Behaviour* — the store must never claim to have persisted a token it could
   not encrypt. `store_token()` logs "Stored token for connection: ..." the
   instant `_save_tokens()` returns, so a swallowed encryption error produced an
   error line followed immediately by a success line, with nothing on disk.

2. *Drift* — the generator vendors a copy of `oauth/` into each connector's
   `versions/<v>/` tree, and that copy is what actually runs in the container.
   A fix applied only to the canonical file leaves the vendored copies on the
   old code indefinitely. That is exactly what happened: the fail-closed fix for
   the missing-crypto fallback landed on the canonical file, while six shipped
   connectors kept storing OAuth access *and* refresh tokens as reversible
   base64 in a file named `.enc`. The drift tests below are the mechanism fix --
   they make that divergence un-mergeable rather than merely fixed once.
"""
import base64
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
CONNECTORS = REPO / "shared" / "mcp-connectors"
CANONICAL = CONNECTORS / "oauth" / "token_manager.py"

sys.path.insert(0, str(CONNECTORS))
from oauth.token_manager import CRYPTO_AVAILABLE, OAuthToken, TokenManager  # noqa: E402

requires_crypto = pytest.mark.skipif(
    not CRYPTO_AVAILABLE, reason="cryptography backend not installed"
)


DEV_KEY = "dev-encryption-key-32-bytes-long!!"
REAL_KEY = "a-real-looking-secret-32-bytes!!!"


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.delenv("OAUTH_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
    # Default to dev so the dev-default-key refusal does not fire in tests that
    # are not about it; the refusal gets its own explicit coverage below.
    monkeypatch.setenv("ENVIRONMENT", "development")
    return monkeypatch


def _store_path():
    return os.path.join(tempfile.mkdtemp(), "tokens.enc")


@requires_crypto
def test_missing_key_fails_closed_instead_of_reporting_success(clean_env):
    """With no key configured at all, the write must raise -- not log success.

    This was the shipped default: OAUTH_ENCRYPTION_KEY is set by no compose file
    and no chart, so every deployment took this path on every token write.
    """
    path = _store_path()
    tm = TokenManager(storage_path=path)
    with pytest.raises(ValueError, match="no token-store encryption key"):
        tm.store_token("c1", OAuthToken(access_token="ya29.SECRET"))
    assert not os.path.exists(path), "nothing may be written when encryption fails"


@requires_crypto
def test_encryption_key_is_accepted_as_a_fallback(clean_env):
    """ENCRYPTION_KEY already exists in every deployment; reuse it rather than
    requiring a second secret that nothing sets."""
    clean_env.setenv("ENCRYPTION_KEY", DEV_KEY)
    path = _store_path()
    TokenManager(storage_path=path).store_token(
        "c1", OAuthToken(access_token="ya29.SECRET", refresh_token="1//REFRESH")
    )
    # A fresh manager is what a container restart sees.
    assert TokenManager(storage_path=path).get_token("c1") is not None


@requires_crypto
def test_explicit_oauth_key_takes_precedence(clean_env):
    clean_env.setenv("ENCRYPTION_KEY", DEV_KEY)
    clean_env.setenv("OAUTH_ENCRYPTION_KEY", "a-distinct-oauth-key-32-bytes!!!!")
    path = _store_path()
    TokenManager(storage_path=path).store_token("c1", OAuthToken(access_token="ya29.SECRET"))
    # Written under the OAuth-specific key, so the fallback key must NOT read it.
    clean_env.delenv("OAUTH_ENCRYPTION_KEY")
    assert TokenManager(storage_path=path).get_token("c1") is None


@requires_crypto
def test_stored_blob_does_not_contain_the_token_in_the_clear(clean_env):
    """The file is named `.enc`; it has to earn the name."""
    clean_env.setenv("ENCRYPTION_KEY", DEV_KEY)
    path = _store_path()
    TokenManager(storage_path=path).store_token(
        "c1", OAuthToken(access_token="ya29.SENTINEL-TOKEN", refresh_token="1//SENTINEL-REFRESH")
    )
    blob = Path(path).read_text()
    assert "SENTINEL" not in blob
    # ...and not merely base64-shaped, which is what the old fallback produced.
    decoded = base64.b64decode(blob).decode("utf-8", errors="ignore")
    assert "SENTINEL" not in decoded


# ---------------------------------------------------------------------------
# Drift guards -- no crypto backend needed; these read source, not behaviour.
# ---------------------------------------------------------------------------

def _tracked_vendored_copies():
    """Files git actually ships. Untracked locally-generated connector dirs are
    deliberately excluded: they are rebuilt from the canonical file on demand."""
    out = subprocess.run(
        ["git", "ls-files", "*/versions/*/oauth/token_manager.py"],
        cwd=REPO, capture_output=True, text=True, check=True,
    ).stdout.split()
    return [REPO / p for p in out]


def test_vendored_token_managers_match_the_canonical_source():
    copies = _tracked_vendored_copies()
    # A zero-length list would make this test pass without testing anything --
    # the failure mode that let a connector loader find 0 of 28 connectors for
    # months while a green test "covered" it.
    assert copies, "found no tracked vendored oauth/token_manager.py -- guard is vacuous"
    canonical = CANONICAL.read_bytes()
    drifted = [str(p.relative_to(REPO)) for p in copies if p.read_bytes() != canonical]
    assert not drifted, (
        "vendored oauth/token_manager.py has drifted from "
        f"{CANONICAL.relative_to(REPO)} in: {drifted}. The vendored copy is what "
        "runs in the connector container, so a security fix that lands only on "
        "the canonical file does not ship."
    )


def test_no_shipped_copy_falls_back_to_storing_credentials_as_base64():
    """Independent of byte-equality: pin the specific plaintext fallback.

    Byte-equality alone would be satisfied by regressing *every* copy together.
    """
    suspects = [CANONICAL] + _tracked_vendored_copies()
    offenders = []
    for p in suspects:
        src = p.read_text()
        if "NOT SECURE" in src or "assume base64 encoded plain JSON" in src:
            offenders.append(str(p.relative_to(REPO)))
    assert not offenders, (
        f"plaintext credential fallback present in: {offenders}. Tokens must "
        "fail closed when the crypto backend is missing, never degrade to "
        "reversible base64 in a file named .enc."
    )


@requires_crypto
def test_known_dev_key_is_refused_outside_dev(clean_env):
    """A deployment that forgets ENCRYPTION_KEY must not silently fall back to a
    passphrase published in this repository."""
    clean_env.setenv("ENVIRONMENT", "production")
    clean_env.setenv("ENCRYPTION_KEY", DEV_KEY)
    path = _store_path()
    tm = TokenManager(storage_path=path)
    with pytest.raises(ValueError, match="known dev encryption key"):
        tm.store_token("c1", OAuthToken(access_token="ya29.SECRET"))
    assert not os.path.exists(path)


@requires_crypto
def test_real_key_is_accepted_in_production(clean_env):
    """Control: the refusal must key off the *value*, not merely off being in prod."""
    clean_env.setenv("ENVIRONMENT", "production")
    clean_env.setenv("ENCRYPTION_KEY", REAL_KEY)
    path = _store_path()
    TokenManager(storage_path=path).store_token("c1", OAuthToken(access_token="ya29.SECRET"))
    assert TokenManager(storage_path=path).get_token("c1") is not None


def test_dev_key_denylist_matches_the_go_keyring():
    """The Go and Python key handling must refuse the same literals.

    Same lockstep contract the error scrubbers have: patch both or neither.
    A key that Go rejects but Python accepts is a hole the size of the whole
    OAuth token store.
    """
    from oauth.token_manager import _DEV_DEFAULT_KEYS

    go_src = (REPO / "shared" / "go" / "crypto" / "encryption.go").read_text()
    missing = [k for k in _DEV_DEFAULT_KEYS if f'"{k}"' not in go_src]
    assert not missing, (
        f"python refuses {missing} but shared/go/crypto/encryption.go does not "
        "mention them -- the two denylists have drifted"
    )
    # And the reverse direction: anything Go refuses must be refused here too.
    import re
    go_refused = set(re.findall(r'k == "([^"]+)"', go_src))
    assert go_refused, "found no dev-key literals in encryption.go -- guard is vacuous"
    assert go_refused <= set(_DEV_DEFAULT_KEYS), (
        f"go refuses {sorted(go_refused - set(_DEV_DEFAULT_KEYS))} which python accepts"
    )
