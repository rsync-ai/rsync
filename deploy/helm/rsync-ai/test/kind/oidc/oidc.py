"""A minimal OIDC client-credentials provider, for stage S4 only.

Why this exists rather than `OAuthBearerUnsecuredValidatorCallbackHandler`:
Kafka ships an "unsecured" OAUTHBEARER handler that accepts any unsigned JWT.
Pointing S4 at it would be the exact same mistake as leaving
`auto.create.topics.enable=true` on -- the stage would go green while proving
nothing, because every token is valid including ones no provider ever issued.
So this mints *signed* RS256 tokens and publishes a JWKS the broker verifies
against, which makes "the broker accepted it" a statement about the signature.

It is deliberately not a real OIDC provider. It does not implement discovery
beyond the one document Kafka reads, it does not rotate keys, it does not
support any grant but client_credentials, and it serves over plain HTTP. A
production deployment MUST use https for the token endpoint: the client secret
travels in the Authorization header on every token fetch.

Two endpoints matter:

  POST /token   client_credentials, HTTP Basic (what Kafka's
                OAuthBearerLoginCallbackHandler sends by default) or form body.
  GET  /jwks    the public half, which the broker's validator fetches and caches.

And one that exists only to make failure legible:

  POST /token?rogue=1   mints a token signed by a DIFFERENT key that is never
                        published in the JWKS. This is S4's discriminating
                        control -- see broker-up.sh. Without it, "the broker
                        accepted our token" cannot be told apart from "the
                        broker accepts anything".
"""

import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

PORT = int(os.environ.get("OIDC_PORT", "8080"))
# The `iss` claim is a fixed string, not a URL anything resolves. It has to be:
# the broker reaches this server by docker DNS (`oidc`) while the cluster pods
# reach it by Kubernetes DNS (`oidc.<ns>.svc.cluster.local`), so no single URL is
# correct from both sides. The broker validates `iss` as an opaque string.
ISSUER = os.environ.get("OIDC_ISSUER", "http://oidc.rsync.svc.cluster.local:8080")
AUDIENCE = os.environ.get("OIDC_AUDIENCE", "kafka")
TTL = int(os.environ.get("OIDC_TOKEN_TTL", "3600"))
KEY_PATH = os.environ.get("OIDC_KEY", "/keys/oidc.key")

# The credential the broker's clients present to *this* server. It is not the
# Kafka credential -- the Kafka credential is the resulting JWT.
CLIENT_ID = os.environ.get("OIDC_CLIENT_ID", "rsync")
CLIENT_SECRET = os.environ.get("OIDC_CLIENT_SECRET", "")
if not CLIENT_SECRET:
    raise SystemExit("OIDC_CLIENT_SECRET is required; refusing to accept any secret")


def _load_or_make_key(path):
    """Persisted, not generated per start.

    A key that changed on every restart would invalidate every token the broker
    had already cached a JWKS for, and the resulting authentication failure
    would look exactly like a chart bug rather than a harness restart.
    """
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, "rb") as fh:
            return serialization.load_pem_private_key(fh.read(), password=None)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
    return key


SIGNING_KEY = _load_or_make_key(KEY_PATH)
KID = "s4"
# Never written to disk and never published: a token signed by this is a token
# no legitimate provider could have issued.
ROGUE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def b64u(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def b64u_int(n: int) -> str:
    return b64u(n.to_bytes((n.bit_length() + 7) // 8, "big"))


def mint(subject: str, key, kid: str) -> str:
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT", "kid": kid}
    claims = {
        "iss": ISSUER,
        "sub": subject,
        "aud": AUDIENCE,
        "iat": now,
        "exp": now + TTL,
        # Kafka's validator reads `scope` when expected.scope is configured; it
        # is harmless otherwise and makes the token look like a real one.
        "scope": "kafka",
    }
    signing_input = (
        b64u(json.dumps(header, separators=(",", ":")).encode())
        + "."
        + b64u(json.dumps(claims, separators=(",", ":")).encode())
    ).encode()
    sig = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return signing_input.decode() + "." + b64u(sig)


def jwks() -> dict:
    pub = SIGNING_KEY.public_key().public_numbers()
    return {
        "keys": [
            {
                "kty": "RSA",
                "use": "sig",
                "alg": "RS256",
                "kid": KID,
                "n": b64u_int(pub.n),
                "e": b64u_int(pub.e),
            }
        ]
    }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # one line per request, no client secrets
        print("oidc %s - %s" % (self.path.split("?")[0], fmt % args), flush=True)

    def _send(self, code, body: dict):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/jwks":
            self._send(200, jwks())
        elif path == "/.well-known/openid-configuration":
            self._send(
                200,
                {
                    "issuer": ISSUER,
                    "token_endpoint": ISSUER + "/token",
                    "jwks_uri": ISSUER + "/jwks",
                    "grant_types_supported": ["client_credentials"],
                },
            )
        elif path == "/healthz":
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not_found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/token":
            self._send(404, {"error": "not_found"})
            return

        length = int(self.headers.get("Content-Length") or 0)
        form = parse_qs(self.rfile.read(length).decode() if length else "")

        # Kafka's OAuthBearerLoginCallbackHandler uses HTTP Basic by default and
        # falls back to form fields; accept both so a client that does either
        # works and a client that does neither is refused for the right reason.
        cid = (form.get("client_id") or [""])[0]
        csec = (form.get("client_secret") or [""])[0]
        auth = self.headers.get("Authorization") or ""
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode()
                cid, csec = decoded.split(":", 1)
            except (ValueError, UnicodeDecodeError):
                self._send(400, {"error": "invalid_request"})
                return

        if cid != CLIENT_ID or csec != CLIENT_SECRET:
            # A real 401, not a token. If this ever returned a token anyway the
            # whole stage would be measuring nothing.
            self._send(401, {"error": "invalid_client"})
            return

        rogue = parse_qs(parsed.query).get("rogue", ["0"])[0] == "1"
        token = mint(cid, ROGUE_KEY if rogue else SIGNING_KEY, "rogue" if rogue else KID)
        self._send(
            200,
            {"access_token": token, "token_type": "Bearer", "expires_in": TTL},
        )


if __name__ == "__main__":
    print(
        "oidc: issuer=%s audience=%s port=%d client_id=%s"
        % (ISSUER, AUDIENCE, PORT, CLIENT_ID),
        flush=True,
    )
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
