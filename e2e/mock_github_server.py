#!/usr/bin/env python3
import json
import os
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer


PORT = int(os.getenv("PORT", "8080"))

EXPIRED_TOKEN = os.getenv("EXPIRED_TOKEN", "expired_token")
VALID_TOKEN = os.getenv("VALID_TOKEN", "valid_token")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN", "refresh_token")

# Token exchange expiry is intentionally within API Gateway's "5 minute refresh buffer",
# but outside orchestrator TokenManager's "1 minute expiring soon" buffer.
# Default 4 minutes:
# - within API Gateway's "5 minute refresh buffer" so refresh is allowed
# - outside orchestrator TokenManager's "1 minute expiring soon" buffer for most of the run
EXCHANGE_EXPIRES_IN_SECONDS = int(os.getenv("EXCHANGE_EXPIRES_IN_SECONDS", "240"))
REFRESH_EXPIRES_IN_SECONDS = int(os.getenv("REFRESH_EXPIRES_IN_SECONDS", "3600"))

STATE = {
    "token_exchange_calls": 0,
    "token_refresh_calls": 0,
    "repos_calls": 0,
    "repos_unauthorized": 0,
    "last_repos_auth": "",
    "last_repos_token": "",
    # /licenses mirrors /repos but matches the shape of the generated github-rest
    # connector's only resource (GET /licenses). Used by the OAuth-refresh E2E:
    # the connector must present a FRESH (refreshed) token to get 200 here.
    "licenses_calls": 0,
    "licenses_unauthorized": 0,
    "last_licenses_token": "",
}


def _read_body(handler: BaseHTTPRequestHandler) -> str:
    length = int(handler.headers.get("Content-Length", "0") or "0")
    return handler.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else ""


def _extract_token(handler: BaseHTTPRequestHandler) -> str:
    auth = handler.headers.get("Authorization", "") or ""
    lower = auth.lower().strip()
    if lower.startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    if lower.startswith("token "):
        return auth.split(" ", 1)[1].strip()
    return ""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Keep test output clean
        return

    def _json(self, code: int, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text(self, code: int, text: str):
        body = (text or "").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]

        if path == "/health":
            return self._text(200, "ok")

        if path == "/metrics":
            return self._json(200, STATE)

        if path == "/user":
            # Used by api-gateway OAuth callback account info (optional).
            return self._json(200, {"name": "mock-user"})

        if path in ("/user/me", "/users/me", "/me", "/whoami"):
            # Auth-protected "whoami" probe. The generated github-rest connector's
            # test_connection tries these paths and REQUIRES a 2xx (it refuses to
            # fall back to unauthenticated roots — security audit T1-8). Rejecting
            # the stale EXPIRED_TOKEN with 401 means a passing test_connection
            # proves the token was refreshed first.
            token = _extract_token(self)
            if token == EXPIRED_TOKEN or not token:
                return self._json(401, {"message": "Bad credentials"})
            return self._json(200, {"login": "mock-user", "id": 42})

        if path == "/repos":
            STATE["repos_calls"] += 1
            auth = self.headers.get("Authorization", "") or ""
            token = _extract_token(self)
            STATE["last_repos_auth"] = auth[:200]
            STATE["last_repos_token"] = token[:200]

            if token == EXPIRED_TOKEN:
                STATE["repos_unauthorized"] += 1
                return self._json(401, {"message": "Bad credentials"})

            # Minimal repo records; keep fields aligned with destination schema in tests.
            return self._json(200, [{"id": 1, "name": "repo1"}, {"id": 2, "name": "repo2"}])

        if path == "/licenses":
            # Data endpoint for the generated github-rest connector (its sole
            # resource). Rejects the stale EXPIRED_TOKEN with 401 so a run that
            # lands rows PROVES the platform refreshed the token first.
            STATE["licenses_calls"] += 1
            token = _extract_token(self)
            STATE["last_licenses_token"] = token[:200]

            if token == EXPIRED_TOKEN:
                STATE["licenses_unauthorized"] += 1
                return self._json(401, {"message": "Bad credentials"})

            # Shape mirrors real GitHub /licenses (id + key/name/spdx_id).
            return self._json(200, [
                {"id": 1, "key": "mit", "name": "MIT License", "spdx_id": "MIT"},
                {"id": 2, "key": "apache-2.0", "name": "Apache License 2.0", "spdx_id": "Apache-2.0"},
                {"id": 3, "key": "gpl-3.0", "name": "GNU GPLv3", "spdx_id": "GPL-3.0"},
            ])

        return self._json(404, {"error": "not_found", "path": path})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        body = _read_body(self)

        if path == "/login/oauth/access_token":
            form = urllib.parse.parse_qs(body)
            grant_type = (form.get("grant_type") or [""])[0].strip()

            if grant_type == "authorization_code":
                STATE["token_exchange_calls"] += 1
                return self._json(
                    200,
                    {
                        "access_token": EXPIRED_TOKEN,
                        "refresh_token": REFRESH_TOKEN,
                        "token_type": "bearer",
                        "expires_in": EXCHANGE_EXPIRES_IN_SECONDS,
                    },
                )

            if grant_type == "refresh_token":
                STATE["token_refresh_calls"] += 1
                return self._json(
                    200,
                    {
                        "access_token": VALID_TOKEN,
                        "refresh_token": REFRESH_TOKEN,
                        "token_type": "bearer",
                        "expires_in": REFRESH_EXPIRES_IN_SECONDS,
                    },
                )

            # Fallback for unknown grant types
            return self._json(
                200,
                {
                    "access_token": VALID_TOKEN,
                    "refresh_token": REFRESH_TOKEN,
                    "token_type": "bearer",
                    "expires_in": REFRESH_EXPIRES_IN_SECONDS,
                },
            )

        return self._json(404, {"error": "not_found", "path": path})


def main():
    addr = ("0.0.0.0", PORT)
    httpd = HTTPServer(addr, Handler)
    print(f"mock-github listening on http://0.0.0.0:{PORT}")
    httpd.serve_forever()


if __name__ == "__main__":
    main()

