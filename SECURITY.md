# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| latest (main) | Yes |
| older releases | No — please update |

## Reporting a vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Email **security@rsync.ai** with:

1. A description of the vulnerability
2. Steps to reproduce (proof-of-concept if possible)
3. The potential impact
4. Your GitHub handle (optional, for credit)

We will acknowledge your report within **48 hours** and aim to ship a fix within **14 days** for critical issues.

## Scope

In scope:
- Authentication and session management
- API authorization and access control
- Connector credential encryption
- WebSocket security
- SQL injection / command injection in pipeline execution

Out of scope:
- Denial of service via resource exhaustion (no SLA on self-hosted instances)
- Issues in third-party dependencies (report upstream)
- Social engineering

## Disclosure policy

We follow coordinated disclosure. We ask that you give us time to patch before publishing details. We will credit reporters in release notes unless you prefer to remain anonymous.
