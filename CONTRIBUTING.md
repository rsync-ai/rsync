# Contributing to rsync.ai

Thank you for your interest in contributing. This guide covers how to set up a local dev environment, submit changes, and follow the project's conventions.

## Before you start

- Check [existing issues](https://github.com/rsync-ai/rsync/issues) before opening a new one
- For large changes, open an issue first to discuss the approach
- All contributions are subject to the [Elastic License 2.0](LICENSE) and require a DCO sign-off (see [below](#developer-certificate-of-origin-dco))

## Local development setup

**Prerequisites:** Docker 24+, Go 1.22+, Node.js 20+, Python 3.11+

```bash
git clone https://github.com/rsync-ai/rsync.git
cd rsync
cp .env.example .env
# Add your OPENAI_API_KEY to .env
make dev
```

This starts all services via Docker Compose. The frontend is at `http://localhost:3000` and the API gateway at `http://localhost:5001`.

## Services

| Service | Language | Path |
|---|---|---|
| API Gateway | Go | `api-gateway/` |
| Orchestrator | Go | `backend-orchestrator/` |
| Temporal Adapter | Go | `backend-temporal-adapter/` |
| LLM Service + Agents | Python | `llm-service/` |
| Frontend | Next.js | `frontend/` |
| Shared Go libs | Go | `shared/go/` |

## Making changes

1. Fork and create a branch: `git checkout -b feat/my-feature`
2. Make your changes with tests where applicable
3. Verify the Go services compile: `cd api-gateway && go build ./...`
4. Run relevant tests: `make test-all`
5. **Sign off your commits** with `git commit -s` (see [DCO](#developer-certificate-of-origin-dco) below)
6. Open a pull request against `main`

## Pull request guidelines

- Keep PRs focused — one logical change per PR
- Write a clear description of what changed and why
- Reference any related issues with `Fixes #123`
- All CI checks must pass before merge

## Developer Certificate of Origin (DCO)

All contributions require a **sign-off** certifying you have the right to submit them under the
[Elastic License 2.0](LICENSE). We use the [Developer Certificate of Origin](DCO) — a lightweight
alternative to a CLA, with no copyright assignment.

Add a sign-off to each commit with the `-s` flag:

```bash
git commit -s -m "feat: my change"
```

This appends a `Signed-off-by: Your Name <you@example.com>` line, which must match your git
`user.name` / `user.email`. By signing off you agree to the terms in the [DCO](DCO) file. To
sign off commits you already made, run `git rebase --signoff main`.

Sign-off is required on every commit in a contributed PR. The
[DCO GitHub App](https://github.com/apps/dco) is installed and posts a **DCO** check on every pull
request, so a missing `Signed-off-by` surfaces as a failing check on the PR itself rather than as a
review comment. It is a *reported* check, not yet a *required* one — treat a red DCO as blocking
anyway and fix the sign-off before asking for a merge.

## Database migrations

Migrations live in `api-gateway/migrations/`. Use the next sequential number as the prefix (e.g., `047_add_foo.sql`). Migrations run automatically on startup and are tracked by full filename.

## Connector development

See [docs/connectors/developer-guide.md](docs/connectors/developer-guide.md) for how to build a new MCP connector.

## Code style

- **Go**: `gofmt`, standard library conventions
- **Python**: PEP 8, type hints encouraged
- **TypeScript**: Prettier (config in `frontend/`)
- No commented-out code; no TODO comments without a linked issue

## Reporting security issues

Do **not** open a public issue for security vulnerabilities. See [SECURITY.md](SECURITY.md) instead.
