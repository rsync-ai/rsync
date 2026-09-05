# Getting help

Start with the docs — most questions are answered there, and the answer is usually more
complete than a thread.

| I want to… | Go to |
|---|---|
| Install it and run a first pipeline | [Quick start](docs/getting-started/quickstart.md) |
| Run it in production | [Self-hosting](docs/deployment/self-hosting.md) · [Kubernetes](docs/deployment/kubernetes.md) |
| Know whether a source or destination is supported | [Connector reference](docs/connectors/reference.md) |
| Configure something | [Environment variables](docs/deployment/env-vars.md) |
| Call the API | [API reference](docs/api/README.md) |
| Understand how it works | [ARCHITECTURE.md](ARCHITECTURE.md) · [System overview](docs/architecture/overview.md) |
| Build a connector | [Connector developer guide](docs/connectors/developer-guide.md) |
| Set up a dev environment or send a patch | [CONTRIBUTING.md](CONTRIBUTING.md) |
| See what changed between versions | [CHANGELOG.md](CHANGELOG.md) |

## Reporting a bug

[Open an issue](https://github.com/rsync-ai/rsync/issues) using the bug template. What
makes a report actionable, roughly in order of usefulness:

- **How you installed it** — the `install.sh` one-liner, `docker compose`, or Helm — and
  which ref or chart version.
- **What you expected and what happened**, with the exact error text.
- **Logs from the service that failed**, not the whole stack:
  `docker compose -p rsync-ai logs --tail=200 <service>`.
- **The pipeline execution id**, if a run failed — every stage event is keyed to it.

Redact credentials, connection strings, and customer data before pasting anything.

## Asking a question

Questions and ideas are welcome as issues too. Search the existing ones first; if you
find a matching one, add your details there rather than opening a duplicate — a second
report on the same issue is more useful than a second issue.

## Security vulnerabilities

**Do not open a public issue.** Follow [SECURITY.md](SECURITY.md), which asks you to mail
`security@rsync.ai` privately so a fix can ship before the details are public.

## What is not supported

rsync.ai is source-available under the [Elastic License 2.0](LICENSE). There is no
commercial support contract attached to this repository, and no service-level commitment
on issue response. Maintainers read everything; they cannot promise a timeline.
