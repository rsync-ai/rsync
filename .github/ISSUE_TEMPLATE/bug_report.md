---
name: Bug report
about: Something is broken or not working as expected
labels: bug
---

<!--
NOT A BUG REPORT? Security vulnerabilities do NOT go here — see SECURITY.md and
report privately instead, so a fix can ship before the details are public.

REDACT BEFORE YOU POST. Everything below is public and permanent. Strip API
keys, passwords, connection strings, bearer tokens, hostnames you do not want
public, and any customer data out of logs and configs before pasting them.
-->

## Description

<!-- What happened? What did you expect to happen? -->

## Steps to reproduce

1.
2.
3.

## How you installed it

<!-- install.sh, `docker compose` from a checkout, or Helm — and which ref,
     tag, or chart version. This is usually the single most useful field. -->

## Environment

- rsync.ai version / commit (or `RSYNC_REF` / chart version):
- Docker version (`docker --version`) or Kubernetes version:
- OS:
- LLM provider / model:

## Logs

<!-- Logs from the service that failed, not the whole stack:
       docker compose -p rsync-ai logs --tail=200 <service>
     On Kubernetes:
       kubectl -n rsync logs deploy/<service> --tail=200
     Redact credentials before pasting. -->

```
```

## Additional context

<!-- Screenshots, pipeline config, connector type, and the pipeline execution
     id if a run failed — every stage event is keyed to it. -->
