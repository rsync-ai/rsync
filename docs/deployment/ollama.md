# Running rsync-ai on local models (Ollama)

For prospects who say *"we don't want OpenAI — nothing leaves our network."*

rsync-ai speaks the OpenAI-compatible API for every LLM call, so Ollama is a drop-in provider.
No code path is OpenAI-specific.

> **What ever reaches an LLM:** schema metadata (table/column names, types, row counts), connector
> metadata, SQL shapes, and user-authored chat text. **Never** row values, query results,
> credentials, or PII. That contract holds on every provider. Offline models are about the *metadata* also staying in-house.

---

## Two independent switches

There are two provider settings, and confusing them is the most common misconfiguration.

| Switch | Controls | Default |
|---|---|---|
| `LLM_PROVIDER` | Every agent: planner, tool-generator, chat, healer | auto-detect |
| `EXPLORER_OFFLINE_ONLY` | Forces **only** the Data Explorer to Ollama | **`false`** |

`LLM_PROVIDER` accepts `openai` · `azure` · `groq` · `ollama`. When unset, `resolve_provider()`
auto-detects: Azure endpoint → `azure`, else `OPENAI_API_KEY` → `openai`, **else `ollama`**
([openai_client.py:77-115](../../llm-service/src/utils/openai_client.py#L77)). Two safety
properties worth quoting to a security reviewer:

- **Fail-closed.** `LLM_PROVIDER=openai` with no key resolves to `ollama`, not to an error and not
  to a silent unauthenticated call (`:101-103`).
- **Groq is never auto-selected.** A stray `GROQ_API_KEY` cannot silently route prompts to an
  undisclosed external LLM; Groq requires an explicit `LLM_PROVIDER=groq` (`:98-99`, `:113-115`).

The Explorer inherits `LLM_PROVIDER` unless `EXPLORER_LLM_PROVIDER` overrides it
([resolve_explorer_provider()](../../llm-service/src/utils/openai_client.py#L139), called by
[main.py:852](../../llm-service/src/gateway/main.py#L852),
[explorer/api.py:122](../../llm-service/src/agents/explorer/api.py#L122) and
[explorer/rank_tables.py:76](../../llm-service/src/agents/explorer/rank_tables.py#L76) — one rule,
three entry points, no way for them to disagree).

> ⚠️ **`EXPLORER_OFFLINE_ONLY` defaults to `false`, not `true`.** Older copies of this doc said
> "Explorer always uses Ollama regardless of whether OpenAI is configured." That has not been true
> since the default flipped. With `LLM_PROVIDER=azure` and no explicit override, **Explorer schema
> metadata goes to Azure OpenAI.** Do not tell a customer otherwise without checking their env.

### For a full air-gap, set `LLM_PROVIDER=ollama`

`EXPLORER_OFFLINE_ONLY=true` narrows *only* the Explorer. To take the whole stack offline, set the
provider itself:

```bash
LLM_PROVIDER=ollama
OLLAMA_BASE_URL=http://ollama:11434     # or wherever Ollama runs
```

That covers the Explorer too, via inheritance.

### Verify it — don't assume it

Every `EXPLORER_*` and `RANK_TABLES_*` variable on this page reaches the container on all three compose files
(`docker-compose.yml` via `env_file:`, quickstart and prod via explicit passthrough). That was not
always true: until 2026-08-03 `docker-compose.quickstart.yml` forwarded only `OPENAI_API_KEY`,
`LLM_PROVIDER`, `LLM_MODEL`, and `OLLAMA_URL` with no `env_file:` at all, so
`EXPLORER_OFFLINE_ONLY=true` in `.env` was **silently inert on the OSS quickstart** — a privacy
guarantee that did not hold, with nothing in the logs to say so
(`KI-EXPLORER-OFFLINE-FLAG-NOT-DELIVERED`).

The lesson generalises past that one flag: **a config flag is only real if you have seen where the
prompts actually went.** llm-service now says so at startup:

```bash
docker logs rsync-llm-service 2>&1 | grep "explorer llm:"
```

```
explorer llm: provider=ollama offline_only=True sql_provider=ollama
  table_link_model=llama3:latest column_link_model=llama3:latest
  query_spec_model=llama3:latest sql_model=sqlcoder:latest
  ollama_base=http://ollama:11434/v1
```

`provider=ollama` and `sql_provider=ollama` is the proof. If `offline_only=True` but either provider
is anything else, the next line is an `ERROR` naming the provider that will receive your schema
metadata. Grep for both:

```bash
docker logs rsync-llm-service 2>&1 | grep -E "explorer (llm|router llm):|rank-tables llm:|WILL leave this deployment"
```

Three lines are expected, one per entry point:

| Log line | Emitted by | Covers |
|---|---|---|
| `explorer llm:` | [main.py:878](../../llm-service/src/gateway/main.py#L878) | Query-spec assembly, NL→SQL |
| `explorer router llm:` | [explorer/api.py:132](../../llm-service/src/agents/explorer/api.py#L132) | Table linking, column linking, next steps |
| `rank-tables llm:` | [explorer/rank_tables.py:84](../../llm-service/src/agents/explorer/rank_tables.py#L84) | Table recommendations during pipeline setup |

They read the same resolver, so they must agree; if they ever don't, that is a bug worth filing.
`rank-tables llm:` is the newest of the three and was added because that endpoint *didn't* read the
shared resolver — it sniffed `OPENAI_API_KEY` directly and ignored `EXPLORER_OFFLINE_ONLY`, so it
sent table names, column names and row counts to `api.openai.com` on a deployment whose other two
lines both said `ollama`. Two-out-of-three is not offline. Check all three.

The container's own view of the variable is the other half of the check:

```bash
docker exec rsync-llm-service printenv EXPLORER_OFFLINE_ONLY
```

---

## Models

Two separate model pools. The Explorer does not use the general default.

### General agents (planner, tool-generator, chat, healer)

| Setting | Default | Source |
|---|---|---|
| `LLM_MODEL` | unset — **overrides everything when set** | [openai_client.py:125](../../llm-service/src/utils/openai_client.py#L125) |
| `OLLAMA_MODEL` | `qwen2.5:7b` | `:130` |

### Data Explorer

Set only when `EXPLORER_LLM_PROVIDER` resolves to `ollama`; on OpenAI/Azure these fall back to
`LLM_MODEL`, then the Azure deployment name, then `gpt-4o-mini`
([explorer_default_model()](../../llm-service/src/utils/openai_client.py#L162)). On Azure the model
argument **is** the deployment name, so leaving both `LLM_MODEL` and `AZURE_OPENAI_DEPLOYMENT` unset
gives you a 404 `DeploymentNotFound` rather than a wrong-model answer.

| Variable | Offline default | Used for |
|---|---|---|
| `EXPLORER_TABLE_LINK_MODEL` | `llama3:latest` | Picking tables for a question |
| `EXPLORER_COLUMN_LINK_MODEL` | `llama3:latest` | Picking columns |
| `EXPLORER_QUERY_SPEC_MODEL` | `llama3:latest` | Query-spec assembly |
| `EXPLORER_NEXT_STEPS_MODEL` | `llama3:latest` | Follow-up suggestions |
| `EXPLORER_SQL_MODEL` | `OLLAMA_MODEL` or `sqlcoder:latest` | NL→SQL |
| `EXPLORER_SQL_MODEL_MYSQL` | same | NL→SQL on MySQL/MariaDB |
| `EXPLORER_SQL_FALLBACK_MODELS` | `qwen2.5:7b,llama3:latest,codellama:7b-instruct` | Retry chain when SQL generation fails ([main.py:2780](../../llm-service/src/gateway/main.py#L2780)) |
| `RANK_TABLES_MODEL` | `llama3:latest` | Table recommendations during pipeline setup ([rank_tables_default_model()](../../llm-service/src/utils/openai_client.py#L197)) |

`RANK_TABLES_MODEL` deliberately does **not** follow `LLM_MODEL` on OpenAI — ranking is a bulk
metadata task pinned to the cheap `gpt-4o-mini`, and inheriting a stack-wide upgrade to `gpt-4o`
would multiply its cost silently. Set `RANK_TABLES_MODEL` explicitly if you want it moved.

### What to actually pull

```bash
ollama pull llama3:latest      # Explorer table/column linking
ollama pull sqlcoder:latest    # NL→SQL
ollama pull qwen2.5:7b         # general agents + SQL fallback
```

Roughly 4–5 GB each; run `ollama list` after pulling for exact sizes. Budget **16 GB RAM minimum**
for Ollama plus weights, 24 GB comfortable. Only pull `codellama:7b-instruct` if you want the full
fallback chain — the first two fallbacks cover most failures.

> `nomic-embed-text` is **not** used anywhere in this codebase. Earlier revisions of this doc
> listed it as required. Do not pull it.

---

## Deployment

### Option A — bundled Ollama container (simplest)

`docker-compose.ollama.yml` is already committed. Do not hand-write a compose block.

```bash
docker compose -f docker-compose.quickstart.yml -f docker-compose.ollama.yml up -d
docker exec rsync-ollama ollama pull llama3:latest
docker exec rsync-ollama ollama pull sqlcoder:latest
docker exec rsync-ollama ollama pull qwen2.5:7b
```

Set `LLM_PROVIDER=ollama` in `.env` (`install.sh` option 2 does this). The overlay points
llm-service, tool-generator, and planner at `http://ollama:11434` and adds an `ollama_models`
volume so weights survive a restart.

**For GPU, uncomment the `deploy.resources.reservations.devices` block** at
[docker-compose.ollama.yml:21-28](../../docker-compose.ollama.yml#L21). Read the timeout section
below before deciding this is optional.

### Option B — Ollama on the host

Without the overlay, the default base URL is `http://host.docker.internal:11434`, which resolves on
Mac and Windows but **not on Linux**. On a Linux VM:

```bash
curl -fsSL https://ollama.com/install.sh | sh
sudo systemctl enable --now ollama
```

Then add `host-gateway` mapping to the services that call Ollama (llm-service, tool-generator,
planner) and point at it:

```yaml
services:
  llm-service:
    extra_hosts:
      - "host-gateway:host-gateway"
```

```bash
OLLAMA_BASE_URL=http://host-gateway:11434
```

Verify from inside the container:

```bash
docker exec rsync-ai-llm-service-1 curl -s http://host-gateway:11434/api/tags
```

### Option C — dedicated inference VM (production)

Isolate inference from the app tier. Install Ollama on its own 16 GB+ VM, then:

```bash
OLLAMA_BASE_URL=http://<private-ip>:11434
```

Keep port 11434 reachable only from the app subnet — same VPC/VNet private subnet, security-group
rule scoped to the app SG. Ollama has no authentication of its own; the network boundary *is* the
access control.

### Base URL resolution

`OLLAMA_BASE_URL` → `OLLAMA_URL` → `http://host.docker.internal:11434`, and `/v1` is appended
automatically ([openai_client.py:65-74](../../llm-service/src/utils/openai_client.py#L65)). Set the
host and port only; do not append `/v1` yourself. `OLLAMA_BASE_URL` wins when both are present.

Both names now have a passthrough on llm-service in `docker-compose.prod.yml`
([`:436`](../../docker-compose.prod.yml#L436) and [`:442`](../../docker-compose.prod.yml#L442)) and
in `docker-compose.quickstart.yml`. Until 2026-08-03 prod forwarded only `OLLAMA_URL` to
llm-service, so an operator following this page and setting `OLLAMA_BASE_URL` had it silently
dropped — the `OLLAMA_BASE_URL` line that *is* in prod at `:636` belongs to `tool-generator`, a
different service. Same defect shape as `KI-EXPLORER-OFFLINE-FLAG-NOT-DELIVERED`: the code reads a
variable that has no delivery path in the compose file that ships it.

---

## GPU is effectively mandatory for the Explorer

The api-gateway gives the Explorer's LLM step a **hard 30-second timeout**
([explorer.go:3395](../../api-gateway/internal/handlers/explorer.go#L3395)); the Python clients
allow 45 s sync / 120 s async ([openai_client.py:223](../../llm-service/src/utils/openai_client.py#L223),
[`:270`](../../llm-service/src/utils/openai_client.py#L270)). The gateway's 30 s is the binding
constraint.

CPU inference for a 7B model runs on the order of tens of seconds per request. That does not fit
inside 30 s with any margin — the Explorer will intermittently time out. **Treat a GPU as a
requirement for Explorer-on-Ollama, not an optimization.** Batch and CDC pipelines are unaffected;
they do not sit behind that timeout.

An earlier revision of this doc called CPU inference "acceptable for demo purposes." It is not, for
the Explorer. If you must demo on CPU, demo the pipeline flow and use a cloud provider for the
Explorer, or accept visible timeouts.

Reference GPU: NVIDIA T4 (AWS `g4dn.xlarge`) or better. Confirm Ollama picked it up:

```bash
docker exec rsync-ollama ollama run llama3 "hi" 2>&1 | head -5
```

Look for `using CUDA` (or `using Metal` on Apple silicon).

---

## Custom fine-tuned model

```bash
cat > Modelfile << 'EOF'
FROM llama3
SYSTEM """
You are the rsync-ai pipeline assistant. You help users create data pipelines
by understanding their data movement needs and translating them into structured
pipeline configurations.
"""
PARAMETER temperature 0.1
PARAMETER top_p 0.9
EOF

ollama create rsync-model -f Modelfile
```

Then point the relevant pool at it — `OLLAMA_MODEL=rsync-model` for general agents, or the specific
`EXPLORER_*_MODEL` variables for the Explorer. There is no `LLM_DEFAULT_MODEL` or `SQL_MODEL`
variable; earlier revisions of this doc invented both.

---

## Environment variable reference

| Variable | Description | Default |
|---|---|---|
| `LLM_PROVIDER` | `openai` · `azure` · `groq` · `ollama` | auto-detect → `ollama` if no cloud key |
| `OLLAMA_BASE_URL` | Ollama server URL (wins over `OLLAMA_URL`) | `http://host.docker.internal:11434` |
| `OLLAMA_URL` | Alternate name; set by `docker-compose.ollama.yml` | — |
| `LLM_MODEL` | Overrides the model for every provider | unset |
| `OLLAMA_MODEL` | Default Ollama model for general agents | `qwen2.5:7b` |
| `EXPLORER_OFFLINE_ONLY` | Force **only** the Explorer to Ollama | **`false`** |
| `EXPLORER_LLM_PROVIDER` | Explorer provider override | inherits `LLM_PROVIDER` |
| `EXPLORER_SQL_PROVIDER` | NL→SQL provider override | inherits Explorer provider |
| `EXPLORER_SQL_ALLOW_ONLINE` | Allow NL→SQL to use a cloud provider | `not EXPLORER_OFFLINE_ONLY` |
| `EXPLORER_TABLE_LINK_MODEL` | Table selection | `llama3:latest` (offline) |
| `EXPLORER_COLUMN_LINK_MODEL` | Column selection | `llama3:latest` (offline) |
| `EXPLORER_QUERY_SPEC_MODEL` | Query-spec assembly | `llama3:latest` (offline) |
| `EXPLORER_NEXT_STEPS_MODEL` | Follow-up suggestions | `llama3:latest` (offline) |
| `EXPLORER_SQL_MODEL` | NL→SQL | `OLLAMA_MODEL` or `sqlcoder:latest` |
| `EXPLORER_SQL_MODEL_MYSQL` | NL→SQL on MySQL/MariaDB | same |
| `EXPLORER_SQL_FALLBACK_MODELS` | SQL retry chain | `qwen2.5:7b,llama3:latest,codellama:7b-instruct` |
| `EXPLORER_SQL_OPENAI_MODEL` | NL→SQL model, applied only when the SQL provider resolves to `openai` ([main.py:2655](../../llm-service/src/gateway/main.py#L2655)) | prompt-registry `model`, else `EXPLORER_SQL_MODEL` |
| `RANK_TABLES_LLM_PROVIDER` | Provider for `/agents/rank-tables` only | inherits `EXPLORER_LLM_PROVIDER`, then `LLM_PROVIDER` |
| `RANK_TABLES_MODEL` | Model for `/agents/rank-tables` | `llama3:latest` offline, `gpt-4o-mini` on OpenAI |
| `OPENAI_API_KEY` | Present ⇒ auto-detect picks `openai` | unset |

Every row above reaches llm-service on `docker-compose.yml` (via `env_file:`),
`docker-compose.quickstart.yml`, and `docker-compose.prod.yml`. That is enforced by
[test_explorer_offline_resolution.py](../../llm-service/tests/test_explorer_offline_resolution.py),
which scrapes `os.getenv("EXPLORER_*")` / `os.getenv("RANK_TABLES_*")` out of all three resolving
modules rather than hardcoding this list — so a **new** knob fails the test until it is given a
delivery path in both compose files. Add the variable to this table and to both composes in the
same change.
