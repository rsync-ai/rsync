# Cloud-Storage Connector Configuration Schema (canonical)

**Status:** Phase 4e — Groups A/B + E–I shipped (source v1 on aws-s3/gcs/azure-blob);
Group C `partition_by` + `partition_time_granularity` wired into the Go sink for the **CDC
bronze** path (4c), `partition_by` extended to the **batch** part-file path (4d), and Group C
`max_file_rows`/`max_file_mb` file rolling now **built** for both CDC and batch (4e). Group D
`cdc_include_op` + `cdc_partition_by_op` are **wired** (4e); `cdc_layout=merged_snapshot` is
**dropped** — object-storage CDC is append-only bronze by design (immutable objects, no in-place
merge; deduped current-state is a downstream transform over the bronze files, not a sink option).
**Scope:** the shared configuration contract for the **hand-built** object-storage connectors
`aws-s3` (exists, `v1.0.0`), `gcs` (to build), `azure-blob` (to build). These are **not**
tool-generator output — edit them by hand and keep them in lockstep with this doc.

This file is the single source of truth the Phase 2–4 code mirrors. When a field changes,
change it here first, then in each connector's `metadata.json` (`configuration_schema` **and**
the duplicated `config_schema`), then in the connector/sink code.

Related: [developer-guide.md](developer-guide.md) · partitioning lives in the Go sink
(`kafka-mcp-sink`) · source parsing lives in the Python connector + `base_connector.py`.

---

## 1. Capability matrix — what "CDC/batch" means here

Object stores have **no transaction log**, so as a *source* there is no CDC — incremental is
always a last-modified cursor (industry standard; matches Fivetran + Airbyte). As a
*destination* the connector receives both batch loads and CDC streams and lands them as bronze
files.

| Role | Batch | Incremental | CDC |
|---|---|---|---|
| **Source** (file → warehouse) | ✅ full re-list | ✅ last-modified cursor | ❌ not applicable |
| **Destination** (warehouse → bronze files) | ✅ | n/a | ✅ bronze envelopes |

Consequence: **`partition_by` is a destination concept** (Groups C/D). The source side gets
**discovery + parsing + cursor** instead (Groups E–I). Keeping these apart avoids a confused schema.

---

## 2. Where each config actually takes effect (architecture reality)

Config is inert unless something reads it. The two halves run in different processes:

| Config group | Read by | Process |
|---|---|---|
| A auth, B format/layout | connector `_get_*_client`, `import_data`, `export`/`read` | Python connector |
| **C/D destination partitioning + CDC layout** | sink `cdcObjectKey` / `partKey` / `buildBronzeCDCEvent` | **Go `kafka-mcp-sink`** |
| **E–I source discovery/parsing/schema/cursor** | connector `discover_schema` + `export`, `base_connector.parse_format_to_rows` (new) | **Python connector** |

**Implication:** adding a `partition_by` field to `metadata.json` does nothing until the sink's
`cdcObjectKey`/`partKey` honor it. As of Phase 4c the **CDC bronze** path's `cdcObjectKey` honors
`partition_by` + `partition_time_granularity`, and as of Phase 4d the **batch** path's `partKey`
honors `partition_by` (rows split by partition tuple via `splitRowsForObjectPartition`, one
part-file per partition value). As of Phase 4e `max_file_rows`/`max_file_mb` roll part-files on
rows-or-bytes (CDC reuses the `cdcObjectBatcher`'s existing events|bytes|interval roll; batch
splits each partition group via `chunkRowsForFileRolling` → `part-NNNNNN-MMMM`), and Group D
`cdc_include_op` / `cdc_partition_by_op` shape the bronze envelope and the CDC partition path.
Adding source fields is self-contained in the Python connector (no sink changes) — which is why
**source-first** is cheaper.

---

## 3. INV-1 — isolation invariant (hard contract)

> **A user object-storage connector's endpoint can NEVER resolve to the internal staging MinIO.**

Internal MinIO is env-driven infra used **only** for claim-check overflow (reached by
`minio-mcp` at `MINIO_ENDPOINT_URL`, default `http://rsync-ai-minio:9000`, creds `minioadmin`).
A user `aws-s3`/`gcs`/`azure-blob` connection is **connection-record-driven**. They share the S3
protocol, so isolation must come from config provenance + an explicit guard — not the wire.

**Current guarantees (verified):**
- Internal `minio` is not user-selectable: `_INTERNAL_ONLY_CONNECTORS = {debezium, kafka-mcp-sink, minio}`
  (`llm-service/src/agents/tool_generator/agents/integration.py:37`), filtered in `api-gateway tools.go:1426`.
- `aws-s3` never reads `MINIO_*` env — its only env fallback is `AWS_S3_*`
  (`.../aws-s3/versions/v1.0.0/connector.py:86-89`).
- Claim-check staging is fail-closed to `minio-mcp` with a bucket allow-list (`claimcheck_url.go:49-81`).

**Gap (verified):** `endpoint_url` is taken verbatim with no host check
(`.../aws-s3/versions/v1.0.0/connector.py:100,113-121`). Nothing stops a user from *typing*
`http://rsync-ai-minio:9000` as their `endpoint_url`.

**Required guards (Phase 2, shared across all three providers):**
1. **Endpoint deny-guard** in the shared client builder: reject any `endpoint_url` whose host
   matches the internal staging host/alias — `rsync-ai-minio`, `minio`, `minio-mcp`, and the
   resolved host of `MINIO_ENDPOINT_URL`. **Fail closed** with a clear error.
   - *Nuance — do not over-block:* this is a self-hostable product; users legitimately point
     `aws-s3` at their own MinIO/R2/S3-compatible store on a private IP. So deny the **rsync-internal**
     host/alias specifically. A broader RFC1918/loopback denial is a **hosted-only toggle**
     (on for `app.rsync.ai`, off for self-host).
2. **No `MINIO_*` in public storage MCP container env** + a contract test asserting the connector
   never reads `MINIO_*` (locks the env fallback to `AWS_S3_*`/`GCS_*`/`AZURE_*` only).
3. Keep the existing internal-only creation-reject + claim-check fail-closed.

---

## 4. Schema conventions (mirror the existing `aws-s3` metadata.json)

Each field is a JSON-Schema-like property under `configuration_schema.properties` (and the
duplicated `config_schema.properties`). Supported props:

- `type`: `string` | `integer` | `number` | `boolean`
- `description`, `default`, `placeholder`
- `secret: true` → masked in UI (credentials)
- `enum: [...]` + `ui_widget: "select"` → dropdown
- `ui_order: N` → field ordering. **Consumed by `GenericConnectorForm`** — fields sort
  ascending by `ui_order` within each tier (missing → last).
- `applies`: `"source" | "destination" | "both"` → the form hides fields that don't match
  the chosen connection direction (a destination never shows source-only knobs like `globs`
  or `sync_mode`, and vice-versa). Absent/`"both"` = always shown. **Required fields are
  always shown regardless** (hiding one would deadlock save validation), so never give a
  required field a single-direction `applies`.
- `ui_tier`: `"basic" | "advanced"` → `basic` renders up-front; `advanced` collapses under
  "Advanced settings". When absent the form falls back to its heuristic (required/secret/
  auth-named → basic). This is how the cloud-storage form keeps the basic view to auth +
  `path_prefix`/`file_format`/`partition_by`/`partition_time_granularity`/`endpoint_url`.
- **Arrays/multi-select are NOT expressible** in the current schema/`GenericConnectorForm`.
  v1 carries multi-value fields (globs, table patterns, null tokens) as **comma-separated or
  JSON strings** parsed by the connector. (A real array widget is deferred — see §9.)

Required fields go in both the top-level `required_config` array and `configuration_schema.required`.

---

## 5. Configuration model

Legend — **Applies:** S=source, D=destination, both. **Enforced in:** Py=Python connector,
Sink=Go `kafka-mcp-sink`. v1 = in scope for this workstream; **defer** = Phase 5.

### Group A — Authentication (per provider; see §6 for deltas)

| Field | Type | Widget | Default | Req | Applies | Notes |
|---|---|---|---|---|---|---|
| `access_key_id` | string 🔒 | — | — | ✅ (s3) | both | aws-s3 |
| `secret_access_key` | string 🔒 | — | — | ✅ (s3) | both | aws-s3 |
| `region` | string | select | `us-east-1` | ✅ (s3) | both | aws-s3 enum (12 regions) |
| `bucket` | string | — | — | ✅ | both | container name for azure |
| `endpoint_url` | string | — | `""` | — | both | S3-compatible/MinIO/R2; **INV-1 deny-guard applies** |
| `role_arn` | string | — | `""` | — | both | aws-s3 IAM assume-role (+ auto `external_id`); v1 |

### Group B — Format & optimization (today's fields)

| Field | Type | Widget | Default | Applies | Enforced | Notes |
|---|---|---|---|---|---|---|
| `path_prefix` | string | — | `""` | both | Py | base key prefix |
| `file_format` | string | select | `json` | both | Py | `csv,tsv,json,jsonl,parquet,avro,orc,arrow,xlsx` |
| `compression` | string | select | `none` | both | Py | `none,gzip,bzip2,snappy,lz4,zstd` (codecs lib-gated on image) |
| `compression` = `infer` | (enum add) | select | — | S | Py | auto-detect from extension; v1 |

> **Codec reality:** `json/jsonl/csv/tsv + gzip` are safely real in `base_connector.convert_data_to_format`.
> `parquet/avro/orc/arrow/xlsx` + `snappy/lz4/zstd` are **library-gated** — only offer them when the
> image ships the dep (`pyarrow`, `fastavro`, `python-snappy`, `lz4`, `zstandard`, `openpyxl`).

### Group C — Destination partitioning (Sink-enforced)

#### Object-key layout (DMS-style)

CDC bronze objects land in a layout that reads like an AWS DMS S3 target with date-based
folder partitioning (sink `cdcObjectKey`):

```
CDC:    <prefix>/<db_or_schema>/<table>/<col=val/…><YYYY-MM-DD>[/HH]/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<offset>.<ext>
batch:  <prefix>/<dataset>/<db_or_schema>/<table>/<col=val/…>dt=<YYYY-MM-DD>/part-<offset:06d>[-<chunk>].<ext>
```

- `<prefix>` (`path_prefix`) is the pipeline namespace — the DMS "bucketFolder" model.
  There is deliberately **no** pipeline-id / dataset segment, so two pipelines writing the
  same `schema.table` must use distinct `path_prefix` values (as with DMS endpoints).
- `<db_or_schema>` / `<table>` come from the source schema/table (`cdcObjectPath` splits
  `sm.Table`). The date folder is **plain** (`2026-06-30`, no `dt=`) — DMS-style.
- The leaf leads with the **event timestamp** (`YYYYMMDD-HHMMSSmmm`, from the change's
  source commit time) for DMS readability, then a **Kafka offset** tiebreaker (and `-p<n>`
  for a multi-partition topic). The offset is what guarantees uniqueness + idempotency: a
  bare timestamp could collide when a bulk change lands many rows in the same millisecond
  (→ silent overwrite), and a redelivered message reuses the same offset → same key →
  overwrite (idempotent retry).

> This replaced the earlier `<prefix>/cdc/<topic>/<table>/…/partition=<n>/batch=<first>-<last>`
> layout (hard cutover, no flag), which leaked the Debezium topic (pipeline id), doubled the
> table name, and exposed the Kafka partition number. Nothing outside the sink reads the CDC
> key shape (no Glue/Athena registration, no manifest on the CDC path). **Batch/full-load**
> keys still carry the `<dataset>` segment + `dt=` folder + `part-<offset:06d>` (DMS's LOAD-
> counter equivalent); aligning the batch folder to the CDC shape also touches the
> orchestrator `keybuilder.go` reload-delete + the `_MANIFEST.json`/`_SUCCESS` markers, so it
> is tracked separately.

**`partition_by` validation:** a configured partition column that is absent from the row
schema (e.g. the `partition_by=event_timestamp` misconfig on a table with no such column) is
now **dropped with a one-time warning** rather than silently bucketing every row into
`__HIVE_DEFAULT_PARTITION__`. A column that is *present but null* still uses the sentinel
(legitimate null). Guard: `filterPresentPartitionColumns` in `cdcPartitionContext` (CDC) and
`splitRowsForObjectPartition` (batch).

| Field | Type | Widget | Default | Applies | Enforced | Status | Notes |
|---|---|---|---|---|---|---|---|
| `partition_by` | string | — | `""` | D | **Sink** | ✅ CDC (4c) + batch (4d) | comma-sep columns → Hive keys `col=val/`; honored by `cdcObjectKey` (CDC) and `partKey` via `splitRowsForObjectPartition` (batch). Columns absent from the row schema are dropped + warned (no silent `__HIVE_DEFAULT_PARTITION__`) |
| `partition_time_granularity` | string | select | `none` | D | **Sink** | ✅ CDC (4c) | `none,hour,day,month` → time-bucketed prefix (`dt=…[/hour=…]`); honored by `cdcObjectKey` from each event's `source_ts_ms`. **CDC only — batch stays day-level by design** (batch messages carry no per-row event ts, and the `_MANIFEST.json`/`_SUCCESS` markers anchor at the day root; finer buckets would fabricate a landing-time hour) |
| `max_file_rows` | integer | — | `0` (off) | D | **Sink** | ✅ CDC + batch (4e) | roll a new part-file every N rows (0 = off); CDC → `maxEvents`, batch → row chunker |
| `max_file_mb` | integer | — | `0` (off) | D | **Sink** | ✅ CDC + batch (4e) | roll a new part-file at ~N MB pre-compression (0 = off); CDC → `maxBytes`, batch → byte chunker |

#### File rolling (`max_file_rows` / `max_file_mb`) — Phase 4e (implemented)

**Goal:** cap each bronze object's size so downstream warehouses/query engines get
well-sized files (too small → many-files overhead; too large → poor read parallelism).

**Industry behavior (what we mirror):**
- **AWS DMS S3 target** rolls a file when **either** a size **or** a time threshold is hit,
  whichever first: full-load `MaxFileSize` (default **1 GB**), CDC `CdcMinFileSize`
  (default **32 MB**) + `CdcMaxBatchInterval` (default **60 s**). Parquet lands smaller than
  the size target because the target is measured **pre-compression**.
- **Airbyte S3 destination** chunks output to ~**200 MB** files, capped **≤1 GB**
  (warehouse-optimal); the old `part_size_mb` (multipart-upload part size) is deprecated.
- **Fivetran** sizes files automatically (not user-tunable).

**Takeaway:** the dominant knobs are **size + time** (rows-per-file is secondary). Roll on
**whichever of {rows, bytes, interval} is hit first**. Targets span 32 MB–1 GB; **128–256 MB**
is the warehouse sweet spot.

**Maps to our two write paths (as built in 4e):**
- **CDC path.** `cdcObjectBatcher` already rolls a new object on `maxEvents` (1000) **or**
  `maxBytes` (10 MB) **or** `flushInterval` (30 s) — rows+bytes+time, whichever first.
  `newCDCObjectBatcher` now lets the **destination config** `max_file_rows`/`max_file_mb`
  override `maxEvents`/`maxBytes` (via `objectFileRollLimits`) on top of the
  `cfg.KafkaSinkWorker.CDCBatching` defaults. The interval stays governed by
  `cdc_batching.flush_interval_seconds` (the AWS DMS `CdcMaxBatchInterval` analog).
- **Batch path.** One Kafka batch message used to map to one `part-NNNNNN` file (bounded only
  by the producer's `EXPORT_CHUNK_SIZE`). The consumer loop now splits each partition group
  via `chunkRowsForFileRolling(rows, max_file_rows, max_file_mb)` → `part-NNNNNN-MMMM` (one
  `objectWriteUnit` per chunk), composing with `splitRowsForObjectPartition` (partition first,
  then size within each partition). The `-MMMM` suffix appears only when rolling is on, so a
  message with rolling off keeps the byte-identical legacy `part-NNNNNN` key. Every part key
  still flows into the EOF `_MANIFEST.json`.

**Byte estimation:** true size is post-encode (parquet/gzip happen in the Python connector),
so the sink estimates pre-compression bytes (running `json.Marshal` length per row), exactly
like DMS's pre-compression target. Recommended over a connector-side post-encode roll (which
would touch all three connectors).

**Defaults:** both `0` = off (current single-file behavior, back-compat). When surfaced in the
UI, suggest `max_file_mb=128` as a starting point; `max_file_rows=0` unless row-count files are
explicitly wanted.

### Group D — Destination CDC layout (Sink-enforced) — wired (4e)

| Field | Type | Widget | Default | Applies | Enforced | Status | Notes |
|---|---|---|---|---|---|---|---|
| `cdc_include_op` | boolean | checkbox | `true` | D | **Sink** | ✅ (4e) | include the bronze envelope's `op` (I/U/D); `false` → append-only after-image log (`buildBronzeCDCEvent`) |
| `cdc_partition_by_op` | boolean | checkbox | `false` | D | **Sink** | ✅ (4e) | prepend `op=<I\|U\|D>/` (outermost partition; composes with `partition_by`) via `cdcPartitionContext` |

> **`cdc_layout` / `merged_snapshot` — dropped (not a sink concern).** Object-storage CDC is
> append-only bronze by design: the sink writes immutable objects, so there is no in-place merge.
> A deduped current-state ("merged snapshot") is a **downstream transform** over the bronze files
> (Spark/Athena/Iceberg/dbt), not a sink option. Relational/warehouse destinations *do* have a
> current-state-vs-append knob — `cdc_write_mode`/`cdcAppendMode`, default upsert/merge — but that
> is a separate, non-object-storage path.
>
> **`cdc_include_op` + `cdc_partition_by_op` together:** with both on, the in-file `op` column
> duplicates the `op=` partition column when the bronze path is registered as a Hive/Athena table;
> set `cdc_include_op=false` if you partition by op.

### Group E — Source: discovery & selection (Py) — v1

| Field | Type | Widget | Default | Applies | Enforced | Notes |
|---|---|---|---|---|---|---|
| `globs` | string (CSV of patterns) | — | `**` | S | Py | file selection beyond prefix (Airbyte streams) |
| `file_pattern` | string (regex) | — | `""` | S | Py | alt regex filter |
| `start_date` | string (datetime) | — | `""` | S | Py | modified-since cutoff |

### Group F — Source: file→table mapping (Py) — v1

| Field | Type | Widget | Default | Applies | Enforced | Notes |
|---|---|---|---|---|---|---|
| `file_mapping` | string | select | `single` | S | Py | `single` · `per_table` · `dynamic` |
| `table_patterns` | string (JSON) | — | `""` | S | Py | for `per_table`: `[{"table","glob"}]` |
| `dynamic_table_regex` | string | — | `""` | S | Py | for `dynamic`: must contain `(?<table>…)` (Fivetran named-capture) |

### Group G — Source: format parsing (Py) — v1 (CSV/JSON/JSONL/Parquet)

| Field | Type | Widget | Default | Applies | Notes |
|---|---|---|---|---|---|
| `csv_delimiter` | string | — | `,` | S | CSV/TSV |
| `csv_quote_char` | string | — | `"` | S | |
| `csv_escape_char` | string | — | `""` | S | |
| `csv_encoding` | string | — | `utf-8` | S | |
| `csv_null_values` | string (CSV) | — | `""` | S | tokens treated as NULL |
| `csv_skip_rows_before_header` | integer | — | `0` | S | |
| `csv_skip_rows_after_header` | integer | — | `0` | S | |
| `csv_header_mode` | string | select | `from_file` | S | `from_file · autogenerated · user_provided` |
| `csv_column_names` | string (CSV) | — | `""` | S | for `user_provided` |
| `csv_allow_inconsistent_rows` | boolean | checkbox | `false` | S | tolerate ragged rows |
| `json_mode` | string | select | `unpacked` | S | `unpacked` (flatten) · `packed` |
| `parquet_decimal_as_float` | boolean | checkbox | `false` | S | |

### Group H — Source: schema (Py) — v1

| Field | Type | Widget | Default | Applies | Notes |
|---|---|---|---|---|---|
| `input_schema` | string (JSON) | — | `""` | S | user-provided; overrides inference |
| `schemaless` | boolean | checkbox | `false` | S | emit `{data:…}` blob, skip inference |
| `schema_sample_files` | integer | — | `10` | S | files sampled to infer columns |
| `validation_policy` | string | select | `emit` | S | `emit · skip · wait` on schema violation |
| `primary_key` | string | — | `""` | S | dedup key for incremental upsert |

### Group I — Source: incremental & delivery (Py) — v1

| Field | Type | Widget | Default | Applies | Notes |
|---|---|---|---|---|---|
| `sync_mode` | string | select | `incremental` | S | `full` (re-list) · `incremental` (last-modified cursor) |
| `delivery_method` | string | select | `records` | S | `records` (parse→rows) · `raw_files` (byte copy, AI/RAG) |
| `preserve_directory_structure` | boolean | checkbox | `true` | S | for `raw_files` |

**Source meta columns (emitted, `_rsync_` convention):** `_rsync_source_file` (object key),
`_rsync_source_file_modified` (last-modified), `_rsync_source_row` (line/record ordinal).
These are the cursor + provenance columns (parity with Airbyte `_ab_source_file_*` / Fivetran `_file`,`_line`,`_modified`).

---

## 6. Per-provider auth deltas

All three share Groups B–I. Only Group A differs.

| Provider | Auth fields | Endpoint default | Container term |
|---|---|---|---|
| **aws-s3** | `access_key_id`🔒, `secret_access_key`🔒, `region`(enum), **or** `role_arn`(+auto `external_id`); optional `endpoint_url` | real AWS `s3.amazonaws.com` when `endpoint_url` empty | `bucket` |
| **gcs** | `service_account_json`🔒 (v1); OAuth deferred | `storage.googleapis.com` | `bucket` |
| **azure-blob** | `account_name` + (`account_key`🔒 **or** `connection_string`🔒 **or** SAS token🔒 **or** service principal `tenant_id`/`client_id`/`client_secret`🔒) | `<account>.blob.core.windows.net` | **`container`** (alias of `bucket`; sink already renames bucket→container) |

INV-1 deny-guard (§3) applies to **every** provider's resolved endpoint.

---

## 7. Competitor parity reference (Fivetran / Airbyte)

What this model covers vs defers, relative to the two reference products:

| Capability | Fivetran | Airbyte | This model |
|---|---|---|---|
| file→table mapping (named-capture regex) | ✅ | ✅ streams | ✅ Group F (`per_table`+`dynamic`) |
| glob / path patterns | ✅ | ✅ | ✅ Group E |
| CSV parsing knobs | ✅ deep | ✅ deep | ✅ Group G (core subset) |
| JSON flatten | ✅ packed/unpacked | ✅ | ✅ `json_mode` |
| Parquet read | ✅ | ✅ | ✅ v1 |
| schema inference / `input_schema` / schemaless | ✅ / — / — | ✅ / ✅ / ✅ | ✅ Group H |
| last-modified incremental + history overflow | ✅ | ✅ + `days_to_sync_if_history_is_full` | ✅ Group I (overflow handling = Phase 2 detail) |
| validation policy | — | ✅ emit/skip/wait | ✅ `validation_policy` |
| delivery: records vs raw-file copy | — | ✅ | ✅ `delivery_method` |
| **Unstructured/Document (PDF/DOCX + OCR)** | — | ✅ | **defer → Phase 5** |
| **archive (TAR/ZIP) + PGP decrypt** | ✅ | — | **defer → Phase 5** |
| Excel cell-reference | — | ✅ | **defer → Phase 5** |
| list-strategy optimization (lexicographic resume) | ✅ time-based | — | **defer → Phase 5** |
| GCS OAuth | — | ✅ | **defer → Phase 5** |

---

## 8. Deferred (Phase 5)

Unstructured/Document parsing + OCR (`auto/fast/ocr_only/hi_res`), archive (TAR/ZIP) + PGP
decryption, Excel cell-reference, `time_based_pattern_listing` list optimization, GCS OAuth,
`delivery_method=raw_files` polish (permissions/ACL sync), Avro/ORC source read.

---

## 9. UI constraints

`GenericConnectorForm.renderField` supports: enum→Select, int/number→number input,
boolean→checkbox, string/secret→(masked) text. **No array/multi-select widget.** Therefore v1
multi-value fields are CSV/JSON strings (`globs`, `csv_null_values`, `csv_column_names`,
`table_patterns`, `input_schema`). Investing in a real array widget is a separate, optional task.

**Direction-aware, tiered rendering (data-driven).** `GenericConnectorForm` filters config
fields by the field's `applies` vs the chosen `connectionType`, and splits `basic` vs
`advanced` (collapsed) by `ui_tier`, sorting by `ui_order` within each tier (see §4). These
keys are **optional and back-compatible** — a connector that omits them renders exactly as
before (all non-auth fields under "Advanced settings", no direction filtering). Cloud-storage
`metadata.json` declares them so a destination shows only auth + `path_prefix`/`file_format`/
`partition_by`/`partition_time_granularity`/`endpoint_url` up-front, with `compression`/
`max_file_*`/`cdc_*` collapsed and source-only knobs hidden.
