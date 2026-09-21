# Cloud-Storage Connector Configuration Schema (canonical)

**Status:** Phase 4e — Groups A/B + E–I shipped (source v1 on aws-s3/gcs/azure-blob);
Group C `partition_by` + `partition_time_granularity` wired into the Go sink for the **CDC
bronze** path (4c), `partition_by` extended to the **batch** part-file path (4d), and Group C
`max_file_rows`/`max_file_mb` file rolling now **built** for both CDC and batch (4e). Group D
`cdc_include_op` + `cdc_partition_by_op` are **wired** (4e); `cdc_layout=merged_snapshot` is
**dropped** — object-storage CDC is append-only bronze by design (immutable objects, no in-place
merge; deduped current-state is a downstream transform over the bronze files, not a sink option).
**Scope:** the shared configuration contract for the **hand-built** object-storage connectors
`aws-s3`, `gcs` and `azure-blob`. All three ship today and are built by
`docker-compose.mcp.yml`; each one's current version lives in its own `latest.json` rather
than as a literal repeated here. These are **not**
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
| `compression` | string | select | `gzip` | both | Py | `none,gzip,bzip2` (gcs/azure) · `+snappy,lz4,zstd` (aws-s3) — codecs lib-gated on image |
| `compression` = `infer` | (enum add) | select | — | S | Py | auto-detect from extension; v1 |

> **Codec reality:** `json/jsonl/csv/tsv + gzip` are safely real in `base_connector.convert_data_to_format`.
> `parquet/avro/orc/arrow/xlsx` + `snappy/lz4/zstd` are **library-gated** — only offer them when the
> image ships the dep (`pyarrow`, `fastavro`, `python-snappy`, `lz4`, `zstandard`, `openpyxl`).
> This is why the default is **`gzip`** and not snappy: one `compression` field covers every
> `file_format` the connector offers, and gzip is the only codec valid for all of them — stdlib
> for the text formats, a real parquet codec, and present in all three enums. gcs/azure ship
> neither `python-snappy`, `lz4` nor `zstandard`, so those three are aws-s3-only.
>
> **Parquet carries its codec INSIDE the file** (per column chunk, in its own footer), so a
> compressed parquet object is still named `.parquet` — never `.parquet.gz`. `bzip2` is a valid
> wrapper codec but **not** a parquet codec, so `parquet` + `bzip2` is rejected outright rather
> than producing a file no reader accepts.
>
> **`none` means none; a missing value means the schema default.** The form seeds each field from
> this schema `default` at create and persists what it seeds, so a connection made in the form
> always stores a codec, and a stored value, `none` included, is always what the writer uses. A
> connection that never stored one (created through the API, imported, or saved by a build before
> the default was `gzip`) used to write **uncompressed**, because the sink read a missing key as
> `none` while the schema said `gzip`. Two connections to the same bucket could then write different
> codecs for no visible reason. The sink now reads a missing or blank value as the connector's own
> schema default, in one place (`objectStorageCompression`, kafka-sink-worker `main.go`) shared by
> the batch write, the CDC write and the CDC batcher. The same value names the object, so the
> `.gz` suffix and the bytes always agree. `minio` has no compression setting and keeps writing
> uncompressed. `compression_default_test.go` fails if the sink's defaults stop matching these
> schemas.
>
> **Existing connections that stored `none` still write uncompressed.** That is a real choice and
> cannot be told apart from the old default, so nothing rewrites it. To compress, open the
> connection, set Compression to `gzip` and save. Files already written keep their codec and name.

### Group C — Destination partitioning (Sink-enforced)

#### Object-key layout (DMS-style)

CDC bronze objects land in a layout that reads like an AWS DMS S3 target with date-based
folder partitioning (sink `cdcObjectKey`):

```
CDC:    <prefix>/<dataset>/<db_or_schema>/<table>/<col=val/…><time-bucket>/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<offset>.<ext>
        where <time-bucket> = YYYY-MM-DD                 when partition_time_granularity is unset/none
                            = dt=YYYY-MM-DD[/hour=HH]    when it is day/hour (month → dt=YYYY-MM)
batch:  <prefix>/<dataset>/<db_or_schema>/<table>/<col=val/…>dt=<YYYY-MM-DD>/part-<offset:06d>[-<chunk>].<ext>
```

- `<prefix>` is the connection's `path_prefix`; `<dataset>` is the slugified pipeline id
  (`cdcPipelineSegment`), the same segment batch writes. Two pipelines writing the same
  `schema.table` into one bucket/prefix therefore land in separate trees (#14 — this replaced
  the earlier no-pipeline-segment layout, under which they collided on the same folder).
- `<db_or_schema>` is the pipeline's destination **namespace** when one is set (the value the
  orchestrator logs as `resolved=`); a blank or placeholder (`default`) namespace falls back to
  the source schema/database. `<table>` comes from the source table (`cdcObjectPath` splits
  `sm.Table`). CDC and batch share the resolved folder, so the backfill and the change
  stream for one table sit side by side. Objects written under the old layout are not moved. With `partition_time_granularity` unset (or `none`, the default) the date
  folder is **plain** (`2026-06-30`, no `dt=`) — DMS-style. Setting a granularity opts into
  the **Hive** layout (`dt=2026-06-30[/hour=14]`) instead, which is what a partitioned
  external table needs: BigQuery's `hive_partitioning_mode` and Athena's partition
  projection recognise a partition only from a literal `key=value` path segment, and read a
  bare `2026-06-30` folder as another level of the table path — so no column exists to prune
  on and every query scans every date. Unset stays byte-identical to the pre-existing keys,
  so an already-registered table never has its layout move underneath it.
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

#### Layout v2 (GCS, S3, Azure Blob): no pipeline id in the path

A `gcs`, `aws-s3` or `azure-blob` pipeline created after migration 108 writes **layout v2**
instead of the layout above (executor `object_layout_v2.go`, sink `object_layout_v2_write.go`,
keys and the destination list pinned by `shared/object_layout_golden.json`, `v2_destinations`):

```
data:      <prefix>/<pipeline prefix>/<db>/[<schema>/]<table>/dt=YYYY-MM-DD/LOAD00000001.parquet
CDC:       <prefix>/<pipeline prefix>/<db>/[<schema>/]<table>/dt=YYYY-MM-DD/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<first offset>.parquet
sidecars:  <prefix>/<pipeline prefix>/_rsync/<db>/[<schema>/]<table>/dt=YYYY-MM-DD/_MANIFEST.json, _SUCCESS
```

- `<pipeline prefix>` is the path prefix entered when the tables are picked (the pipeline's
  `destination_namespace`). For these three it is **required** in the UI: lowercase letters, digits and
  underscores, starting with a letter, at most 63 characters, not `default` or a reserved
  word. It replaces the pipeline id segment, so each pipeline on one connection needs its own.
- `<db>` is the source database; PostgreSQL-family, SQL Server and Oracle sources add the
  `<schema>` folder, MongoDB and MySQL do not. Batch, initial-load, reload and CDC-snapshot rows
  are `LOAD%08d.parquet` files, numbered per table in `object_load_counters`; a redelivered
  message rewrites the same file. `-p<n>` appears only for Kafka partition > 0.
- The `_MANIFEST.json`/`_SUCCESS` sidecars sit under `_rsync/`, outside the table folder, so a
  BigQuery hive external table over `<table>/*` reads only Parquet.
- Parquet only: a `bzip2` connection writes snappy Parquet. `partition_by` and
  `partition_time_granularity` do not apply (the only partition folder is `dt=`); file rolling
  (`max_file_rows`/`max_file_mb`) still splits a batch, one LOAD file per chunk. A source column
  named `dt` is written as `dt_source`.
- A table folder is emptied before its first write and on every reload, so a new pipeline that
  reuses an old prefix replaces the files there.
- **Who gets v2.** `pipelines.storage_layout_version` is `0` (undecided) for a new pipeline.
  When its first sink starts, a GCS, S3 or Azure Blob destination with a valid prefix, a destination connection
  and a known database source (PostgreSQL family, MySQL, SQL Server, Oracle, MongoDB) records
  `2`; anything else records `1` and keeps the layout above. A prefix already used by another
  v2 pipeline on the same connection records `1`. A pipeline created before migration 108 stays
  on `1` until a **batch reload**, which moves an eligible one to `2`; a CDC sink restart never
  changes the version. A v2 pipeline that can no longer build its keys fails to start (restart
  answers 409) instead of writing v1 keys next to its v2 folders.
- Old `<prefix>/<pipeline id>/…` folders are not moved or deleted; remove them by hand.
- The internal `minio` staging store (and any other object store) keeps the layout above, and
  its path prefix stays optional. An S3 or Azure Blob pipeline whose sink first started before
  S3/Azure support was added has already recorded `1`, so it stays v1 until a batch reload.

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
| `max_file_interval_seconds` | integer | — | unset (30 s) | D | **Sink** | 🔬 CDC (unit-tested only) | longest time CDC changes wait in the sink before they are written as a file; CDC → `flushInterval` (`objectFlushIntervalOverride`). Whole seconds from 1 to 240 (why 240: see the CDC path notes below). Any other value is refused, never clamped: `start_sink` returns `status: invalid_config` with the reason, and the worker refuses it again at startup. The value is checked for every destination, so a bad value stops a relational sink too, but only object storage uses it. Batch writes are not affected. Set it on the storage connection form (advanced settings): all three connectors declare it in `metadata.json` with no default, so a blank box saves no key and the 30 s default applies. The orchestrator copies every connection key into the sink's `destination_config`, so the value applies to every CDC pipeline writing to that connection. Guard: `test_kafka_sink_flush_interval.py` (declared in both schema blocks, no default, description states the enforced bounds) |

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
- **CDC path.** `cdcObjectBatcher` rolls a new object on `maxEvents` (2000) **or**
  `maxBytes` (24 MB) **or** `flushInterval` (30 s) — rows+bytes+time, whichever first
  (defaults in `resolveCDCBatchingParams`). `newCDCObjectBatcher` lets the **destination
  config** override all three: `max_file_rows`/`max_file_mb` → `maxEvents`/`maxBytes` (via
  `objectFileRollLimits`) and `max_file_interval_seconds` → `flushInterval` (via
  `objectFlushIntervalOverride`, 1–240 s). The destination keys win over
  `kafka_sink_worker.cdc_batching`; no start path (connector, orchestrator start or restart)
  sends `kafka_sink_worker` today, so in practice the destination keys are the only way to
  change these.
  - The timer is checked after every message and on the consumer's 1 s idle poll, so a file
    on a quiet topic is written about one interval after its first change arrived.
  - Offsets are committed only after the file is written. File names come from the table,
    time bucket, partition and offset range of the batch, not from when the flush ran or
    which interval triggered it, so retrying a failed write rewrites the same object.
  - The consumer stall watchdog (`RSYNC_SINK_STALL_WATCHDOG_SECONDS`, default 60 s) counts
    buffered, uncommitted changes as records still waiting. Its window is therefore raised to
    at least interval + 30 s (`stallWindowCoveringFlush`), so a quiet topic holding a partly
    filled file is not restarted as stalled. With the 30 s default this is the same 60 s; at
    the 240 s ceiling it is 270 s.
  - Why the ceiling is 240 s: for object storage the pipeline's "last applied" time moves
    only when a file is written. The pipeline page shows a pipeline as idle once that time is
    more than 300 s old while changes are waiting (`cdcLivenessPhase`), and the CDC sentinel
    uses the same 5-minute bound (`CDC_SINK_STALE_BOUND`) for its opt-in sink restart
    (`CDC_SINK_AUTORESTART_ENABLED`, off by default). 240 s keeps a busy pipeline's writes
    inside that bound, with about a minute to spare for the 1 s poll and the write itself.
  - What a longer interval still shows:
    - Changes waiting for the timer are not yet committed, so they count as sink consumer
      lag. If more than `CDC_SINK_KAFKA_LAG_ALERT` (default 1000) are waiting, the sentinel
      raises a `sink_lag` issue, which clears after the file is written.
    - After a quiet spell of more than five minutes, the first new changes wait up to one
      interval before they are written, so the pipeline page can read idle for up to that
      long. If the sink restart is turned on and the lag alert is also over its threshold,
      the sentinel can restart the sink once during that wait. Nothing is lost: the changes
      are read again and written within one interval, before the restart cooldown (`CDC_SENTINEL_RESTART_COOLDOWN`, 5 min by default)
      allows another attempt.
  - Trade-offs: a short interval writes many small files; a long one holds more changes in
    the sink's memory and replays more from the topic after a restart.
  - Relational destinations keep their own 5 s default (`newCDCDBBatcher`) and do not use
    `max_file_interval_seconds`, although a bad value is still refused at start.
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

#### Reading a MongoDB CDC row out of bronze

A MongoDB CDC row is **not** a flat column-per-field record. The sink packs the whole document
into one field, so a bronze row looks like:

```json
{"_id": "5251", "document": {"_id": 5251, "name": "…", "started_at": {"$date": 1758…}}, "op": "u", "kafka_offset": 41}
```

Two things trip up every first query against it:

1. **`_id` appears twice with different types.** The top-level `_id` is the *key* — always a
   **string** (`"5251"`), because Kafka keys are stringified. `document._id` is the value as it
   exists in MongoDB (here the **int** `5251`). Join on the wrong one and you get zero rows, not
   an error.
2. **Dates stay in Extended JSON.** A MongoDB date is `{"$date": <millis>}`, an object — so
   `JSON_VALUE(document, '$.started_at')` returns **NULL silently** in BigQuery (`JSON_VALUE`
   only extracts scalars). The `$` in the key must also be quoted.

Correct forms:

```sql
JSON_VALUE(document, '$.name')                       -- a scalar field
JSON_VALUE(document, '$.started_at."$date"')         -- a date: reach inside $date, quote the key
JSON_QUERY(document, '$.address')                    -- a nested object, kept as JSON
```

Implementation: `shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/cmd/kafka-sink-worker/main.go:7482-7489`
(the `{_id, document}` packing) and `:7505-7570` (Extended JSON passthrough).

> The same packing is why a column mask cannot match a MongoDB CDC field
> (`KI-MONGO-CDC-MASK-SILENT-NOOP`).

#### CDC output has no completeness marker — and should not grow one inside the partition

A batch load writes a manifest; a **CDC** load writes none. There is no per-partition file that
says "this `dt=` partition is complete", so a reader cannot distinguish "no changes today" from
"the pipeline stopped". This is a **limitation, documented deliberately**, not a defect queued
for a fix (`main.go:3946`, `:6454-6462`).

The obvious fix is the wrong one. Writing `_MANIFEST.json` into a `dt=` partition **breaks the
BigQuery external table over that folder**: in a Hive-partitioned external table an underscore
prefix does **not** exclude a file, and the *last* file read sets the schema — so one manifest
makes `<table>/*` unqueryable. The v2 layout already moves sidecars out to
`…/<prefix>/_rsync/<db>/<table>/dt=…/` for exactly this reason.

So any completeness marker must live **outside the partition tree** the external table reads —
the `_rsync/` sidecar path, or pipeline metadata — never beside the data files.

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
