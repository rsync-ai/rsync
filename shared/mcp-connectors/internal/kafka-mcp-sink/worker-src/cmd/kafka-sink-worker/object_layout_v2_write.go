package main

// Object-storage layout v2 write path.
//
// A pipeline writes layout v2 only when the orchestrator says so: batch messages carry an
// object_layout block (executor), and a CDC sink is started with storage_layout_version=2
// (WorkerConfig). Everything else keeps the v1 writers byte-for-byte. The key builders
// live in object_layout.go and are pinned by shared/object_layout_golden.json; this file
// holds what v2 adds on top of them:
//
//   - LOAD numbers. A LOAD file is named LOAD%08d.parquet, and the number comes from
//     object_load_counters / object_load_reservations (migration 106). A reservation key
//     (b|<execution>|<batch_offset>|<unit> for batch, s|<topic>|<partition>|<first_offset>
//     for a CDC snapshot batch) always maps to the same number, so a redelivered message
//     rewrites the same object instead of adding one.
//   - The folder clean. A table folder is emptied before its first write and on every
//     reload (a new generation). A writer never writes a generation whose clean has not
//     succeeded, so leftover files can never sit next to new ones.
//   - The dt column rename. dt is the Hive partition key; a source column of that name
//     would collide with it in BigQuery, so it lands as dt_source.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"
)

// objectLayoutV2Msg is the object_layout block of a batch message. The executor sends it
// only for a pipeline on layout v2; nil means v1.
type objectLayoutV2Msg struct {
	PipelinePrefix string
	SourceFamily   string
	Database       string
	Schema         string
	Table          string
	Dt             string
}

// parseObjectLayoutV2Msg reads payload["object_layout"]. A block with a version other
// than 2 is an error: a writer must not guess a layout.
func parseObjectLayoutV2Msg(payload map[string]interface{}) (*objectLayoutV2Msg, error) {
	raw, ok := payload["object_layout"].(map[string]interface{})
	if !ok || raw == nil {
		return nil, nil
	}
	if v := toInt64(raw["version"]); v != 2 {
		return nil, fmt.Errorf("object_layout version %d is not supported (want 2)", v)
	}
	return &objectLayoutV2Msg{
		PipelinePrefix: strings.TrimSpace(toString(raw["pipeline_prefix"])),
		SourceFamily:   strings.TrimSpace(toString(raw["source_family"])),
		Database:       strings.TrimSpace(toString(raw["database"])),
		Schema:         strings.TrimSpace(toString(raw["schema"])),
		Table:          strings.TrimSpace(toString(raw["table"])),
		Dt:             strings.TrimSpace(toString(raw["dt"])),
	}, nil
}

// objectLayoutV2Destination reports whether layout v2 is built for this destination
// connector type: gcs, aws-s3 and azure-blob. Each stamps the rsync_* object metadata
// and implements get_cdc_offsets. minio (internal staging) and anything else stay v1.
// The orchestrator's objectLayoutV2DestSupported and the frontend's
// isLayoutV2Destination use the same list, pinned by v2_destinations in
// shared/object_layout_golden.json.
func objectLayoutV2Destination(destType string) bool {
	switch canonicalConnectorType(destType) {
	case "gcs", "aws-s3", "azure-blob":
		return true
	}
	return false
}

// objectLayoutV2CDCEnabled reports whether this CDC sink writes layout v2. The
// orchestrator only sets version 2 for a v2 destination; the destination check here
// keeps any other store on v1 even if the config says otherwise.
func objectLayoutV2CDCEnabled(cfg *WorkerConfig) bool {
	return cfg != nil && cfg.StorageLayoutVersion == 2 && objectLayoutV2Destination(cfg.DestinationConnector)
}

func batchObjectLayoutV2Table(cfg *WorkerConfig, l *objectLayoutV2Msg) objectLayoutV2Table {
	return objectLayoutV2Table{
		ConnPrefix:     objectLayoutV1Prefix(cfg.DestinationConfig),
		PipelinePrefix: l.PipelinePrefix,
		SourceFamily:   l.SourceFamily,
		Database:       l.Database,
		Schema:         l.Schema,
		Table:          l.Table,
	}
}

// cdcObjectLayoutV2Table names a CDC event's table from the Debezium source block
// (db / schema / table, or collection for MongoDB), falling back to the sink's source
// database and the last segment of sm.Table.
func cdcObjectLayoutV2Table(cfg *WorkerConfig, sm *SinkMessage) objectLayoutV2Table {
	t := objectLayoutV2Table{
		ConnPrefix:     objectLayoutV1Prefix(cfg.DestinationConfig),
		PipelinePrefix: strings.TrimSpace(cfg.DestinationNamespace),
		SourceFamily:   strings.TrimSpace(cfg.SourceFamily),
	}
	if sm == nil {
		return t
	}
	t.Database = sm.SourceDB
	if t.Database == "" {
		t.Database = strings.TrimSpace(cfg.SourceDatabase)
	}
	t.Schema = sm.SourceSchema
	t.Table = sm.SourceBareTable
	if t.Table == "" {
		t.Table = sm.Table
		if idx := strings.LastIndex(t.Table, "."); idx >= 0 && idx+1 < len(t.Table) {
			t.Table = t.Table[idx+1:]
		}
	}
	return t
}

// objectLayoutV2TableKey is object_load_counters.table_key: the namespace without its
// trailing slash ('<db>/[<schema>/]<table>').
// It also validates the whole table prefix, pipeline prefix included, so a name the
// layout rejects never reaches the store.
func objectLayoutV2TableKey(t objectLayoutV2Table) (string, error) {
	if _, err := objectLayoutV2TablePrefix(t); err != nil {
		return "", err
	}
	ns, err := objectLayoutV2Namespace(t)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(ns, "/"), nil
}

// objectLayoutV2ParquetCodec keeps the configured codec when parquet can store it and
// uses snappy otherwise (bzip2 has no parquet codec). Layout v2 always writes parquet.
func objectLayoutV2ParquetCodec(compression string) string {
	c := strings.ToLower(strings.TrimSpace(compression))
	if c == "gz" {
		c = "gzip"
	}
	switch c {
	case "none", "uncompressed", "snappy", "gzip", "zstd", "lz4", "brotli":
		return c
	case "":
		return "none"
	}
	return "snappy"
}

// objectBucketParams adds the bucket (or Azure container) the destination call targets.
func objectBucketParams(destType string, destCfg map[string]interface{}, params map[string]interface{}) {
	bucket := firstStr(destCfg, "bucket", "bucket_name")
	container := firstStr(destCfg, "container")
	if canonicalConnectorType(destType) == "azure-blob" {
		if container != "" {
			params["container"] = container
		} else if bucket != "" {
			params["container"] = bucket
		}
	} else if bucket != "" {
		params["bucket"] = bucket
	}
}

// objectLayoutV2Clean empties a table's data folder and its _rsync sidecar folder.
// delete_prefix fails closed (success only when the listing was walked to its end), so
// an error here means the folder may still hold files and the caller must not write.
func objectLayoutV2Clean(httpClient *http.Client, cfg *WorkerConfig, destType string, t objectLayoutV2Table) func(context.Context) error {
	return func(ctx context.Context) error {
		dataPrefix, err := objectLayoutV2TablePrefix(t)
		if err != nil {
			return err
		}
		sidecarPrefix, err := objectLayoutV2SidecarTablePrefix(t)
		if err != nil {
			return err
		}
		for _, p := range []string{dataPrefix, sidecarPrefix} {
			params := map[string]interface{}{"config": cfg.DestinationConfig, "prefix": p}
			objectBucketParams(destType, cfg.DestinationConfig, params)
			if _, err := callDestinationTool(ctx, httpClient, cfg, destType, "delete_prefix", params); err != nil {
				return fmt.Errorf("layout v2 folder clean %s: %w", p, err)
			}
		}
		return nil
	}
}

// objectLoadStore hands out LOAD numbers and guards the folder clean.
type objectLoadStore interface {
	// ensureClean makes sure the table's current generation has been cleaned, calling
	// clean when it has not. bump starts a new generation first (a reload), which
	// restarts LOAD numbering at 1 and always cleans.
	ensureClean(ctx context.Context, pipelineID, tableKey string, bump bool, clean func(context.Context) error) (int64, error)
	// reserveLoadSeq returns the LOAD number and object key for reservationKey in the
	// current generation, handing out the next number (and storing keyFn's key) the
	// first time the reservation key is seen.
	reserveLoadSeq(ctx context.Context, pipelineID, tableKey, reservationKey, executionID string, keyFn func(int64) (string, error)) (int64, string, error)
}

var errObjectLoadStoreUnavailable = errors.New("object layout v2 needs the pipeline database for LOAD numbers, and the sink has none")

type pgObjectLoadStore struct {
	db *sql.DB
}

func newObjectLoadStore(db *sql.DB) objectLoadStore {
	return &pgObjectLoadStore{db: db}
}

func (s *pgObjectLoadStore) ensureClean(ctx context.Context, pipelineID, tableKey string, bump bool, clean func(context.Context) error) (gen int64, err error) {
	if s == nil || s.db == nil {
		return 0, errObjectLoadStoreUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.ExecContext(ctx, `INSERT INTO object_load_counters (pipeline_id, table_key)
		VALUES ($1::uuid, $2) ON CONFLICT (pipeline_id, table_key) DO NOTHING`, pipelineID, tableKey); err != nil {
		return 0, err
	}
	var cleaned int64
	if err = tx.QueryRowContext(ctx, `SELECT generation, cleaned_generation FROM object_load_counters
		WHERE pipeline_id = $1::uuid AND table_key = $2 FOR UPDATE`, pipelineID, tableKey).Scan(&gen, &cleaned); err != nil {
		return 0, err
	}
	if bump {
		gen++
		if _, err = tx.ExecContext(ctx, `UPDATE object_load_counters SET generation = $3, next_seq = 1, updated_at = NOW()
			WHERE pipeline_id = $1::uuid AND table_key = $2`, pipelineID, tableKey, gen); err != nil {
			return 0, err
		}
	}
	if cleaned < gen {
		// The row lock is held across the delete, so a second writer for this table
		// waits here instead of writing into a folder that is being emptied.
		if err = clean(ctx); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE object_load_counters SET cleaned_generation = $3, updated_at = NOW()
			WHERE pipeline_id = $1::uuid AND table_key = $2`, pipelineID, tableKey, gen); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return gen, nil
}

func (s *pgObjectLoadStore) reserveLoadSeq(ctx context.Context, pipelineID, tableKey, reservationKey, executionID string, keyFn func(int64) (string, error)) (seq int64, destKey string, err error) {
	if s == nil || s.db == nil {
		return 0, "", errObjectLoadStoreUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, "", err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var gen, next, cleaned int64
	err = tx.QueryRowContext(ctx, `SELECT generation, next_seq, cleaned_generation FROM object_load_counters
		WHERE pipeline_id = $1::uuid AND table_key = $2 FOR UPDATE`, pipelineID, tableKey).Scan(&gen, &next, &cleaned)
	if errors.Is(err, sql.ErrNoRows) {
		err = fmt.Errorf("layout v2 table %s has no LOAD counter: its folder was never cleaned", tableKey)
		return 0, "", err
	}
	if err != nil {
		return 0, "", err
	}
	if cleaned < gen {
		err = fmt.Errorf("layout v2 table %s generation %d is not cleaned yet", tableKey, gen)
		return 0, "", err
	}
	var stored string
	err = tx.QueryRowContext(ctx, `SELECT load_seq, COALESCE(dest_key, '') FROM object_load_reservations
		WHERE pipeline_id = $1::uuid AND table_key = $2 AND generation = $3 AND reservation_key = $4`,
		pipelineID, tableKey, gen, reservationKey).Scan(&seq, &stored)
	switch {
	case err == nil:
		// A redelivery: same number, same object.
		if stored == "" {
			if stored, err = keyFn(seq); err != nil {
				return 0, "", err
			}
		}
		if err = tx.Commit(); err != nil {
			return 0, "", err
		}
		return seq, stored, nil
	case !errors.Is(err, sql.ErrNoRows):
		return 0, "", err
	}
	if next < 1 || next > 99999999 {
		err = &objectLayoutV2Error{Code: "load_seq_out_of_range"}
		return 0, "", err
	}
	if destKey, err = keyFn(next); err != nil {
		return 0, "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO object_load_reservations
		(pipeline_id, table_key, generation, reservation_key, load_seq, execution_id, dest_key)
		VALUES ($1::uuid, $2, $3, $4, $5, NULLIF($6, ''), $7)`,
		pipelineID, tableKey, gen, reservationKey, next, executionID, destKey); err != nil {
		return 0, "", err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE object_load_counters SET next_seq = $3, updated_at = NOW()
		WHERE pipeline_id = $1::uuid AND table_key = $2`, pipelineID, tableKey, next+1); err != nil {
		return 0, "", err
	}
	if err = tx.Commit(); err != nil {
		return 0, "", err
	}
	return next, destKey, nil
}

// objectLayoutV2Cleaned remembers, per process, the tables whose current generation is
// known to be cleaned, so a first-write check costs one DB round-trip per table rather
// than one per message. A reload always goes to the store.
type objectLayoutV2Cleaned map[string]bool

func (c objectLayoutV2Cleaned) ensure(ctx context.Context, store objectLoadStore, pipelineID, tableKey string, bump bool, clean func(context.Context) error) error {
	if !bump && c[tableKey] {
		return nil
	}
	if _, err := store.ensureClean(ctx, pipelineID, tableKey, bump, clean); err != nil {
		return err
	}
	c[tableKey] = true
	return nil
}

// objectLayoutV2BatchReservationKey is b|<execution_id>|<batch_offset>|<write_unit_index>.
func objectLayoutV2BatchReservationKey(executionID string, batchOffset int64, unit int) string {
	return fmt.Sprintf("b|%s|%d|%d", executionID, batchOffset, unit)
}

// objectLayoutV2SnapshotReservationKey is s|<topic>|<partition>|<first_offset>.
func objectLayoutV2SnapshotReservationKey(topic string, partition int, firstOffset int64) string {
	return fmt.Sprintf("s|%s|%d|%d", topic, partition, firstOffset)
}

// objectLayoutV2Dt is the dt folder of a batch message: the executor's run date, or
// fallback (the table's write-state date, fixed at its first message) when it sent none.
func objectLayoutV2Dt(l *objectLayoutV2Msg, fallback string) string {
	if l != nil && strings.TrimSpace(l.Dt) != "" {
		return strings.TrimSpace(l.Dt)
	}
	return strings.TrimSpace(fallback)
}

// objectLayoutV2RenameDtColumns renames every source column whose name folds to "dt"
// (BigQuery column names are case-insensitive) to dt_source, then dt_source_2, … when a
// name is taken, in the rows, the column types and the key fields. dt stays the Hive
// partition key only. Spellings are handled in sorted order so each gets the same new
// name on every message.
func objectLayoutV2RenameDtColumns(rows []map[string]interface{}, columnTypes map[string]string, keyFields []string) {
	taken := map[string]bool{}
	dtCols := map[string]bool{}
	note := func(c string) {
		taken[strings.ToLower(c)] = true
		if strings.EqualFold(c, "dt") {
			dtCols[c] = true
		}
	}
	for c := range columnTypes {
		note(c)
	}
	for _, r := range rows {
		for c := range r {
			note(c)
		}
	}
	if len(dtCols) == 0 {
		return
	}
	spellings := make([]string, 0, len(dtCols))
	for c := range dtCols {
		spellings = append(spellings, c)
	}
	sort.Strings(spellings)
	renames := map[string]string{}
	for _, c := range spellings {
		name := "dt_source"
		for i := 2; taken[name]; i++ {
			name = fmt.Sprintf("dt_source_%d", i)
		}
		taken[name] = true
		renames[c] = name
	}
	for _, r := range rows {
		for from, to := range renames {
			if v, ok := r[from]; ok {
				r[to] = v
				delete(r, from)
			}
		}
	}
	for from, to := range renames {
		if v, ok := columnTypes[from]; ok {
			columnTypes[to] = v
			delete(columnTypes, from)
		}
	}
	for i, k := range keyFields {
		if to, ok := renames[k]; ok {
			keyFields[i] = to
		}
	}
}

// objectV2Write is what writeToDestination needs to write one layout v2 LOAD file: the
// reserved key and the object metadata. nil means the v1 key layout.
type objectV2Write struct {
	Key         string
	Compression string
	Metadata    map[string]interface{}
}

// objectLayoutV2BatcherKey is the CDC batcher's batch key in layout v2: one open batch
// per (topic, partition, table) and kind, a snapshot read (LOAD file) or a change (CDC
// file).
func objectLayoutV2BatcherKey(topic string, partition int, table string, snapshot bool) string {
	kind := "cdc"
	if snapshot {
		kind = "load"
	}
	return fmt.Sprintf("%s|%d|%s|v2|%s", topic, partition, table, kind)
}

// objectLayoutV2FlushKey names a CDC batch's object in layout v2: a LOAD file for a
// snapshot batch (numbered by the store, dated by its first event), a CDC file for a
// change batch. The table folder is cleaned first when its generation has not been.
// A store or delete error is retried with the batcher's backoff; a layout error (a name
// the layout rejects) is returned at once, since a retry cannot fix it.
func (b *cdcObjectBatcher) objectLayoutV2FlushKey(ctx context.Context, batch *cdcObjectBatch, sm *SinkMessage) (string, error) {
	t := cdcObjectLayoutV2Table(b.cfg, sm)
	tableKey, err := objectLayoutV2TableKey(t)
	if err != nil {
		return "", err
	}
	ts := batch.firstEventTS
	if ts <= 0 {
		ts = time.Now().UTC().UnixMilli()
	}
	var lastErr error
	for attempt := 0; attempt <= b.params.maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(b.params.backoff*time.Duration(1<<(attempt-1)) + time.Duration(rand.Intn(250))*time.Millisecond)
		}
		key, err := b.objectLayoutV2FlushKeyOnce(ctx, batch, sm, t, tableKey, ts)
		if err == nil {
			return key, nil
		}
		var layoutErr *objectLayoutV2Error
		if errors.As(err, &layoutErr) {
			return "", err
		}
		lastErr = err
		logf("warning", "cdc layout v2 key attempt %d failed: %v", attempt+1, err)
	}
	return "", lastErr
}

func (b *cdcObjectBatcher) objectLayoutV2FlushKeyOnce(ctx context.Context, batch *cdcObjectBatch, sm *SinkMessage, t objectLayoutV2Table, tableKey string, ts int64) (string, error) {
	if err := b.v2Cleaned.ensure(ctx, b.store, b.cfg.PipelineID, tableKey, false, objectLayoutV2Clean(b.httpClient, b.cfg, b.destType, t)); err != nil {
		return "", err
	}
	if sm == nil || !sm.IsSnapshot {
		return objectLayoutV2CDCKey(t, ts, batch.partition, batch.firstOffset)
	}
	dt := time.UnixMilli(ts).UTC().Format("2006-01-02")
	_, key, err := b.store.reserveLoadSeq(ctx, b.cfg.PipelineID, tableKey,
		objectLayoutV2SnapshotReservationKey(batch.topic, batch.partition, batch.firstOffset), "",
		func(seq int64) (string, error) { return objectLayoutV2LoadKey(t, dt, seq) })
	return key, err
}

// objectLayoutV2BatchEnabled reports whether a batch message writes layout v2: the
// executor sent an object_layout block and the destination is one v2 is built for
// (objectLayoutV2Destination; any other store keeps v1).
func objectLayoutV2BatchEnabled(cfg *WorkerConfig, sm *SinkMessage) bool {
	return cfg != nil && sm != nil && sm.ObjectLayout != nil && objectLayoutV2Destination(cfg.DestinationConnector)
}

// objectLayoutV2BatchWriter prepares one batch message's layout v2 writes: the folder
// clean (a new generation when reload is set) and one reserved LOAD key per write unit.
type objectLayoutV2BatchWriter struct {
	store       objectLoadStore
	cleaned     objectLayoutV2Cleaned
	clean       func(context.Context) error
	pipelineID  string
	executionID string
	batchOffset int64
	table       objectLayoutV2Table
	dt          string
	codec       string
	reload      bool
	// reloaded is set once the reload's new generation is committed, so a retry of the
	// same message does not start another one.
	reloaded bool
}

func newObjectLayoutV2BatchWriter(store objectLoadStore, cleaned objectLayoutV2Cleaned, httpClient *http.Client, cfg *WorkerConfig, sm *SinkMessage, dt string, reload bool) *objectLayoutV2BatchWriter {
	t := batchObjectLayoutV2Table(cfg, sm.ObjectLayout)
	destType := canonicalConnectorType(cfg.DestinationConnector)
	return &objectLayoutV2BatchWriter{
		store:       store,
		cleaned:     cleaned,
		clean:       objectLayoutV2Clean(httpClient, cfg, destType, t),
		pipelineID:  cfg.PipelineID,
		executionID: strings.TrimSpace(sm.ExecutionID),
		batchOffset: sm.BatchOffset,
		table:       t,
		dt:          dt,
		codec:       objectLayoutV2ParquetCodec(objectStorageCompression(destType, cfg.DestinationConfig)),
		reload:      reload,
	}
}

// prepare validates the table's names and makes sure its folder is clean for the
// current generation, starting a new generation first on a reload's first message.
func (w *objectLayoutV2BatchWriter) prepare(ctx context.Context) error {
	tableKey, err := objectLayoutV2TableKey(w.table)
	if err != nil {
		return err
	}
	if err := objectLayoutV2CheckDt(w.dt); err != nil {
		return err
	}
	bump := w.reload && !w.reloaded
	if err := w.cleaned.ensure(ctx, w.store, w.pipelineID, tableKey, bump, w.clean); err != nil {
		return err
	}
	if bump {
		w.reloaded = true
	}
	return nil
}

// unit returns the write descriptor for write unit idx of this message. The same
// (execution, batch offset, unit) always gets the same LOAD number.
func (w *objectLayoutV2BatchWriter) unit(ctx context.Context, idx int) (*objectV2Write, error) {
	tableKey, err := objectLayoutV2TableKey(w.table)
	if err != nil {
		return nil, err
	}
	_, key, err := w.store.reserveLoadSeq(ctx, w.pipelineID, tableKey,
		objectLayoutV2BatchReservationKey(w.executionID, w.batchOffset, idx), w.executionID,
		func(seq int64) (string, error) { return objectLayoutV2LoadKey(w.table, w.dt, seq) })
	if err != nil {
		return nil, err
	}
	return &objectV2Write{
		Key:         key,
		Compression: w.codec,
		Metadata:    map[string]interface{}{"rsync_pipeline_id": w.pipelineID},
	}, nil
}
