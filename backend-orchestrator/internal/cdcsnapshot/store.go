package cdcsnapshot

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrUnavailable means cdc_snapshot_requests does not exist yet (the gateway
// applies migration 113 at start). Callers fall back to sending the signal
// directly, which is what every orchestrator did before the queue existed.
var ErrUnavailable = errors.New("cdc snapshot request queue is not available (migration 113 not applied)")

// ErrInitialLoadOpen is BeginInitial losing a race: another run of the pipeline
// opened an initial load after this one's supersede ran and before its insert,
// and migration 118's unique index allows one open load.
var ErrInitialLoadOpen = errors.New("another run's initial load is already open for this pipeline")

// Store reads and writes cdc_snapshot_requests and cdc_object_reload_requests.
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// isUndefinedTable reports a Postgres 42P01 from either driver.
func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	var st interface{ SQLState() string }
	if errors.As(err, &st) && st.SQLState() == "42P01" {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "42P01") ||
		(strings.Contains(msg, "cdc_snapshot_requests") && strings.Contains(msg, "does not exist")) ||
		(strings.Contains(msg, "cdc_object_reload_requests") && strings.Contains(msg, "does not exist"))
}

// isOldSourceCheck is a gateway older than migration 118 rejecting source
// 'initial'. A driver that reports a SQLSTATE must report a CHECK violation, so
// another error that merely names the constraint stays an error.
func isOldSourceCheck(err error) bool {
	if err == nil || !strings.Contains(err.Error(), "cdc_snapshot_requests_source_check") {
		return false
	}
	var st interface{ SQLState() string }
	return !errors.As(err, &st) || st.SQLState() == "23514"
}

func wrap(err error) error {
	// An older schema has not recorded the initial load yet, the same as a
	// queue that does not exist.
	if isUndefinedTable(err) || isOldSourceCheck(err) {
		return ErrUnavailable
	}
	return err
}

const requestColumns = `
	id::text, pipeline_id::text, connector_name, mode, tables::text, source, status,
	attempts, completed_tables::text, COALESCE(last_error, ''), cleans_folder,
	not_before, requested_at, sent_at, last_sent_at, started_at, last_progress_at, completed_at`

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanRequest(rs rowScanner) (Request, error) {
	var (
		r                                                 Request
		tablesRaw, completedRaw                           string
		sentAt, lastSentAt, startedAt, progressAt, doneAt sql.NullTime
	)
	if err := rs.Scan(&r.ID, &r.PipelineID, &r.ConnectorName, &r.Mode, &tablesRaw, &r.Source, &r.Status,
		&r.Attempts, &completedRaw, &r.LastError, &r.CleansFolder,
		&r.NotBefore, &r.RequestedAt, &sentAt, &lastSentAt, &startedAt, &progressAt, &doneAt); err != nil {
		return Request{}, err
	}
	_ = json.Unmarshal([]byte(tablesRaw), &r.Tables)
	_ = json.Unmarshal([]byte(completedRaw), &r.CompletedTables)
	r.SentAt = nullTime(sentAt)
	r.LastSentAt = nullTime(lastSentAt)
	r.StartedAt = nullTime(startedAt)
	r.LastProgressAt = nullTime(progressAt)
	r.CompletedAt = nullTime(doneAt)
	return r, nil
}

func nullTime(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	v := t.Time
	return &v
}

func jsonList(v []string) string {
	if v == nil {
		v = []string{}
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// Insert queues a request. NotBefore zero means "send as soon as the connector
// is ready".
func (s *Store) Insert(ctx context.Context, r Request) (Request, error) {
	if s == nil || s.db == nil {
		return Request{}, ErrUnavailable
	}
	notBefore := r.NotBefore
	if notBefore.IsZero() {
		notBefore = time.Now()
	}
	row := s.db.QueryRowContext(ctx, `
		INSERT INTO cdc_snapshot_requests
			(pipeline_id, connector_name, mode, tables, source, cleans_folder, not_before)
		VALUES ($1::uuid, $2, $3, $4::jsonb, $5, $6, $7)
		RETURNING `+requestColumns,
		r.PipelineID, r.ConnectorName, r.Mode, jsonList(r.Tables), NormalizeSource(r.Source), r.CleansFolder, notBefore)
	out, err := scanRequest(row)
	if err != nil {
		return Request{}, wrap(err)
	}
	return out, nil
}

func (s *Store) list(ctx context.Context, query string, args ...interface{}) ([]Request, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, wrap(err)
	}
	defer rows.Close()
	var out []Request
	for rows.Next() {
		r, err := scanRequest(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListOpen returns every request the dispatcher still owns, oldest first.
func (s *Store) ListOpen(ctx context.Context) ([]Request, error) {
	return s.list(ctx, `SELECT `+requestColumns+` FROM cdc_snapshot_requests
		WHERE status IN ('queued', 'sent', 'started') ORDER BY requested_at`)
}

// ListRecent returns a pipeline's sent/started requests — the ones snapshot
// rows can move — and the ones that finished since `since`, whose tail rows
// must not read as a new initial load.
func (s *Store) ListRecent(ctx context.Context, pipelineID string, since time.Time) ([]Request, error) {
	return s.list(ctx, `SELECT `+requestColumns+` FROM cdc_snapshot_requests
		WHERE pipeline_id = $1::uuid
		  AND (status IN ('sent', 'started')
		       OR (status IN ('completed', 'unconfirmed', 'failed') AND updated_at >= $2))
		ORDER BY requested_at`, pipelineID, since)
}

// initialBlockedWhere is true when pipeline $1 has an open initial load, or
// one that changed at or after firstParam (the first row of the load being
// recorded): rows read before an earlier load closed are that load's.
func initialBlockedWhere(firstParam string) string {
	return `pipeline_id = $1::uuid AND source = 'initial'
	AND (status IN ('queued', 'sent', 'started') OR updated_at >= ` + firstParam + `)`
}

// InitialBlocked reports whether a load whose first row was read at first must
// not be recorded (initialBlockedWhere).
func (s *Store) InitialBlocked(ctx context.Context, pipelineID string, first time.Time) (bool, error) {
	var blocked bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM cdc_snapshot_requests WHERE `+initialBlockedWhere("$2")+`)`,
		pipelineID, first).Scan(&blocked)
	return blocked, wrap(err)
}

// InsertInitial records r as the pipeline's initial load unless one is already
// open or newer (the same guard as InitialBlocked, repeated so two orchestrators
// cannot both insert; migration 118's unique index backs it). r.SentAt is the
// first row read, and stands in for the request and send times.
func (s *Store) InsertInitial(ctx context.Context, r Request) (bool, error) {
	if s == nil || s.db == nil {
		return false, ErrUnavailable
	}
	if r.SentAt == nil {
		return false, fmt.Errorf("initial load without a start time")
	}
	var id string
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO cdc_snapshot_requests
			(pipeline_id, connector_name, mode, tables, source, status, completed_tables,
			 not_before, requested_at, sent_at, last_sent_at, started_at, last_progress_at, completed_at)
		SELECT $1::uuid, $2, $3, $4::jsonb, 'initial', $5, $6::jsonb,
			   $7, $7, $7, $7, $8, $9, $10
		 WHERE NOT EXISTS (
			SELECT 1 FROM cdc_snapshot_requests WHERE `+initialBlockedWhere("$7")+`)
		ON CONFLICT DO NOTHING
		RETURNING id::text`,
		r.PipelineID, r.ConnectorName, r.Mode, jsonList(r.Tables), r.Status, jsonList(r.CompletedTables),
		*r.SentAt, r.StartedAt, r.LastProgressAt, r.CompletedAt).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, wrap(err)
	}
	return true, nil
}

// SupersededMessage closes an initial load that a new batch load replaces.
const SupersededMessage = "replaced by a newer full load"

// BeginInitial records a batch initial load the caller runs itself (the hybrid
// executor) as started, closing any initial load still open for the pipeline —
// a run that died mid-load leaves one. A run that races another loses with
// ErrInitialLoadOpen. Its last_progress_at stays NULL: no
// snapshot row moves it, so the dispatcher leaves it to the caller (Watch), who
// closes it with Finish.
func (s *Store) BeginInitial(ctx context.Context, r Request) (Request, error) {
	if s == nil || s.db == nil {
		return Request{}, ErrUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Request{}, wrap(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
		UPDATE cdc_snapshot_requests
		   SET status = 'unconfirmed', last_error = $2, updated_at = NOW()
		 WHERE pipeline_id = $1::uuid AND source = 'initial' AND status IN ('sent', 'started')`,
		r.PipelineID, SupersededMessage); err != nil {
		return Request{}, wrap(err)
	}
	out, err := scanRequest(tx.QueryRowContext(ctx, `
		INSERT INTO cdc_snapshot_requests
			(pipeline_id, connector_name, mode, tables, source, status,
			 not_before, requested_at, sent_at, last_sent_at, started_at)
		VALUES ($1::uuid, $2, $3, $4::jsonb, 'initial', 'started', NOW(), NOW(), NOW(), NOW(), NOW())
		ON CONFLICT DO NOTHING
		RETURNING `+requestColumns,
		r.PipelineID, r.ConnectorName, r.Mode, jsonList(r.Tables)))
	if errors.Is(err, sql.ErrNoRows) {
		// Another run's load was opened after the supersede above ran; it
		// is the one the page shows.
		return Request{}, ErrInitialLoadOpen
	}
	if err != nil {
		return Request{}, wrap(err)
	}
	return out, wrap(tx.Commit())
}

// Claim moves r to sent before its signal is produced. It is conditional on
// the status and attempt count the caller read, so two dispatchers (or a
// dispatcher and a slow tick of itself) never both send one request. sent_at
// keeps the first send; last_sent_at is this one.
func (s *Store) Claim(ctx context.Context, r Request) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE cdc_snapshot_requests
		   SET status = 'sent', attempts = attempts + 1,
		       sent_at = COALESCE(sent_at, NOW()), last_sent_at = NOW(),
		       last_error = NULL, updated_at = NOW()
		 WHERE id = $1::uuid AND status = $2 AND attempts = $3`,
		r.ID, r.Status, r.Attempts)
	if err != nil {
		return false, wrap(err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// Unclaim undoes a Claim whose produce failed: the request goes back to the
// status it was claimed from, the attempt is not counted, and the error is kept
// so a request that can never be sent shows why.
func (s *Store) Unclaim(ctx context.Context, r Request, cause string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE cdc_snapshot_requests
		   SET status = $2, attempts = GREATEST(attempts - 1, 0),
		       sent_at = CASE WHEN $2 = 'queued' THEN NULL ELSE sent_at END,
		       last_sent_at = CASE WHEN $2 = 'queued' THEN NULL ELSE last_sent_at END,
		       last_error = $3, updated_at = NOW()
		 WHERE id = $1::uuid AND status = 'sent'`,
		r.ID, r.Status, truncate(cause))
	return wrap(err)
}

// SaveProgress writes what ApplyObservations changed — the table list too,
// which an initial load grows. Conditional on the request still being in
// flight, so a late flush never reopens a finished one.
func (s *Store) SaveProgress(ctx context.Context, r Request) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE cdc_snapshot_requests
		   SET status = $2, started_at = $3, last_progress_at = $4,
		       completed_tables = $5::jsonb, completed_at = $6, tables = $7::jsonb, updated_at = NOW()
		 WHERE id = $1::uuid AND status IN ('sent', 'started')`,
		r.ID, r.Status, r.StartedAt, r.LastProgressAt, jsonList(r.CompletedTables), r.CompletedAt, jsonList(r.Tables))
	return wrap(err)
}

// Finish moves an open request to a final status (or to started, for the
// "the sink already consumed the clean marker" case). Conditional on the
// status the caller read.
func (s *Store) Finish(ctx context.Context, r Request, to, cause string) (bool, error) {
	var lastErr interface{}
	if cause != "" {
		lastErr = truncate(cause)
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE cdc_snapshot_requests
		   SET status = $3,
		       last_error = COALESCE($4, last_error),
		       started_at = CASE WHEN $3 = 'started' THEN COALESCE(started_at, NOW()) ELSE started_at END,
		       last_progress_at = CASE WHEN $3 = 'started' THEN NOW() ELSE last_progress_at END,
		       completed_at = CASE WHEN $3 = 'completed' THEN NOW() ELSE completed_at END,
		       updated_at = NOW()
		 WHERE id = $1::uuid AND status = $2`,
		r.ID, r.Status, to, lastErr)
	if err != nil {
		return false, wrap(err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// WriteReloadMarkers asks the sink to empty each topic's layout-v2 folder at
// the next snapshot batch it writes for that topic (migration 113's
// cdc_object_reload_requests). An existing marker is re-armed with NOW(): the
// sink compares the batch's event time against requested_at.
func (s *Store) WriteReloadMarkers(ctx context.Context, pipelineID, requestID string, topics []string) error {
	if len(topics) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, t := range topics {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO cdc_object_reload_requests (pipeline_id, topic, requested_at, request_id)
			VALUES ($1::uuid, $2, NOW(), $3::uuid)
			ON CONFLICT (pipeline_id, topic)
			DO UPDATE SET requested_at = NOW(), request_id = EXCLUDED.request_id`,
			pipelineID, t, requestID); err != nil {
			return wrap(err)
		}
	}
	return tx.Commit()
}

// PendingReloadMarkers counts the topics whose marker the sink has not consumed
// yet. Zero after a send means the sink has already written a snapshot batch
// for every one of them.
func (s *Store) PendingReloadMarkers(ctx context.Context, pipelineID string, topics []string) (int, error) {
	if len(topics) == 0 {
		return 0, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM cdc_object_reload_requests
		 WHERE pipeline_id = $1::uuid AND topic = ANY(SELECT jsonb_array_elements_text($2::jsonb))`,
		pipelineID, jsonList(topics)).Scan(&n)
	return n, wrap(err)
}

// truncate bounds last_error. The text is a connector state or a Kafka/HTTP
// error — never row data — but a Connect trace can be long.
func truncate(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 500 {
		return s[:500] + "…"
	}
	return s
}

// String is for logs.
func (r Request) String() string {
	return fmt.Sprintf("snapshot request %s (%s, %s, %d table(s), %s)", r.ID, r.Mode, r.Source, len(r.Tables), r.Status)
}
