// Package cdcsnapshot queues CDC snapshot requests (Re-snapshot, Edit tables
// with "load existing rows", the auto-pickup watcher) and follows each one until
// Debezium has read the tables.
//
// A request used to be fire-and-forget: the execute-snapshot signal was produced
// the moment the API was called. After Edit tables it could reach the OLD
// connector task, which ignores a table it does not capture yet and marks the
// signal done; a blocking snapshot's signal is acknowledged within seconds, so a
// restart mid-snapshot lost it; and the UI had nothing to show in between. Now
// the API only queues the request (cdc_snapshot_requests, migration 113), the
// Dispatcher sends it once the connector's running tasks capture every table,
// and the CDC stats consumer's snapshot rows move it to started and completed.
package cdcsnapshot

import (
	"strings"
	"time"
)

// Request statuses (the cdc_snapshot_requests.status CHECK list).
const (
	StatusQueued      = "queued"
	StatusSent        = "sent"
	StatusStarted     = "started"
	StatusCompleted   = "completed"
	StatusUnconfirmed = "unconfirmed"
	StatusFailed      = "failed"
)

// Request sources (the cdc_snapshot_requests.source CHECK list).
const (
	SourceResnapshot = "resnapshot"
	SourceTableEdit  = "table_edit"
	SourceAutoPickup = "auto_pickup"
	// SourceInitial is the pipeline's initial (full) load. The dispatcher records
	// it from the snapshot rows it sees, and the hybrid executor around its batch
	// load; a caller can never ask for one (NormalizeSource).
	SourceInitial = "initial"
)

// NormalizeSource maps a caller-supplied source onto the CHECK list; anything
// unknown is a Re-snapshot, the only request a user makes directly.
func NormalizeSource(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case SourceTableEdit:
		return SourceTableEdit
	case SourceAutoPickup:
		return SourceAutoPickup
	default:
		return SourceResnapshot
	}
}

// Request is one cdc_snapshot_requests row.
type Request struct {
	ID            string
	PipelineID    string
	ConnectorName string
	Mode          string // incremental | blocking
	// Tables are the Debezium data-collections the signal names: schema.table for
	// PostgreSQL, db.collection for MongoDB.
	Tables          []string
	Source          string
	Status          string
	Attempts        int
	CompletedTables []string
	LastError       string
	CleansFolder    bool
	NotBefore       time.Time
	RequestedAt     time.Time
	// SentAt is the FIRST send: snapshot rows are credited from here, so rows of a
	// snapshot triggered by an earlier attempt still count after a re-send.
	SentAt *time.Time
	// LastSentAt is the latest send: the "no rows yet, send again" clock.
	LastSentAt     *time.Time
	StartedAt      *time.Time
	LastProgressAt *time.Time
	CompletedAt    *time.Time
}

// Open reports whether the request still needs the dispatcher.
func (r Request) Open() bool {
	switch r.Status {
	case StatusQueued, StatusSent, StatusStarted:
		return true
	}
	return false
}

// Observation is what the CDC stats consumer saw of a snapshot of one table in
// one flush window. Table is named the way the consumer names it: the event's
// source schema (or database) plus table (or, for MongoDB, the topic's last
// segment), i.e. the same string as the request's data-collection.
type Observation struct {
	Table string
	// Rows is the number of snapshot reads (op "r").
	Rows int64
	// FirstSeen/LastSeen are the envelope ts_ms of the first and last of them —
	// the time the connector read the row, not the time it reached this process.
	FirstSeen time.Time
	LastSeen  time.Time
	// TableDone: a row carried source.snapshot "last_in_data_collection" or "last".
	TableDone bool
	// AllDone: a row carried source.snapshot "last" — the whole snapshot finished.
	AllDone bool
	// Incremental: a row carried source.snapshot "incremental". Such a snapshot
	// marks no table or snapshot end.
	Incremental bool
}
