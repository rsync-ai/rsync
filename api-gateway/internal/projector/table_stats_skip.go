package projector

import (
	log "github.com/sirupsen/logrus"
)

// tableStatsSkipReason names why upsertTableStats dropped a TABLE_STATS event without
// touching the database.
//
// Every one of these used to be a bare `return nil`. TABLE_STATS is the only way the
// sink's applied counts reach pipeline_run_table_stats, so a producer that changed its
// envelope — renamed metadata.table, stopped sending pipeline_id, sent a new mode — froze
// the per-table counts, the captured-vs-applied reconciliation and the CDC after-pipeline
// trigger with no log line and no counter. The event was even stored in the run-event
// store, so it looked delivered.
//
// Now each skip is counted per reason and logged. The split between the two log levels:
//
//   - an event with a shape no producer in this repo emits (no pipeline_id, no metadata,
//     no mode, no table) is a defect somewhere upstream, and is logged at warn;
//   - a well-formed event in a mode this projector deliberately does not project is an
//     expected no-op, and is logged at debug.
//
// Warn lines are sampled at powers of two per reason (1st, 2nd, 4th, 8th, ...) and carry
// the running total, because a malformed producer emits on every flush of every table and
// one warn per event would bury the log it is trying to be noticed in. The counter itself
// is exact.
//
// Privacy: the log carries only the reason, identifiers, the skip counter and the
// producer-declared mode/source labels — nothing read from a row.
type tableStatsSkipReason int

const (
	tableStatsSkipMissingPipelineID tableStatsSkipReason = iota
	tableStatsSkipMissingMetadata
	tableStatsSkipMissingMode
	tableStatsSkipUnsupportedMode
	tableStatsSkipMissingTable
	tableStatsSkipEmptyTableName

	numTableStatsSkipReasons
)

var tableStatsSkipReasonNames = [numTableStatsSkipReasons]string{
	tableStatsSkipMissingPipelineID: "missing_pipeline_id",
	tableStatsSkipMissingMetadata:   "missing_metadata",
	tableStatsSkipMissingMode:       "missing_mode",
	tableStatsSkipUnsupportedMode:   "unsupported_mode",
	tableStatsSkipMissingTable:      "missing_table",
	tableStatsSkipEmptyTableName:    "empty_table_name",
}

func (r tableStatsSkipReason) String() string {
	if r < 0 || r >= numTableStatsSkipReasons {
		return "unknown"
	}
	return tableStatsSkipReasonNames[r]
}

// expected reports whether the skip is a deliberate no-op rather than a malformed event.
func (r tableStatsSkipReason) expected() bool {
	return r == tableStatsSkipUnsupportedMode
}

// maxSkipLabelLen bounds the producer-supplied labels copied into a skip log line.
const maxSkipLabelLen = 64

// skipTableStats records one dropped TABLE_STATS event. It always returns nil: a skip is
// not a projection failure, and returning an error would only produce a second, vaguer
// log line at the call site.
func (p *EventProjector) skipTableStats(reason tableStatsSkipReason, raw, meta map[string]interface{}) error {
	var n uint64
	if reason >= 0 && reason < numTableStatsSkipReasons {
		n = p.tableStatsSkips[reason].Add(1)
	}

	fields := log.Fields{
		"event_type":    "TABLE_STATS",
		"reason":        reason.String(),
		"skipped_total": n,
	}
	if pipelineID, _ := raw["pipeline_id"].(string); pipelineID != "" {
		fields["pipeline_id"] = truncateLabel(pipelineID)
	}
	if meta != nil {
		if mode, _ := meta["mode"].(string); mode != "" {
			fields["mode"] = truncateLabel(mode)
		}
		if source, _ := meta["source"].(string); source != "" {
			fields["source"] = truncateLabel(source)
		}
	}

	entry := log.WithFields(fields)
	if reason.expected() {
		entry.Debug("Event Projector: TABLE_STATS not projected (mode is not projected)")
		return nil
	}
	if n&(n-1) == 0 { // 1, 2, 4, 8, ... (n is never 0 for a known reason)
		entry.Warn("Event Projector: dropped a malformed TABLE_STATS event; per-table counts for this pipeline will not advance from it")
	} else {
		entry.Debug("Event Projector: dropped a malformed TABLE_STATS event")
	}
	return nil
}

// TableStatsSkipCounts reports how many TABLE_STATS events this projector has dropped,
// per reason, since it started. Reasons that never fired are included with 0 so a reader
// can tell "none dropped" from "not reported".
func (p *EventProjector) TableStatsSkipCounts() map[string]uint64 {
	out := make(map[string]uint64, numTableStatsSkipReasons)
	for r := tableStatsSkipReason(0); r < numTableStatsSkipReasons; r++ {
		out[r.String()] = p.tableStatsSkips[r].Load()
	}
	return out
}

func truncateLabel(s string) string {
	if len(s) > maxSkipLabelLen {
		return s[:maxSkipLabelLen]
	}
	return s
}
