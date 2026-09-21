package handlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// A CDC pipeline built from a whole-database ("*") or whole-namespace
// ("<ns>.*") selection means "replicate everything here" — including tables
// that do not exist yet. Until now the sentinel that expressed that was
// expanded to the table list of the day and then thrown away, so the pipeline
// froze at that list: every table created afterwards was silently missing, and
// the only cure was for someone to notice and re-run the picker by hand.
//
// The fix stores the RULE beside its expansion. `config.selected_tables` keeps
// meaning "the tables Debezium is capturing right now"; `config.table_selection_rule`
// records how that set was chosen, and CDCTableWatcher re-applies it on a schedule.
//
//	["*"]                  every table of the source, forever
//	["public.*", "ops.*"]  every table of those namespaces
//	[]                     an exact list — never auto-add
//
// The empty rule is written deliberately, not merely left absent: narrowing a
// whole-database pipeline to a hand-picked list is exactly how a user says
// "stop adding things", and only an explicit [] can turn the watcher off again.
// A pipeline created before this field existed has no rule at all and is also
// left alone, which is the agreed default — existing pipelines keep their fixed
// list until someone edits the selection.
const tableSelectionRuleKey = "table_selection_rule"

// autoPickupSkippedKey records tables the watcher found but could NOT add,
// with the reason. Today the only reason is a missing PRIMARY KEY on a pipeline
// whose destination needs one for upsert/delete: the user must add the key (the
// product decision is that rsync never invents one), so the fact has to survive
// the sweep that discovered it instead of living in a log line.
const autoPickupSkippedKey = "auto_pickup_skipped_tables"

// persistTableSelectionRule stores the sentinel tokens of a user's selection on
// the pipeline. rawTables is the selection AS THE USER SENT IT — before
// sentinel expansion — because the sentinel is the whole point; passing the
// expanded list would persist an empty rule and quietly disable auto-pickup.
func persistTableSelectionRule(database *sql.DB, pipelineID string, rawTables []string) error {
	if database == nil {
		return fmt.Errorf("database not available")
	}
	b, err := json.Marshal(selectionRuleTokens(rawTables))
	if err != nil {
		return fmt.Errorf("failed to encode the table selection rule: %w", err)
	}
	_, err = database.Exec(`
		UPDATE pipelines
		SET config = jsonb_set(COALESCE(config, '{}'::jsonb), '{`+tableSelectionRuleKey+`}', $1::jsonb, true),
		    updated_at = NOW()
		WHERE id = $2::uuid
	`, string(b), pipelineID)
	return err
}

// decodeJSONStringArray parses a jsonb text column that holds an array of
// strings, dropping blanks. A malformed or NULL value reads as empty rather
// than as an error: every caller treats "no list" and "unreadable list" the
// same way — do nothing — and a sweep must not stall on one bad row.
func decodeJSONStringArray(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var parsed []string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil
	}
	out := make([]string, 0, len(parsed))
	for _, v := range parsed {
		if s := strings.TrimSpace(v); s != "" {
			out = append(out, s)
		}
	}
	return out
}
