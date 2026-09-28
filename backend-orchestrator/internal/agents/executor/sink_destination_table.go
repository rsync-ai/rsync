package executor

import (
	"fmt"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/internal/storage"
)

// KI-NSPROBE-USES-SOURCE-TABLE-NAMES.
//
// Which destination table a single-table run writes is decided HERE, in the
// orchestrator, from the natural-language request ("… into table orders_archive")
// or, failing that, from the bare name of the one selected table. api-gateway's
// first-run namespace probe used to see only the SOURCE table names, so a run
// renamed by its prompt was never probed for the table it actually writes — the
// pre-existing data the probe exists to protect could be written into anyway.
//
// These helpers are the ONE definition of that decision. startKafkaMCPSink uses
// them to pin destCfg["table"] for the sink, and ensureDestinationNamespaceLocked
// uses the same ones to tell api-gateway which destination table to probe, so the
// name that is probed and the name that is written cannot drift apart.

// taskUserRequest returns the natural-language request the run was started from
// (Params first, then Payload; `user_request` before the legacy `request`).
func taskUserRequest(task *ExecutorTask) string {
	if task == nil {
		return ""
	}
	for _, src := range []map[string]interface{}{task.Params, task.Payload} {
		if src == nil {
			continue
		}
		for _, key := range []string{"user_request", "request"} {
			if s, ok := src[key].(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// taskSinkTables is the table list the sink is started with: `tables` before
// `selected_tables`, Params before Payload, first non-empty wins. Unlike
// namespaceLockTables it keeps "*" sentinels, because the sink's table COUNT
// (which decides single- vs multi-table routing) counts them.
func taskSinkTables(task *ExecutorTask) []string {
	if task == nil {
		return nil
	}
	for _, src := range []map[string]interface{}{task.Params, task.Payload} {
		if src == nil {
			continue
		}
		for _, key := range []string{"tables", "selected_tables"} {
			if v, ok := src[key]; ok && v != nil {
				if out := coerceTrimmedStringList(v); len(out) > 0 {
					return out
				}
			}
		}
	}
	return nil
}

func coerceTrimmedStringList(v interface{}) []string {
	out := make([]string, 0, 4)
	switch tv := v.(type) {
	case []string:
		for _, it := range tv {
			if s := strings.TrimSpace(it); s != "" {
				out = append(out, s)
			}
		}
	case []interface{}:
		for _, it := range tv {
			if s := strings.TrimSpace(fmt.Sprint(it)); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// taskInferredDestTable is the destination table named in the run's request, or "".
func taskInferredDestTable(task *ExecutorTask) string {
	if task == nil {
		return ""
	}
	srcType, dstType := "", ""
	if task.Source != nil {
		srcType = task.Source.Type
	}
	if task.Destination != nil {
		dstType = task.Destination.Type
	}
	_, dest := inferTablesFromUserRequest(taskUserRequest(task), srcType, dstType)
	return dest
}

// singleTableDestination is the table a run pins as destCfg["table"]: "" for a
// multi-table run (the sink routes each table to its own name), otherwise the
// table named in the request, otherwise the bare name of the one selected table.
func singleTableDestination(inferredDestTable string, tables []string) string {
	if len(tables) != 1 {
		return ""
	}
	if override := strings.TrimSpace(inferredDestTable); override != "" {
		return override
	}
	only := strings.TrimSpace(tables[0])
	if only == "" {
		return ""
	}
	if _, t := storage.ExtractSchemaAndTable(only); strings.TrimSpace(t) != "" {
		return strings.TrimSpace(t)
	}
	return only
}

// namespaceLockDestinationTables is what the run-boundary lock reports as the
// destination tables this run writes, beyond the ones api-gateway can derive from
// the source names itself. Empty for multi-table runs, whose destination tables
// ARE the source names, and for an unresolved "*" sentinel, which names nothing.
func namespaceLockDestinationTables(task *ExecutorTask) []string {
	dest := singleTableDestination(taskInferredDestTable(task), taskSinkTables(task))
	if dest == "" || dest == "*" || strings.HasSuffix(dest, ".*") {
		return nil
	}
	return []string{dest}
}
