package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// First-run destination-namespace resolution used to be reachable only through the
// table-selection HITL, which made a data-safety check contingent on how the user
// happened to word their request (KI-NSLOCK-PROBE-UNREACHABLE-WITHOUT-HITL).
//
// A prompt that NAMES its tables lets the planner resolve them itself. That pipeline
// never parks, so api-gateway's ResumeTables never runs, so the namespace is never
// probed and never locked — and the run merges into whatever is already sitting in
// the seeded namespace. Prod pipeline 12c3579c wrote 2000 rows into another
// pipeline's table that way: no probe log, no lock, no notification.
//
// The fix puts the trigger where every run passes regardless of wording — the write
// boundary, once the final table set is known and before the first row lands. The
// probe itself stays in api-gateway, which is the service that can decrypt the
// destination connection and connect out to it.

// namespaceLockHTTPClient is package-level so the probe's connect-out cost is paid
// on a pooled connection rather than a fresh one per run.
var namespaceLockHTTPClient = &http.Client{Timeout: 45 * time.Second}

// ensureDestinationNamespaceLocked resolves + locks the pipeline's destination
// namespace before the first write of this run, and adopts the answer for the rest
// of the run.
//
// FAIL-SOFT, every branch. A missing gateway URL, a refused connection, a non-200,
// an unparseable body: log and continue. Blocking a run on the control plane would
// turn a probe that has always been best-effort into a new way for pipelines to
// stop. The point of this call is to make the GOOD case reachable, not to add a
// hard dependency.
//
// There is NO write-time backstop, and this comment used to claim one. The
// destination connector's _rsync_pipelines ledger is written but never gates:
// drop_table reads it only via SELECT to_regclass(...), co-registers the
// reloading pipeline ON CONFLICT DO NOTHING, and falls through to DROP. The
// pipeline_id-equality refusal was deliberately removed in PR #121 because it
// refused EVERY reload of a shared namespace and silently dropped data; under
// the connection+namespace ownership model that replaced it, a collision on a
// shared (connection, namespace) is authorized, not an error. Ownership is
// answered by the control-plane query added in #762 instead. So fail-soft here
// is unbacked -- still the right call, since blocking is worse, but do not
// reason as though something downstream will catch it.
func (a *Agent) ensureDestinationNamespaceLocked(ctx context.Context, task *ExecutorTask) {
	if a == nil || task == nil || strings.TrimSpace(task.PipelineID) == "" {
		return
	}
	pipelineID := strings.TrimSpace(task.PipelineID)

	tables := namespaceLockTables(task)
	if len(tables) == 0 {
		// Deliberate: locking on an empty probe set would freeze the seeded
		// namespace having proven nothing about it, and the lock is permanent.
		log.WithField("pipeline_id", pipelineID).
			Debug("namespace lock: no resolved tables at the write boundary; nothing to probe")
		return
	}

	// Cheap local short-circuit for the common case (every run after the first).
	// Saves an HTTP round-trip and, more importantly, keeps the steady state of a
	// long-lived pipeline entirely off this path.
	if a.db != nil {
		var locked bool
		if err := a.db.QueryRowContext(ctx,
			`SELECT COALESCE((config->>'destination_namespace_locked')::bool, false) FROM pipelines WHERE id = $1`,
			pipelineID,
		).Scan(&locked); err == nil && locked {
			return
		}
	}

	gwURL := strings.TrimRight(os.Getenv("API_GATEWAY_INTERNAL_URL"), "/")
	if gwURL == "" {
		gwURL = strings.TrimRight(os.Getenv("API_GATEWAY_URL"), "/")
	}
	if gwURL == "" {
		gwURL = "http://api-gateway:8080"
	}
	url := fmt.Sprintf("%s/api/v1/internal/pipelines/%s/namespace/lock", gwURL, pipelineID)

	body, err := json.Marshal(map[string]interface{}{"selected_tables": tables})
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("namespace lock: marshal failed (ignored)")
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("namespace lock: build request failed (ignored)")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Source", "executor")
	// Mirror the healer + OAuth-refresh clients: set the header only when the
	// secret is present, so a local stack without it gets a visible 401 rather
	// than a request that silently looks authenticated.
	if secret := strings.TrimSpace(os.Getenv("INTERNAL_SERVICE_SECRET")); secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}

	resp, err := namespaceLockHTTPClient.Do(req)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("namespace lock: request failed; run proceeds unlocked (ignored)")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.WithFields(log.Fields{"pipeline_id": pipelineID, "status": resp.StatusCode}).
			Warn("namespace lock: non-200 from api-gateway; run proceeds unlocked (ignored)")
		return
	}

	var out struct {
		Locked    bool   `json:"locked"`
		Namespace string `json:"namespace"`
		// Reported so the RUN's own trace records that the lock relocated it and
		// reset its resume state. api-gateway does the reset itself, next to the
		// write that makes it observable — `relocated` is true exactly once in a
		// pipeline's life (the lock is one-way), so a reset that lives a network
		// hop away from it can be lost with no second chance.
		Relocated          bool  `json:"relocated"`
		CheckpointsCleared int64 `json:"checkpoints_cleared"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("namespace lock: undecodable response (ignored)")
		return
	}
	if !out.Locked || strings.TrimSpace(out.Namespace) == "" {
		// A stand-down (schema mirroring, or no seeded mapping) — not a failure.
		return
	}

	if out.Relocated {
		// Without this line a relocated run that moves 0 rows reads exactly like a
		// healthy incremental no-op: same success, same empty DATA_PLANE_METRICS.
		// KI-NSLOCK-RELOCATION-STRANDS-CHECKPOINT was found only because the
		// checkpoint was inspected by hand afterwards.
		log.WithFields(log.Fields{
			"pipeline_id":         pipelineID,
			"namespace":           strings.TrimSpace(out.Namespace),
			"checkpoints_cleared": out.CheckpointsCleared,
		}).Info("namespace lock: run relocated to a fresh namespace; resume checkpoints reset so this run re-transfers into it")
	}

	adoptResolvedNamespace(task, strings.TrimSpace(out.Namespace))
}

// adoptResolvedNamespace makes the locked namespace authoritative for the rest of
// this run.
//
// This is not cosmetic. resolveDestinationNamespace reads task.Params FIRST and the
// pipelines row LAST, so a workflow launched with the pre-lock namespace in its
// params would keep writing to the namespace the lock just moved it OFF of — the
// row would say rsync_public while the data went to public. Same reason drop_table
// and the sink must agree: a namespace divergence inside one run is worse than
// either value alone.
func adoptResolvedNamespace(task *ExecutorTask, ns string) {
	if task == nil || ns == "" {
		return
	}
	prev := ""
	if task.Params != nil {
		if v, ok := task.Params["destination_namespace"].(string); ok {
			prev = strings.TrimSpace(v)
		}
		task.Params["destination_namespace"] = ns
	}
	if task.Payload != nil {
		task.Payload["destination_namespace"] = ns
	}
	if prev != "" && !strings.EqualFold(prev, ns) {
		log.WithFields(log.Fields{"pipeline_id": task.PipelineID, "from": prev, "to": ns}).
			Warn("namespace lock: run namespace relocated by the first-run probe")
	}
}

// namespaceLockTables extracts the final table list the run is about to write,
// using the same precedence the rest of the executor uses (Params["tables"] is what
// qualifySelectedTablesForSource normalizes into, so it is preferred).
//
// Returns bare-trimmed strings; api-gateway maps source-qualified names to their
// destination table names itself (destTableProbeSet), so qualifiers are passed
// through rather than stripped here.
func namespaceLockTables(task *ExecutorTask) []string {
	if task == nil {
		return nil
	}
	for _, src := range []map[string]interface{}{task.Params, task.Payload} {
		if src == nil {
			continue
		}
		for _, key := range []string{"tables", "selected_tables"} {
			v, ok := src[key]
			if !ok || v == nil {
				continue
			}
			if out := toStringList(v); len(out) > 0 {
				return out
			}
		}
	}
	return nil
}

func toStringList(v interface{}) []string {
	var out []string
	appendTok := func(s string) {
		s = strings.TrimSpace(s)
		// An unresolved "*" / "<ns>.*" sentinel names no table. Probing on one
		// would ask the destination about a table that cannot exist and come back
		// "no collision" — a false all-clear, which is the failure mode this whole
		// path exists to prevent.
		if s == "" || s == "*" || strings.HasSuffix(s, ".*") {
			return
		}
		out = append(out, s)
	}
	switch vv := v.(type) {
	case []string:
		for _, s := range vv {
			appendTok(s)
		}
	case []interface{}:
		for _, it := range vv {
			if it == nil {
				continue
			}
			appendTok(fmt.Sprintf("%v", it))
		}
	}
	return out
}
