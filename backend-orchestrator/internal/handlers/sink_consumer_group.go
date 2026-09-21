package handlers

import (
	"context"
	"database/sql"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/shared/kafkaclient"

	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

// sinkConsumerGroupQuery resolves one pipeline's sink consumer group from the manifest.
//
// THE BACKFILL TIEBREAK. A hybrid-CDC pipeline registers TWO kafka_sink_worker rows under
// one pipeline_id — the backfill sink ("sink-<pid8>-batch") and the streaming sink
// ("sink-<pid8>"). upsertDependency's ON CONFLICT ... DO UPDATE rewrites required_phases
// and metadata but never created_at (dependency_manifest.go:50-52), so the backfill row
// keeps the earlier timestamp and plain `created_at DESC` returns the streaming row only
// ONCE STREAMING HAS STARTED. In the window before that, the backfill row is the newest
// one, and a CDC sink restart resolving to it stop_sinks the running backfill worker and
// re-registers its group with sink_mode="cdc" and CDC topics. The metadata->>'backfill'
// key the executor records (executor.go, registerSinkWorker) is the only discriminator
// available: for a hybrid, BOTH rows carry sink_mode="cdc" and start_offset="earliest".
//
// PREFER, NEVER EXCLUDE. A pure-batch pipeline registers ONLY a backfill row —
// isBatchBackfillTopic is true for its "pipeline.<id>.data" topic — so filtering backfill
// rows out would drop this function to DerivedSinkConsumerGroup ("sink-<pid8>"), a group
// that never existed, and silently blind the lag probe at cmd/orchestrator/main.go:270.
// A preference leaves the single-row case byte-identical and only decides ties.
//
// COALESCE, NOT A BARE ->>. Rows written before this key existed have no 'backfill'
// member, and a NULL sort key sorts LAST under ASC — which for a mixed-vintage hybrid
// would rank a NEW backfill row AHEAD of a LEGACY streaming row. Treating a missing key
// as false makes legacy rows fall through to created_at DESC, i.e. exactly today's
// behaviour, until the next run rewrites them.
//
// The ordering assumes sync_mode never moves CDC -> batch: preferring an older
// non-backfill row across executions is only correct while that holds. Today the sole
// statement mutating an existing pipeline's mode is api-gateway
// internal/projector/event_projector.go:561 (`sync_mode = 'cdc'`), i.e. batch -> CDC. A
// future cdc -> batch conversion must revisit this tiebreak.
const sinkConsumerGroupQuery = `
	SELECT identifier
	FROM pipeline_dependencies
	WHERE pipeline_id = $1
	  AND kind        = 'kafka_sink_worker'
	ORDER BY COALESCE(metadata->>'backfill', 'false') = 'true' ASC,
	         (execution_id IS NOT NULL) DESC,
	         created_at DESC
	LIMIT 1
`

// The kafka-mcp-sink consumer group is the name three unrelated subsystems must agree
// on: the executor CHOOSES it, the sentinel PROBES it, and the restart path ACTS on it.
// The executor mints one of three shapes depending on mode
// (executor.go:5823-5830) —
//
//	sink-<pid8>            CDC, the common case
//	sink-<pid8>-batch      batch sink
//	sink-<pid8>-<eid8>     cdc_mode streaming_only | never
//
// — and every place that re-derived "sink-<pid8>" by hand was silently guessing the
// first shape for all three. That guess has two very different consequences depending
// on which side of the probe/act line it lands:
//
//   - On a PROBE (lag, sink_status) a wrong name is a silent no-op that reports health
//     for a pipeline whose sink is dead.
//   - On an ACTION (stop_sink / start_sink) it is worse than a no-op: stop_sink hits
//     nothing, start_sink registers a SECOND worker under the wrong group reading from
//     'earliest', and the sentinel's next probe still finds the real group absent — so
//     the rung re-fires every tick, burns its attempt cap, and escalates naming a group
//     it never started.
//
// ResolveSinkConsumerGroup ends the guessing for both by reading what the executor
// recorded. dependency_manifest.go registers the sink with kind='kafka_sink_worker' and
// identifier=<the consumer group it actually used>, so the manifest is authoritative by
// construction rather than by two copies of a naming rule staying in sync. Non-backfill
// rows win first, then execution-scoped rows, then newest — see sinkConsumerGroupQuery.
//
// It lives in this package rather than in sentinel because the dependency edge runs
// sentinel -> handlers (the sentinel calls RestartCDCSinkWorker); putting it here lets
// the probe side and the action side share one implementation instead of two that can
// drift apart again.
func ResolveSinkConsumerGroup(ctx context.Context, db *sql.DB, pipelineID string) string {
	fallback := DerivedSinkConsumerGroup(pipelineID)
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return fallback
	}
	var identifier string
	err := db.QueryRowContext(ctx, sinkConsumerGroupQuery, pipelineID).Scan(&identifier)
	if err != nil || strings.TrimSpace(identifier) == "" {
		// Fail SAFE, not closed: an unregistered sink (pre-manifest pipeline, or a
		// manifest row lost to a cascade) still gets the historical name, which is
		// correct for the majority CDC shape. Returning "" here would disarm the lag
		// probe entirely, which is the blindness this function exists to remove.
		if err != nil && err != sql.ErrNoRows {
			log.WithError(err).WithField("pipeline_id", pipelineID).
				Debug("sink consumer group: manifest lookup failed, using derived name")
		}
		return fallback
	}
	return strings.TrimSpace(identifier)
}

// sinkGroupsToStopQuery lists every sink worker a pipeline registered. Delete stops all
// of them: a hybrid pipeline holds a -batch AND a streaming worker, and
// sinkConsumerGroupQuery's LIMIT 1 names only one.
const sinkGroupsToStopQuery = `
	SELECT DISTINCT TRIM(identifier)
	FROM pipeline_dependencies
	WHERE pipeline_id = $1
	  AND kind        = 'kafka_sink_worker'
	  AND TRIM(identifier) <> ''
`

// manifestSinkGroups returns the sink consumer groups this pipeline registered, in the
// order the manifest query returns them. A failed lookup returns nothing and is logged:
// the caller still has the derived names.
func manifestSinkGroups(ctx context.Context, db *sql.DB, pipelineID string) []string {
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	rows, err := db.QueryContext(ctx, sinkGroupsToStopQuery, pipelineID)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Warn("sink consumer groups: manifest lookup failed, stopping derived names only")
		return nil
	}
	defer rows.Close()
	var groups []string
	for rows.Next() {
		var g string
		if rows.Scan(&g) == nil && g != "" {
			groups = append(groups, g)
		}
	}
	return groups
}

// derivedSinkGroups returns the consumer groups the executor can mint for this pipeline
// without an execution id -- "", "-batch" and "-stream" -- each in the namespaced
// spelling first and then the bare one a pipeline created before the namespace still
// runs under. The id is lower-cased because every producer derives it from
// pipelines.id::text, and the delete path passes the id from a URL parameter as typed.
func derivedSinkGroups(pipelineID string) []string {
	pipelineID = strings.ToLower(strings.TrimSpace(pipelineID))
	var groups []string
	for _, suffix := range []string{"", "-batch", "-stream"} {
		base := DerivedSinkConsumerGroup(pipelineID) + suffix
		groups = append(groups, kafkaclient.Group(base), base)
	}
	return uniqueSinkGroups(groups)
}

// uniqueSinkGroups concatenates the lists, dropping blanks and repeats and keeping the
// first position of each name.
func uniqueSinkGroups(lists ...[]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, list := range lists {
		for _, g := range list {
			g = strings.TrimSpace(g)
			if g == "" || seen[g] {
				continue
			}
			seen[g] = true
			out = append(out, g)
		}
	}
	return out
}

// SinkConsumerGroupsToStop names every kafka-mcp-sink worker a pipeline delete must stop:
// every manifest row first, then the derived names in both spellings, without repeats.
//
// The manifest alone is not enough. upsertDependency only logs a failed insert, so a
// hybrid pipeline can hold a row for its -batch worker and none for its streaming one;
// returning the rows alone stopped the first and left the second consuming and writing.
// A derived name with no worker behind it costs one "Worker not found".
//
// The sink keys workers by the exact consumer_group string, so a guess in the wrong
// spelling is "Worker not found" while the real worker keeps consuming and writing. The
// executor mints kafkaclient.Group("sink-<pid8>…"), while DerivedSinkConsumerGroup stays
// bare for the probe side, so both are listed. Shapes that can't be derived
// (sink-<pid8>-<eid8>) come from the manifest or from the teardown's group listing.
//
// Derived names are not checked against other pipelines here. A caller that stops them
// must first know the 8-character id is not shared (see sinkGroupsForCleanup).
func SinkConsumerGroupsToStop(ctx context.Context, db *sql.DB, pipelineID string) []string {
	if strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	return uniqueSinkGroups(manifestSinkGroups(ctx, db, pipelineID), derivedSinkGroups(pipelineID))
}

// sharedID8CleanupWarning is returned by the pre-delete cleanup when names built from the
// 8-character id were skipped because another pipeline's id starts with the same 8
// characters.
const sharedID8CleanupWarning = "some sink workers for this pipeline were not stopped: another pipeline's id " +
	"starts with the same 8 characters, so the workers named after that id could belong to it. " +
	"Check the sink service for a leftover worker of this pipeline and stop it there."

// sinkGroupsForCleanup is SinkConsumerGroupsToStop for the pre-delete cleanup, with every
// name that could belong to another pipeline dropped when the 8-character id is shared.
//
// It runs while this pipeline's row still exists. id8IsUnique counts only OTHER rows
// (`id::text <> $1`), so "unique" here means this pipeline is the only row with the
// prefix. A failed check counts as shared: a leftover worker is reported and recoverable,
// a stopped worker of a live pipeline is an outage.
//
// Under a shared id a manifest row is no proof of ownership. The executor names the
// streaming, -batch and -stream workers from the 8-character id alone, so the other
// pipeline registers and runs the very same names, and the sink keys one worker per name.
// Only rows that carry more than that id (sink-<pid8>-<eid8>) are stopped.
func sinkGroupsForCleanup(ctx context.Context, db *sql.DB, pipelineID string) (groups, warnings []string) {
	if strings.TrimSpace(pipelineID) == "" {
		return nil, nil
	}
	if id8IsUnique(ctx, db, pipelineID, utils.SafeID8(pipelineID)) {
		return SinkConsumerGroupsToStop(ctx, db, pipelineID), nil
	}
	return withoutDerivedSinkGroups(manifestSinkGroups(ctx, db, pipelineID), pipelineID), []string{sharedID8CleanupWarning}
}

// withoutDerivedSinkGroups drops from groups every name derivedSinkGroups builds for the
// pipeline, in either spelling. sinkGroupsToStopQuery already returns each name once.
func withoutDerivedSinkGroups(groups []string, pipelineID string) []string {
	derived := map[string]bool{}
	for _, g := range derivedSinkGroups(pipelineID) {
		derived[g] = true
	}
	var own []string
	for _, g := range groups {
		if !derived[g] {
			own = append(own, g)
		}
	}
	return own
}

// DerivedSinkConsumerGroup is the historical name for the majority CDC shape. It is the
// FALLBACK only — never call it directly on a path that acts. Kept as one function so
// the literal "sink-" prefix has a single definition.
func DerivedSinkConsumerGroup(pipelineID string) string {
	return "sink-" + utils.SafeID8(pipelineID)
}
