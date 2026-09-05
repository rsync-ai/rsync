package handlers

import (
	"context"
	"database/sql"
	"strings"

	log "github.com/sirupsen/logrus"

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

// DerivedSinkConsumerGroup is the historical name for the majority CDC shape. It is the
// FALLBACK only — never call it directly on a path that acts. Kept as one function so
// the literal "sink-" prefix has a single definition.
func DerivedSinkConsumerGroup(pipelineID string) string {
	return "sink-" + utils.SafeID8(pipelineID)
}
