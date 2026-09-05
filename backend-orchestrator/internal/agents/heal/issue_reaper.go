package heal

// issue_reaper.go — closes Sentinel issues whose component no longer exists.
//
// The Sentinel has three resolvers and they are all RE-OBSERVATION resolvers:
// batch_sentinel.go resolveStaleIssues, cdc_sentinel.go resolveLagIssue and
// cdc_wal_watchdog.go resolveWALAlert each close a row on the tick that finds the
// component healthy again. That covers the recovery path completely and the
// disappearance path not at all: delete the pipeline and there is no next
// observation, so the row it raised is unreachable by all three and stays open
// for good. Nothing else writes resolved_at.
//
// Measured on production 2026-08-03: 10 open rows, 9 of them pointing at deleted
// pipelines, and resolved_at NOT NULL on zero rows ever.
//
// The leak is not only cosmetic. issueSweepCandidatesQuery takes the newest
// IssueBatchSize rows by last_occurrence and its CTE is MATERIALIZED, so the
// LIMIT is applied to the issue scan and orphans consume slots in it before the
// join to pipelines drops them. Left unbounded, accumulated orphans eventually
// fill every slot in the batch and the issue sweep quietly becomes a no-op — the
// healer would stop seeing live issues without a single error being logged.

import (
	"context"

	log "github.com/sirupsen/logrus"
)

// uuidTextPattern matches the canonical 8-4-4-4-12 hex form, anchored.
//
// A regex and not a cast, deliberately. component_id is VARCHAR(255) holding a
// mixture, and `component_id::uuid` raises
//
//	ERROR: invalid input syntax for type uuid: "rsync_slot_abc"
//
// on the first non-UUID row, which aborts the whole statement rather than
// skipping that row. `~` returns false instead. This is the same hazard
// documented at length on issueSweepCandidatesQuery, reached from the other
// direction: that query avoids it by casting pipelines.id to text, which works
// because it only ever needs equality. Here the shape itself is the predicate,
// so there is nothing to compare against and the test has to be a match.
const uuidTextPattern = `^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`

// reapOrphanedIssuesQuery closes pipeline issues whose pipeline is gone.
//
// Three guards, each load-bearing:
//
//  1. component_type IN ('cdc_pipeline','batch_pipeline') — the only two types
//     whose component_id is meant to be a pipeline id at all. 'infrastructure'
//     rows (the Kafka/Temporal/Redis health the component-health ticker writes)
//     name a service, never a pipeline, so "no matching pipeline" is their
//     permanent normal state.
//
//  2. The UUID shape guard — component_type alone is NOT sufficient to conclude
//     component_id is a pipeline id. cdc_wal_watchdog.go:305-307 writes
//     component_type='cdc_pipeline' with the replication SLOT NAME as
//     component_id whenever the slot has no pipeline attached. That row matches
//     no pipeline by construction, and the problem it reports — an unattached
//     slot pinning WAL on the source, which is how a source disk fills up — is
//     live and ongoing. Reaping it would convert a stuck-open alarm into a
//     vanishing one, which is strictly worse than the leak this fixes.
//
//  3. NOT EXISTS over `p.id::text = c.component_id`, cast in that direction for
//     the reason spelled out on issueSweepCandidatesQuery: casting the other way
//     cannot fail for any value either column can hold.
//
// It sets resolved_at rather than DELETEing. The row is the healer's record that
// the alarm fired and when; a DELETE would erase the evidence, and an operator
// asking "did we ever notice?" after a pipeline was removed is exactly the
// question this table exists to answer.
const reapOrphanedIssuesQuery = `
		UPDATE sentinel_active_issues c
		SET resolved_at = NOW()
		WHERE c.resolved_at IS NULL
		  AND c.component_type IN ('cdc_pipeline', 'batch_pipeline')
		  AND c.component_id ~ $1
		  AND NOT EXISTS (
		      SELECT 1 FROM pipelines p WHERE p.id::text = c.component_id
		  )
		RETURNING c.id
	`

// reapOrphanedIssues closes every open pipeline issue whose pipeline no longer
// exists and reports how many it closed.
//
// Returns an error rather than swallowing one: the caller logs it and carries on
// with the sweep, but a reaper that silently returned 0 on a failing query would
// be indistinguishable from a reaper with nothing to do — the shape that let the
// batch-stall self-delete bug (#730) sit behind a green test.
func (w *HealWorker) reapOrphanedIssues(ctx context.Context) (int, error) {
	rows, err := w.DB.QueryContext(ctx, reapOrphanedIssuesQuery, uuidTextPattern)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var reaped []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return len(reaped), err
		}
		reaped = append(reaped, id)
	}
	if err := rows.Err(); err != nil {
		return len(reaped), err
	}

	if len(reaped) > 0 {
		// Info, not Debug. persistHealthToDB logs its failures at Debug and that
		// made "no errors in the log" read as proof a write had landed when it
		// proved nothing. A row transitioning to resolved is a state change an
		// operator may need to account for later, so it gets a durable line.
		log.WithFields(log.Fields{
			"count":     len(reaped),
			"issue_ids": reaped,
		}).Info("healer: reaped sentinel issues whose pipeline no longer exists")
	}
	return len(reaped), nil
}
