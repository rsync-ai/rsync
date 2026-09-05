package executor

import (
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/storage"
)

// TestResolveBatchDataset locks in the fix for the BATCH object-storage top-level
// key-prefix inconsistency found during CDC-robustness staging validation
// (pipeline 1ddbe968, PG → aws-s3): the batch data plane's two producer paths
// disagreed on the bronze prefix. Claim-checked (large) tables such as
// docs.attachments stamped the human-readable pipelines.dataset NAME slug, while
// inline (small) tables stamped nothing so the sink's batch partKey fell back to
// slugify(pipeline_id) (the ID slug). So large tables landed under the pipeline-
// NAME prefix and small ones under the pipeline-ID prefix — a reader scoped to one
// prefix silently missed the other's tables.
//
// The invariant: for object-storage destinations the batch dataset MUST be the
// pipeline-id slug for every table. Relational destinations ignore dataset and
// must pass through unchanged. (This governs the batch bronze layout only;
// streaming CDC uses the separate DMS-style layout from #585.)
func TestResolveBatchDataset(t *testing.T) {
	const pipelineID = "1ddbe968-9e28-4fb1-9a7e-362070506eab"
	const nameSlug = "cdc-validation-581-583-584" // slugify(pipeline name)
	idSlug := storage.Slugify(pipelineID)

	if idSlug == storage.Slugify(nameSlug) {
		t.Fatalf("test precondition broken: pipeline-id slug and name slug must differ")
	}

	cases := []struct {
		name            string
		objectStorage   bool
		resolvedDataset string
		want            string
	}{
		// The exact bug: object storage + the human-readable name slug (what the
		// claim-check path used to stamp) must be pinned back to the id slug.
		{"objstore_name_slug_pinned_to_id", true, nameSlug, idSlug},
		// Empty dataset (nothing persisted) → id slug (matches the sink partKey
		// fallback the inline path already relies on).
		{"objstore_empty_falls_to_id", true, "", idSlug},
		// Already the id slug → idempotent.
		{"objstore_id_slug_idempotent", true, idSlug, idSlug},
		// Relational destinations ignore dataset → pass through unchanged.
		{"relational_name_slug_unchanged", false, nameSlug, nameSlug},
		{"relational_empty_unchanged", false, "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveBatchDataset(tc.objectStorage, pipelineID, tc.resolvedDataset)
			if got != tc.want {
				t.Fatalf("resolveBatchDataset(%v, %q, %q) = %q; want %q",
					tc.objectStorage, pipelineID, tc.resolvedDataset, got, tc.want)
			}
		})
	}

	// Cross-table consistency: the two tables that split in the original bug —
	// one whose dataset resolved to the NAME slug (claim-checked docs.attachments)
	// and one that resolved to the ID slug (inline sales.orders) — must now land
	// under the IDENTICAL top-level batch prefix for the same object-storage
	// pipeline, and that prefix must be the pipeline-id slug.
	attachmentsDataset := resolveBatchDataset(true, pipelineID, nameSlug)
	ordersDataset := resolveBatchDataset(true, pipelineID, idSlug)
	if attachmentsDataset != ordersDataset {
		t.Fatalf("object-storage batch tables must share one prefix: claim-check=%q inline=%q",
			attachmentsDataset, ordersDataset)
	}
	if attachmentsDataset != idSlug {
		t.Fatalf("batch object-storage prefix %q must be the pipeline-id slug %q",
			attachmentsDataset, idSlug)
	}
}
