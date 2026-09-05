package executor

import (
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"

	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

const (
	testPipelineID = "4631bd14-0000-4000-8000-000000000001"
	testExecA      = "aaaaaaaa-0000-4000-8000-000000000001"
	testExecB      = "bbbbbbbb-0000-4000-8000-000000000002"
)

// The A6 defect. Two executions of the SAME streaming_only pipeline used to mint
// two different consumer groups, so the set a customer must grant exact-match
// consumer-group ACLs over was unbounded and only knowable after the fact — and a
// group denied at join does not raise an error the UI shows, the sink just never
// gets an assignment and rows stop moving.
func TestStreamingOnlySinkGroupIsStableAcrossExecutions(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")

	first := sinkConsumerGroup(testPipelineID, testExecA, false, true)
	second := sinkConsumerGroup(testPipelineID, testExecB, false, true)

	if first != second {
		t.Fatalf("group changed between executions: %q then %q", first, second)
	}
	if strings.Contains(first, utils.SafeID8(testExecA)) {
		t.Fatalf("group %q still carries the execution id", first)
	}
}

// The bound that makes the ACL grant writable: the whole set of group ids a
// deployment will ever join is three per pipeline, derivable from the pipeline id.
func TestSinkGroupSetIsBoundedAtThreePerPipeline(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")

	seen := map[string]bool{}
	for _, exec := range []string{testExecA, testExecB, ""} {
		seen[sinkConsumerGroup(testPipelineID, exec, false, false)] = true
		seen[sinkConsumerGroup(testPipelineID, exec, true, false)] = true
		seen[sinkConsumerGroup(testPipelineID, exec, false, true)] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct group ids for one pipeline, got %d: %v", len(seen), seen)
	}
}

// A streaming_only start with no execution id used to fall through to the plain
// CDC group while still asking for start_offset=latest, so where it began reading
// depended on whether the pipeline had ever run in "initial" mode.
func TestStreamingOnlySinkGroupIsDistinctFromTheDefaultCDCGroup(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")

	stream := sinkConsumerGroup(testPipelineID, "", false, true)
	plain := sinkConsumerGroup(testPipelineID, "", false, false)
	if stream == plain {
		t.Fatalf("streaming_only and default CDC collided on %q despite wanting different start offsets", stream)
	}
}

func TestBatchBackfillSinkGroupStaysSeparate(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")

	batch := sinkConsumerGroup(testPipelineID, testExecA, true, false)
	cdc := sinkConsumerGroup(testPipelineID, testExecA, false, false)
	if batch == cdc {
		t.Fatalf("batch and CDC sinks collided on %q — start_sink dedups by consumer_group, so the second start would be a silent no-op", batch)
	}
	// isBatchBackfillTopic wins: the hybrid-CDC backfill runs under sync_mode=cdc.
	if got := sinkConsumerGroup(testPipelineID, testExecA, true, true); got != batch {
		t.Fatalf("batch topic under streaming_only gave %q, want %q", got, batch)
	}
}

// Every group id must land under the same KAFKA_TOPIC_PREFIX as the topics it
// reads, because topics and groups are granted together in one ACL set.
func TestSinkGroupsAreNamespaced(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")

	for _, tc := range []struct {
		name          string
		batch, stream bool
	}{
		{"cdc", false, false},
		{"batch", true, false},
		{"streaming_only", false, true},
	} {
		got := sinkConsumerGroup(testPipelineID, testExecA, tc.batch, tc.stream)
		if !strings.HasPrefix(got, "acme.") {
			t.Fatalf("%s group %q is outside the configured namespace", tc.name, got)
		}
	}
}

// The empty prefix is the documented migration lever; it must disable
// qualification rather than mean "use the default".
func TestSinkGroupsHonorTheEmptyPrefixMigrationLever(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")
	t.Setenv(kafkaclient.EnvTopicPrefix, "")

	got := sinkConsumerGroup(testPipelineID, testExecA, false, false)
	if want := "sink-" + utils.SafeID8(testPipelineID); got != want {
		t.Fatalf("got %q, want the unqualified %q", got, want)
	}
}

// Group ids are persisted in pipeline_dependencies and read back by
// handlers.ResolveSinkConsumerGroup on the next run; re-qualifying one must not
// produce rsync.rsync.sink-....
func TestSinkGroupQualificationIsIdempotent(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")
	t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")

	got := sinkConsumerGroup(testPipelineID, testExecA, false, false)
	if requalified := kafkaclient.Group(got); requalified != got {
		t.Fatalf("re-qualifying %q gave %q", got, requalified)
	}
}

// The rollback lever is explicit and default-off. It restores the pre-fix name so
// the old offset behaviour can be asked for deliberately — which is the whole
// point: the fresh-group reset used to be an accident of the naming.
func TestPerExecutionSinkGroupIsOptInOnly(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "")
	if got := sinkConsumerGroup(testPipelineID, testExecA, false, true); strings.Contains(got, utils.SafeID8(testExecA)) {
		t.Fatalf("per-execution naming is on by default: %q", got)
	}

	t.Setenv(sinkGroupPerExecutionEnv, "true")
	got := sinkConsumerGroup(testPipelineID, testExecA, false, true)
	if !strings.Contains(got, utils.SafeID8(testExecA)) {
		t.Fatalf("opt-in did not restore the per-execution name: %q", got)
	}
	if other := sinkConsumerGroup(testPipelineID, testExecB, false, true); other == got {
		t.Fatalf("opt-in produced the same group for two executions: %q", got)
	}
}

// With the lever on but no execution id there is nothing to make unique; falling
// back to the stable name is right, and it must still be the streaming shape.
func TestPerExecutionLeverFallsBackToTheStableNameWithoutAnExecutionID(t *testing.T) {
	t.Setenv(sinkGroupPerExecutionEnv, "1")
	t.Setenv(kafkaclient.EnvTopicPrefix, "")

	if got, want := sinkConsumerGroup(testPipelineID, "  ", false, true), "sink-"+utils.SafeID8(testPipelineID)+"-stream"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestIsStreamingOnlySink(t *testing.T) {
	cases := []struct {
		sinkMode, cdcMode string
		want              bool
	}{
		{"cdc", "streaming_only", true},
		{"cdc", "never", true},
		{"cdc", "initial", false},
		{"cdc", "", false},
		{"batch", "streaming_only", false},
		{"", "streaming_only", false},
	}
	for _, c := range cases {
		if got := isStreamingOnlySink(c.sinkMode, c.cdcMode); got != c.want {
			t.Fatalf("isStreamingOnlySink(%q, %q) = %v, want %v", c.sinkMode, c.cdcMode, got, c.want)
		}
	}
}
