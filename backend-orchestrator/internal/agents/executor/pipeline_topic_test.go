package executor

import (
	"testing"

	"github.com/rsync-ai/shared/kafkaclient"
)

// The batch data topic is named by the PLANNER (Python) and consumed by the SINK
// (told its subscription by this package). Those are two different processes that
// can be at two different versions, and the failure mode when they disagree is not
// an error -- the export produces into the qualified name, the sink waits on the
// unqualified one, and the run fails closed reporting an unreachable destination.
// These tests pin the only thing that makes the disagreement unrepresentable.
func TestResolvePipelineTopic(t *testing.T) {
	const pipelineID = "9dce54e4-0e50-4312-a4bf-0f38f6f041a0"

	planned := func(name string) map[string]interface{} {
		return map[string]interface{}{
			"topic_provisioning": map[string]interface{}{"topic_name": name},
		}
	}

	t.Run("unqualified planned name is qualified", func(t *testing.T) {
		// The real regression: a planner from before the namespace existed (or a
		// plan persisted before the upgrade) hands over "pipeline.<id8>.data".
		// Passed through verbatim it became the sink's subscription while the
		// export produced into "rsync.pipeline.<id8>.data".
		t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")
		if got := resolvePipelineTopic(planned("pipeline.9dce54e4.data"), pipelineID); got != "rsync.pipeline.9dce54e4.data" {
			t.Fatalf("planned topic not qualified: got %q", got)
		}
	})

	t.Run("already-qualified planned name is untouched", func(t *testing.T) {
		// Qualification must be idempotent or the current-version planner's own
		// output turns into "rsync.rsync.pipeline...".
		t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")
		if got := resolvePipelineTopic(planned("rsync.pipeline.9dce54e4.data"), pipelineID); got != "rsync.pipeline.9dce54e4.data" {
			t.Fatalf("qualified topic re-prefixed: got %q", got)
		}
	})

	t.Run("fallback name is qualified the same way", func(t *testing.T) {
		// No plan-provisioned topic: the generated name must land in the same
		// namespace as the provisioned one, or the two branches route the same
		// pipeline to two different topics.
		t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")
		if got := resolvePipelineTopic(map[string]interface{}{}, pipelineID); got != "rsync.pipeline.9dce54e4.data" {
			t.Fatalf("fallback topic not qualified: got %q", got)
		}
	})

	t.Run("empty prefix disables qualification on both branches", func(t *testing.T) {
		// The documented migration lever: KAFKA_TOPIC_PREFIX="" keeps the legacy
		// bare names so an existing deployment's live topics and committed offsets
		// stay reachable. It has to reach BOTH branches or the lever half-works.
		t.Setenv(kafkaclient.EnvTopicPrefix, "")
		if got := resolvePipelineTopic(planned("pipeline.9dce54e4.data"), pipelineID); got != "pipeline.9dce54e4.data" {
			t.Fatalf("planned topic changed with prefix disabled: got %q", got)
		}
		if got := resolvePipelineTopic(map[string]interface{}{}, pipelineID); got != "pipeline.9dce54e4.data" {
			t.Fatalf("fallback topic changed with prefix disabled: got %q", got)
		}
	})

	t.Run("blank and missing planned names fall through", func(t *testing.T) {
		// A provisioning block that failed leaves the name empty; that must take
		// the generated-name branch rather than subscribing to "".
		t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")
		for _, params := range []map[string]interface{}{
			planned(""),
			planned("   "),
			{"topic_provisioning": map[string]interface{}{"provisioned": false}},
			{"topic_provisioning": "not-a-map"},
		} {
			if got := resolvePipelineTopic(params, pipelineID); got != "rsync.pipeline.9dce54e4.data" {
				t.Fatalf("params %v did not fall back: got %q", params, got)
			}
		}
	})
}
