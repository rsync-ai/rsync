package executor

import (
	"os"
	"strings"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"

	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

// sinkGroupPerExecutionEnv is the rollback lever for the change below. Setting it
// truthy restores the pre-fix, per-execution group id for streaming_only / never
// pipelines. It exists so the offset behaviour that used to fall out of a changing
// group name can be asked for DELIBERATELY, on a prod deployment, without a code
// change — never so it can happen by accident. Default off.
const sinkGroupPerExecutionEnv = "CDC_STREAMING_SINK_GROUP_PER_EXECUTION"

// sinkConsumerGroup names the kafka-mcp-sink consumer group for one sink start.
//
// There are exactly three shapes, all of them a function of the PIPELINE, so the
// full set a deployment will ever join is enumerable from the pipeline list alone:
//
//	sink-<pid8>            CDC streaming, the common case      (start_offset earliest)
//	sink-<pid8>-batch      hybrid-CDC batch backfill           (start_offset earliest)
//	sink-<pid8>-stream     cdc_mode streaming_only | never     (start_offset latest)
//
// The third used to be "sink-<pid8>-<eid8>" — a new group per pipeline EXECUTION.
// Deterministic, so not the per-invocation defect the DLQ drains had, but it
// carries the same two consequences. A customer writing exact-match consumer-group
// ACLs cannot enumerate a set that only exists after the fact, and a group denied
// at join does not surface as an error the UI shows: the sink simply never gets an
// assignment and the pipeline silently stops moving rows. On the broker side, each
// run leaves behind one more group in __consumer_offsets, retained for
// offsets.retention.minutes with nothing to reclaim it.
//
// OFFSET SEMANTICS — the reason this is not a rename. kafka-go applies
// ReaderConfig.StartOffset only to a group with NO committed offset for the
// partition (kafka-sink-worker main.go:176-180 says so, and startOffset() maps
// "latest" to kafka.LastOffset at main.go:4299). So the per-execution name was the
// only thing making start_offset="latest" bite on runs 2..N: every run got a
// virgin group and jumped to the tail.
//
// That reset is not worth preserving, and the reasoning is worth writing down.
// "Only stream new changes (no snapshot)" is a SOURCE-side promise, kept by
// Debezium's snapshot.mode=no_data — a streaming_only pipeline's topic never
// contains a backfill to resume into. Everything in that topic is a change event.
// So a stable group resuming from its committed offset applies exactly the changes
// that were captured while the sink was down, which is what CDC is for, and the
// apply is an idempotent PK upsert. What the old reset actually did was DISCARD
// those changes — silent data loss between runs, dressed as a feature. The two
// earlier shapes have always been stable per pipeline and resume the same way;
// this only brings the third into line.
//
// The distinct "-stream" suffix is kept rather than collapsing into "sink-<pid8>"
// because the two want different first-run offsets. Sharing one group would make
// where a streaming_only pipeline starts depend on whether it had previously run
// in "initial" mode.
//
// Qualification goes through kafkaclient.Group so the id lands under the same
// KAFKA_TOPIC_PREFIX namespace as the topics it reads, which is how the customer
// grants both in one ACL set.
func sinkConsumerGroup(pipelineID, executionID string, isBatchBackfillTopic bool, streamingOnly bool) string {
	pid := utils.SafeID8(pipelineID)

	switch {
	case isBatchBackfillTopic:
		return kafkaclient.Group("sink-" + pid + "-batch")
	case streamingOnly:
		if sinkGroupPerExecution() && strings.TrimSpace(executionID) != "" {
			return kafkaclient.Group("sink-" + pid + "-" + utils.SafeID8(executionID))
		}
		return kafkaclient.Group("sink-" + pid + "-stream")
	default:
		return kafkaclient.Group("sink-" + pid)
	}
}

// isStreamingOnlySink reports whether this sink start is for a CDC pipeline that
// was configured to skip the backfill, which is the case that reads from the tail.
func isStreamingOnlySink(sinkMode, cdcMode string) bool {
	return sinkMode == "cdc" && (cdcMode == "streaming_only" || cdcMode == "never")
}

func sinkGroupPerExecution() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(sinkGroupPerExecutionEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
