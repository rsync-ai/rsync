package executor

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// A CDC data topic used to be born with one partition however many brokers the customer
// attached: nothing created it deliberately, so the BROKER auto-created it at its own
// num.partitions. One partition is one leader, so a three-node cluster carried all of a
// topic's produce and fetch traffic on one node.
//
// The shape is now derived from the live broker list and handed to Kafka Connect, which
// creates the topic through the AdminClient before it produces. This file pins the two
// halves that fail silently: the keyless clamp (which trades that throughput back for
// per-key ordering, and must say so) and the parameter names that carry the numbers
// across the module boundary into the Python connector.

// TestKeylessSourcesClampToOnePartition: Debezium keys a change event with the source
// primary key, so every version of one row lands on one partition and stays ordered at
// any partition count. A table with no key emits a null key, the producer round-robins
// it, and a partition offset stops being a total order over the table — two versions of
// one row can then be applied out of order at the destination.
func TestKeylessSourcesClampToOnePartition(t *testing.T) {
	wide := kafka.CDCTopicShape{Partitions: 3, ReplicationFactor: 3, MinInsyncReplicas: 2}

	t.Run("every selected table has a key", func(t *testing.T) {
		got, reason := clampPartitionsForKeys(wide, cdcSizeEstimate{measured: true, allHavePK: true, matched: 2})
		if !reflect.DeepEqual(got, wide) {
			t.Errorf("shape = %+v, want it untouched at %+v", got, wide)
		}
		if reason != "" {
			t.Errorf("reason = %q, want none: nothing was clamped", reason)
		}
	})

	t.Run("a selected table has no key", func(t *testing.T) {
		est := cdcSizeEstimate{measured: true, allHavePK: false, noPKTables: []string{"public.events"}, matched: 2}
		got, reason := clampPartitionsForKeys(wide, est)
		if got.Partitions != 1 {
			t.Errorf("partitions = %d, want 1: null-keyed records round-robin and lose their order", got.Partitions)
		}
		if !strings.Contains(reason, "public.events") {
			t.Errorf("reason = %q, want it to name the table that caused the clamp", reason)
		}
	})

	// The trap this case exists for: cdcSizeEstimate starts with allHavePK at its zero
	// value and estimateCDCSourceSize sets measured=false when discovery failed — but it
	// sets allHavePK=true on the way. Reading allHavePK without measured turns "we could
	// not look" into "every table has a key", which is the wrong way to be wrong.
	t.Run("discovery that did not measure is treated as keyless", func(t *testing.T) {
		got, reason := clampPartitionsForKeys(wide, cdcSizeEstimate{measured: false, allHavePK: true})
		if got.Partitions != 1 {
			t.Errorf("partitions = %d, want 1: an unmeasured source cannot promise a key", got.Partitions)
		}
		if !strings.Contains(reason, "could not be measured") {
			t.Errorf("reason = %q, want it to say the source could not be measured", reason)
		}
	})

	t.Run("the clamp only moves the partition count", func(t *testing.T) {
		got, _ := clampPartitionsForKeys(wide, cdcSizeEstimate{measured: false})
		if got.ReplicationFactor != wide.ReplicationFactor || got.MinInsyncReplicas != wide.MinInsyncReplicas {
			t.Errorf("shape = %+v, want RF and the floor untouched from %+v", got, wide)
		}
	})

	// A single-broker cluster already resolves 1. Reporting a clamp there would tell an
	// operator their keyless table cost them throughput it never had.
	t.Run("a single-partition shape reports nothing", func(t *testing.T) {
		narrow := kafka.CDCTopicShape{Partitions: 1, ReplicationFactor: 1, MinInsyncReplicas: 1}
		got, reason := clampPartitionsForKeys(narrow, cdcSizeEstimate{measured: false})
		if !reflect.DeepEqual(got, narrow) || reason != "" {
			t.Errorf("shape = %+v reason = %q, want %+v and no reason", got, reason, narrow)
		}
	})
}

func TestCDCTopicShapeParamsCarryTheWholeShape(t *testing.T) {
	params := map[string]interface{}{"existing": "kept"}
	applyCDCTopicShapeParams(params, kafka.CDCTopicShape{Partitions: 3, ReplicationFactor: 3, MinInsyncReplicas: 2})

	want := map[string]interface{}{
		"existing":                  "kept",
		"topic_partitions":          3,
		"topic_replication_factor":  3,
		"topic_min_insync_replicas": 2,
	}
	if !reflect.DeepEqual(params, want) {
		t.Fatalf("params = %#v, want %#v", params, want)
	}

	// Connect ignores a default topic-creation group that is missing either half, so
	// half a shape is not a smaller change than none — it is the same no-op, dressed up
	// in the config as a setting that is in force.
	t.Run("an unpinned floor is omitted, not sent as zero", func(t *testing.T) {
		params := map[string]interface{}{}
		applyCDCTopicShapeParams(params, kafka.CDCTopicShape{Partitions: 1, ReplicationFactor: 1})
		if _, ok := params["topic_min_insync_replicas"]; ok {
			t.Errorf("params = %#v, want no floor key when the policy pinned none", params)
		}
	})
}

// TestCDCTopicShapeParamNamesMatchTheDebeziumConnector reads the two sides of the
// boundary and checks them against each other.
//
// The orchestrator (Go) sends these names over MCP; the connector (Python) turns them
// into topic.creation.default.*. Nothing in either language checks the other, and the
// failure is silent in the worst way: the connector emits no topic.creation.* at all,
// Connect lets the broker auto-create the topic at 1 partition, and every log line on
// both sides still reads as success. The only symptom is a cluster doing single-node
// work — which is the bug this whole change exists to fix, returning unannounced.
func TestCDCTopicShapeParamNamesMatchTheDebeziumConnector(t *testing.T) {
	sent := map[string]interface{}{}
	applyCDCTopicShapeParams(sent, kafka.CDCTopicShape{Partitions: 3, ReplicationFactor: 3, MinInsyncReplicas: 2})

	// versions/<latest.json.current_version>/ is the Docker build context, so it is the
	// code that actually runs. Reading <connector>/connector.py directly would check a
	// copy that does not exist.
	root := filepath.Join("..", "..", "..", "..", "shared", "mcp-connectors", "internal", "debezium")
	version, ok := connectorpaths.ResolveCurrentVersion(root)
	if !ok {
		t.Fatalf("could not resolve the current version from %s/latest.json", root)
	}
	src, err := os.ReadFile(filepath.Join(root, "versions", version, "connector.py"))
	if err != nil {
		t.Fatalf("reading the connector that runs (%s): %v", version, err)
	}
	// Non-vacuity: a file this test could read but that no longer builds a shape would
	// pass every substring check below for the wrong reason.
	if !strings.Contains(string(src), "topic.creation.default.partitions") {
		t.Fatalf("%s/connector.py sets no topic.creation.default.partitions — the shape reaches nothing", version)
	}

	names := make([]string, 0, len(sent))
	for k := range sent {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		if !strings.Contains(string(src), `"`+name+`"`) {
			t.Errorf("the orchestrator sends %q but %s/connector.py never reads it: "+
				"Connect gets no shape and the broker auto-creates at 1 partition", name, version)
		}
	}
}
