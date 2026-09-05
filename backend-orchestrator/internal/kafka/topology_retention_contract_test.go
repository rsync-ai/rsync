package kafka

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// Two provisioners create pipeline.domain.events and pipeline.agent.telemetry:
// EnsureAgentControlTopics here, and scripts/kafka-init-new-topics.sh in the
// bundled stack. NEITHER alters an existing topic -- the script skips on "already
// exists" and ensureTopicLocked returns early -- so whichever runs first wins
// permanently, and on a BYO/K8s deployment there is no kafka-init at all, which
// makes the orchestrator the only creator.
//
// A divergence therefore does not surface as a conflict. It surfaces as a topic
// that was born with the wrong retention: pipeline.domain.events is the canonical
// event log the api-gateway projector rebuilds read models from, so a 1-day
// retention on it discards history silently -- produce and consume keep working
// and the loss is invisible until a rebuild comes back empty.
//
// The expectations below are READ OUT OF THE SHELL SCRIPT rather than restated,
// so this fails if either side moves. Restating them would make the test agree
// with topology.go by construction, which is the one thing it must not do.
func TestSteadyStateTopicRetentionMatchesTheShellProvisioner(t *testing.T) {
	wantRetention := retentionFromInitScript(t)

	tm, admin := newFakeManager(1)
	if err := tm.EnsureAgentControlTopics(context.Background(), 3); err != nil {
		t.Fatalf("EnsureAgentControlTopics: %v", err)
	}

	for topic, want := range wantRetention {
		created, ok := admin.created[kafkaclient.Topic(topic)]
		if !ok {
			t.Errorf("%s is created by scripts/kafka-init-new-topics.sh but NOT by "+
				"EnsureAgentControlTopics -- on a BYO cluster with no kafka-init it "+
				"would exist only by broker auto-creation", topic)
			continue
		}
		got := created.ConfigEntries["retention.ms"]
		if got == nil {
			t.Errorf("%s: EnsureAgentControlTopics sets no retention.ms, so it inherits "+
				"the broker's; the shell provisioner pins %s", topic, want)
			continue
		}
		if *got != want {
			t.Errorf("%s: retention.ms = %s via EnsureAgentControlTopics but %s via "+
				"scripts/kafka-init-new-topics.sh -- whichever provisioner runs first "+
				"wins permanently, so these must agree", topic, *got, want)
		}
	}
}

// The bundled bootstrappers create pipeline.domain.events with 3 partitions
// (docker-compose.quickstart.yml) and everything else with $PARTITIONS, which
// docker-compose.yml sets to 3. Creating it at 1 here caps the api-gateway
// projector's consumer group at a single active member forever, because
// KeepExistingPartitions means nothing can widen it afterwards.
func TestSteadyStateTopicsAreNotBornSinglePartition(t *testing.T) {
	tm, admin := newFakeManager(1)
	if err := tm.EnsureAgentControlTopics(context.Background(), 3); err != nil {
		t.Fatalf("EnsureAgentControlTopics: %v", err)
	}
	for _, topic := range []string{"pipeline.domain.events", "pipeline.agent.telemetry"} {
		created, ok := admin.created[kafkaclient.Topic(topic)]
		if !ok {
			t.Fatalf("%s was not created at all", topic)
		}
		if created.NumPartitions < 3 {
			t.Errorf("%s created with %d partition(s); the shell provisioners create it "+
				"with 3 and KeepExistingPartitions means this width is permanent",
				topic, created.NumPartitions)
		}
	}
}

// retentionFromInitScript parses the retention argument the shell provisioner
// passes for each topic it creates, so the contract is anchored to that file.
func retentionFromInitScript(t *testing.T) map[string]string {
	t.Helper()
	path := filepath.Join(serviceRoot(t), "..", "scripts", "kafka-init-new-topics.sh")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the shell provisioner at %s: %v", path, err)
	}

	// SEVEN_DAYS_MS=$((7 * 24 * 60 * 60 * 1000)) -- resolve it rather than assume.
	sevenDays := strconv.Itoa(7 * 24 * 60 * 60 * 1000)

	out := map[string]string{}
	re := regexp.MustCompile(`create_topic\s+"([^"]+)"\s+"?\$?\{?([^"\s}]+)\}?"?`)
	for _, m := range re.FindAllStringSubmatch(string(src), -1) {
		topic, retention := m[1], m[2]
		if retention == "SEVEN_DAYS_MS" {
			retention = sevenDays
		}
		if _, err := strconv.Atoi(retention); err != nil {
			continue // a retention we cannot resolve statically; not a contract we can pin
		}
		out[topic] = retention
	}

	// Anti-vacuity: if the parse finds nothing, every assertion above is skipped
	// and the test passes while proving nothing.
	for _, must := range []string{"pipeline.domain.events", "pipeline.agent.telemetry"} {
		if _, ok := out[must]; !ok {
			t.Fatalf("could not read %s's retention out of %s -- the parse is broken, so a "+
				"green result here would mean nothing", must, path)
		}
	}
	return out
}
