package sentinel

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// consumeCallRe matches `…ConsumeWithContext("some.topic", …)` and captures the literal
// topic. Call sites that pass a variable or a constant are deliberately NOT matched — see
// findModuleRoot's doc comment for why that is safe here.
var consumeCallRe = regexp.MustCompile(`ConsumeWithContext\(\s*"([^"]+)"`)

// findModuleRoot walks up from the test's working directory to the directory holding go.mod.
func findModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not find go.mod above %q", dir)
		}
		dir = parent
	}
}

// scanOrchestratorSources returns the concatenated text of every non-test .go file in the
// orchestrator module, plus the set of literal topics passed to ConsumeWithContext.
func scanOrchestratorSources(t *testing.T) (allSource string, consumed map[string]bool) {
	t.Helper()
	root := findModuleRoot(t)
	consumed = make(map[string]bool)
	var b strings.Builder

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == "vendor" || info.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		b.Write(data)
		b.WriteString("\n")
		for _, m := range consumeCallRe.FindAllStringSubmatch(string(data), -1) {
			consumed[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %q: %v", root, err)
	}
	return b.String(), consumed
}

// TestOrchestratorConsumedTopics_EveryEntryIsActuallyConsumed is the structural guard for the
// consumer-liveness check in health_monitor.go.
//
// HealthMonitor.checkKafkaConsumerLag asks kafkaManager.IsConsumerActive(topic), which answers
// from THIS Manager's own `consumers` map (kafka/manager.go). Only ConsumeWithContext writes
// that map, and only in the process that calls it. So a topic this binary merely *produces* to
// has no entry, IsConsumerActive returns false forever, and the monitor pins a permanently
// Unhealthy component and logs "⚠️ Consumer group is closed or inactive" on every single tick.
//
// That is exactly what `pipeline.domain.events` did: the orchestrator produces to it while all
// of its consumers live in other processes (api-gateway's event projector / WebSocket bridge /
// domain-event handler, and the Kafka sink worker). This test fails if anyone adds a topic to
// orchestratorConsumedTopics without a matching ConsumeWithContext call in this module.
func TestOrchestratorConsumedTopics_EveryEntryIsActuallyConsumed(t *testing.T) {
	_, consumed := scanOrchestratorSources(t)

	if len(consumed) == 0 {
		t.Fatal("source scan found zero ConsumeWithContext(\"literal\") call sites — the scanner is broken, " +
			"not the code; fix consumeCallRe before trusting this test")
	}

	for _, topic := range orchestratorConsumedTopics {
		if !consumed[topic] {
			t.Errorf("orchestratorConsumedTopics contains %q but no ConsumeWithContext(%q, …) call exists "+
				"in this module.\nIsConsumerActive answers from the LOCAL Manager's consumers map, so this "+
				"topic will report inactive forever and pin an Unhealthy component every tick.\n"+
				"Produce ≠ consume — remove it, or add the consumer.", topic, topic)
		}
	}
}

// TestOrchestratorConsumedTopics_ExcludesProduceOnlyDomainEvents pins the specific regression.
//
// The two halves matter together: the topic must be absent from the liveness list AND the
// orchestrator must still produce to it. If the second assertion ever fails, the first has
// become vacuous — the topic left the codebase — and this test should be deleted rather than
// left standing as false assurance.
func TestOrchestratorConsumedTopics_ExcludesProduceOnlyDomainEvents(t *testing.T) {
	const topic = "pipeline.domain.events"

	for _, got := range orchestratorConsumedTopics {
		if got == topic {
			t.Fatalf("%q is back in orchestratorConsumedTopics. The orchestrator only PRODUCES to it; "+
				"its consumers live in api-gateway and the sink worker. Having it here makes "+
				"checkKafkaConsumerLag log \"Consumer group is closed or inactive\" and report Unhealthy "+
				"on every poll, forever.", topic)
		}
	}

	allSource, _ := scanOrchestratorSources(t)
	if !strings.Contains(allSource, `"`+topic+`"`) {
		t.Fatalf("%q no longer appears anywhere in the orchestrator sources — the premise of this "+
			"regression test is gone. Delete this test instead of letting it pass vacuously.", topic)
	}
}

// TestOrchestratorConsumedTopics_CoversEveryAgentCommandTopic asserts the flip side: the nine
// agent command topics the orchestrator's workers really do consume must all be monitored. A
// topic silently dropped from this list is a consumer that can wedge with nobody watching.
func TestOrchestratorConsumedTopics_CoversEveryAgentCommandTopic(t *testing.T) {
	_, consumed := scanOrchestratorSources(t)

	inList := make(map[string]bool, len(orchestratorConsumedTopics))
	for _, topic := range orchestratorConsumedTopics {
		inList[topic] = true
	}

	for topic := range consumed {
		if !strings.HasPrefix(topic, "agent.control.commands.") {
			continue
		}
		if !inList[topic] {
			t.Errorf("this module consumes %q but it is missing from orchestratorConsumedTopics, "+
				"so its consumer group is not liveness-monitored at all", topic)
		}
	}
}
