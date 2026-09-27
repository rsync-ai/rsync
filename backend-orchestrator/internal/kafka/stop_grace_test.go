package kafka

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The orchestrator's stop grace period is the time between SIGTERM and SIGKILL.
// A SIGKILL skips every deferred Stop and Close in main.go, including the Kafka
// close this package bounds with consumerCloseTimeout. The grace period is set
// in three files that do not read one another: docker-compose.yml, the
// standalone docker-compose.quickstart.yml, and the Helm chart. Left unset, the
// daemon's default applies: 10 s on Docker Engine, 1 s on the Docker Desktop this
// was measured on, and 30 s on Kubernetes.
//
// This test pins all three to one value, and that value to the shutdown it has
// to cover.
const orchestratorStopGrace = 45 * time.Second

// agentStopsBudget covers the agents' Stop calls, which main's defers run
// between the HTTP drain and the Kafka close. The sentinel's took 5-6 s on the
// dev stack.
const agentStopsBudget = 10 * time.Second

func TestOrchestratorStopGraceCoversItsShutdown(t *testing.T) {
	need := orchestratorHTTPDrain(t) + agentStopsBudget + consumerCloseTimeout
	if orchestratorStopGrace < need {
		t.Fatalf("stop grace %s is under the shutdown it has to cover (%s): HTTP drain + %s for "+
			"the agents' Stop calls + consumerCloseTimeout %s", orchestratorStopGrace, need,
			agentStopsBudget, consumerCloseTimeout)
	}

	repo := filepath.Join(serviceRoot(t), "..")
	for _, file := range []string{"docker-compose.yml", "docker-compose.quickstart.yml"} {
		raw := serviceKey(t, filepath.Join(repo, file), "  orchestrator:", "    stop_grace_period:")
		got, err := time.ParseDuration(raw)
		if err != nil {
			t.Errorf("%s: orchestrator stop_grace_period %q: %v", file, raw, err)
			continue
		}
		if got != orchestratorStopGrace {
			t.Errorf("%s: orchestrator stop_grace_period is %s, want %s", file, got, orchestratorStopGrace)
		}
	}

	values := filepath.Join("deploy", "helm", "rsync-ai", "values.yaml")
	raw := serviceKey(t, filepath.Join(repo, values), "orchestrator:", "  terminationGracePeriodSeconds:")
	secs, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s: orchestrator.terminationGracePeriodSeconds %q: %v", values, raw, err)
	}
	if got := time.Duration(secs) * time.Second; got != orchestratorStopGrace {
		t.Errorf("%s: orchestrator.terminationGracePeriodSeconds is %s, want %s", values, got, orchestratorStopGrace)
	}

	// The value does nothing unless the pod spec reads it.
	tmpl := filepath.Join("deploy", "helm", "rsync-ai", "templates", "apps", "orchestrator.yaml")
	src, err := os.ReadFile(filepath.Join(repo, tmpl))
	if err != nil {
		t.Fatalf("reading %s: %v", tmpl, err)
	}
	const ref = "terminationGracePeriodSeconds: {{ .Values.orchestrator.terminationGracePeriodSeconds }}"
	if !strings.Contains(string(src), ref) {
		t.Errorf("%s does not set %q, so the pod gets Kubernetes' 30 s default", tmpl, ref)
	}
}

// orchestratorHTTPDrain reads the timeout main.go gives srv.Shutdown, the first
// step after SIGTERM.
func orchestratorHTTPDrain(t *testing.T) time.Duration {
	t.Helper()
	path := filepath.Join(serviceRoot(t), "cmd", "orchestrator", "main.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	re := regexp.MustCompile(`shutdownCtx, \w+ := context\.WithTimeout\(context\.Background\(\), (\d+) ?\* ?time\.Second\)`)
	m := re.FindSubmatch(src)
	if m == nil {
		t.Fatalf("%s: found no shutdownCtx timeout; update this test to read the HTTP drain from where it moved", path)
	}
	secs, _ := strconv.Atoi(string(m[1]))
	if secs == 0 {
		t.Fatalf("%s: HTTP drain parsed as 0 s", path)
	}
	return time.Duration(secs) * time.Second
}

// serviceKey returns the value of the one line starting with keyPrefix inside the
// block that opens with the line blockHeader. The block ends at the next line
// indented no deeper than blockHeader that is not blank or a comment. A key that
// is missing or set twice fails the test.
func serviceKey(t *testing.T, path, blockHeader, keyPrefix string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	defer f.Close()

	indent := len(blockHeader) - len(strings.TrimLeft(blockHeader, " "))
	inBlock, found := false, false
	var values []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if !inBlock {
			if line == blockHeader {
				inBlock, found = true, true
			}
			continue
		}
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if len(line)-len(strings.TrimLeft(line, " ")) <= indent {
			break
		}
		if rest, ok := strings.CutPrefix(line, keyPrefix); ok {
			values = append(values, strings.TrimSpace(rest))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning %s: %v", path, err)
	}
	if !found {
		t.Fatalf("%s: no %q block", path, strings.TrimSpace(blockHeader))
	}
	if len(values) != 1 {
		t.Fatalf("%s: %q block sets %q %d times, want once", path, strings.TrimSpace(blockHeader),
			strings.TrimSpace(keyPrefix), len(values))
	}
	return values[0]
}
