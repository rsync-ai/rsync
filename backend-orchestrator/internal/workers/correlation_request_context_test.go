package workers

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"
)

func TestCorrelationWorkBudgetMatchesTemporalWaits(t *testing.T) {
	path := filepath.Join("..", "..", "..", "backend-temporal-adapter", "internal", "workflows", "nl_pipeline_v2_activities.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("temporal adapter source not in this checkout: %v", err)
	}
	re := regexp.MustCompile(`waitForResponseWithHeartbeats\(ctx, "([a-z_]+)", correlationID, (\d+)\*time\.(Second|Minute)\)`)
	unit := map[string]time.Duration{"Second": time.Second, "Minute": time.Minute}
	seen := 0
	for _, m := range re.FindAllStringSubmatch(string(src), -1) {
		agent := m[1]
		if agent == "executor" { // own long-running budget (EXECUTOR_REQUEST_TIMEOUT)
			continue
		}
		n, _ := strconv.Atoi(m[2])
		want := time.Duration(n) * unit[m[3]]
		got, ok := correlationWorkBudget[agent]
		if !ok {
			t.Errorf("agent %q waits %s in temporal but has no work budget here (falls back to %s)", agent, want, correlationDefaultWait)
			continue
		}
		if got != want {
			t.Errorf("agent %q: work budget %s, temporal waits %s", agent, got, want)
		}
		seen++
	}
	if seen < len(correlationWorkBudget) {
		t.Fatalf("matched %d temporal waits for %d budgets — the regex no longer finds them", seen, len(correlationWorkBudget))
	}
}

func TestCorrelationWorkContextLeavesDeliverySlack(t *testing.T) {
	ctx, cancel := correlationWorkContext(context.Background(), "planner")
	defer cancel()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("no deadline")
	}
	if left := time.Until(dl); left > 3*time.Minute-correlationDeliverySlack || left < 3*time.Minute-correlationDeliverySlack-time.Second {
		t.Fatalf("planner work budget %s", left)
	}
	ctx2, cancel2 := correlationWorkContext(context.Background(), "unknown_agent")
	defer cancel2()
	dl2, _ := ctx2.Deadline()
	if left := time.Until(dl2); left > correlationDefaultWait {
		t.Fatalf("unknown agent budget %s", left)
	}
}

func TestCorrelationDeliveryOutlivesAnExpiredWorkContext(t *testing.T) {
	work, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-work.Done()
	deliver, cancelDeliver := correlationDeliveryContext()
	defer cancelDeliver()
	if deliver.Err() != nil {
		t.Fatalf("delivery context dead with the work context: %v", deliver.Err())
	}
}
