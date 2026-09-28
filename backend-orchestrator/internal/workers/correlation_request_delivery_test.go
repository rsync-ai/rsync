package workers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rsync-ai/shared/correlation"
	log "github.com/sirupsen/logrus"
)

// fakeRedis is the smallest RESP2 server the correlation client needs:
// PING (NewClient), LPUSH + EXPIRE (WriteResponse), DEL (DeleteRequest).
// It records every command so a test can see what actually reached "Redis".
type fakeRedis struct {
	ln   net.Listener
	mu   sync.Mutex
	cmds [][]string
}

func startFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeRedis{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return f
}

func (f *fakeRedis) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		cmd, err := readRESPArray(r)
		if err != nil {
			return
		}
		f.mu.Lock()
		f.cmds = append(f.cmds, cmd)
		f.mu.Unlock()
		var reply string
		switch strings.ToUpper(cmd[0]) {
		case "PING":
			reply = "+PONG\r\n"
		case "LPUSH", "EXPIRE", "DEL":
			reply = ":1\r\n"
		default:
			reply = "+OK\r\n"
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("unexpected RESP line %q", line)
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(hdr[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:size]))
	}
	return out, nil
}

func (f *fakeRedis) find(verb, key string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.cmds {
		if strings.EqualFold(c[0], verb) && len(c) > 1 && c[1] == key {
			return c
		}
	}
	return nil
}

// A work call that outlives its budget must still have its failure routed to
// the waiting Temporal activity and its request deleted. This is P-1: the 41 s
// planner whose "context deadline exceeded" never reached PlannerActivityV2, so
// the activity waited out its full 3 min. Both earlier shapes fail this test:
// routing on the work context (pre-#1238) and a delivery context whose clock
// started before the work (#1238).
func TestRunCorrelationRequestRoutesTheFailureOfAWorkCallThatTimedOut(t *testing.T) {
	fake := startFakeRedis(t)
	client, err := correlation.NewClient(fake.ln.Addr().String(), "")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer client.Close()

	prevClient, prevDelivery := correlationClient, correlationDeliveryTimeout
	correlationClient = client
	// Work runs 400 ms (budget - slack); delivery gets 150 ms. The work alone
	// outlasts the whole delivery budget, like the 41 s planner vs 15 s.
	const agent = "p1_test_agent"
	correlationWorkBudget[agent] = correlationDeliverySlack + 400*time.Millisecond
	correlationDeliveryTimeout = 150 * time.Millisecond
	t.Cleanup(func() {
		correlationClient, correlationDeliveryTimeout = prevClient, prevDelivery
		delete(correlationWorkBudget, agent)
	})

	task := Task{CorrelationID: "corr-p1-0001", TaskID: "t-p1", StepID: agent}
	workErr := make(chan error, 1)
	start := time.Now()
	runCorrelationRequest(context.Background(), agent, client, task, log.NewEntry(log.StandardLogger()),
		func(ctx context.Context) TaskResult {
			<-ctx.Done() // an LLM call that never answers in time
			workErr <- ctx.Err()
			return TaskResult{TaskID: task.TaskID, Status: "failed", Error: "failed to generate plan: " + ctx.Err().Error()}
		})
	elapsed := time.Since(start)

	if got := <-workErr; got != context.DeadlineExceeded {
		t.Fatalf("control: work call should have hit its own deadline, got %v", got)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("delivery did not happen promptly: %s", elapsed)
	}

	push := fake.find("LPUSH", "agent:response:"+task.CorrelationID)
	if push == nil {
		t.Fatalf("the timed-out work's failure was never routed; commands seen: %v", fake.cmds)
	}
	var resp correlation.WorkerResponse
	if err := json.Unmarshal([]byte(push[2]), &resp); err != nil {
		t.Fatalf("routed response is not JSON: %v", err)
	}
	// PlannerActivityV2 fails fast (PLANNING_FAILED) on any status other than
	// success/completed — that is what turns a 3 min stall into an immediate error.
	if resp.Status != "failed" || !strings.Contains(resp.Error, "context deadline exceeded") {
		t.Fatalf("routed response %+v, want status=failed carrying the deadline error", resp)
	}
	if fake.find("DEL", "agent:request:"+task.CorrelationID) == nil {
		t.Fatalf("request was not deleted; commands seen: %v", fake.cmds)
	}
}

// Every correlation worker must answer through runCorrelationRequest, so the
// delivery context is always created after the work — never alongside it.
func TestRedisPollingWorkersDeliverThroughRunCorrelationRequest(t *testing.T) {
	files := map[string]string{
		"planner_redis_polling.go":              `"planner"`,
		"intent_redis_polling.go":               `"intent"`,
		"validator_redis_polling.go":            `"validator"`,
		"cost_estimator_redis_polling.go":       `"cost_estimator"`,
		"connection_validator_redis_polling.go": `"connection_validator"`,
		"capability_resolver_redis_polling.go":  `"connector_resolver"`,
	}
	for file, agent := range files {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		s := string(src)
		if !strings.Contains(s, "runCorrelationRequest(w.ctx, "+agent+",") {
			t.Errorf("%s does not answer through runCorrelationRequest(w.ctx, %s, ...)", file, agent)
		}
		for _, banned := range []string{"correlationDeliveryContext(", "correlationWorkContext(", "RouteResult(", ".DeleteRequest("} {
			if strings.Contains(s, banned) {
				t.Errorf("%s calls %s directly — its delivery context would not be fresh", file, banned)
			}
		}
	}
}
