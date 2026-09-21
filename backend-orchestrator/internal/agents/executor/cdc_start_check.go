package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
)

// Why this exists (#19): start_sync reports success as soon as Kafka Connect
// ACCEPTS the connector config (HTTP 201, or a no-op on an identical existing
// config). Accepting a config says nothing about whether the Debezium task
// started. A task that FAILED on start used to leave the pipeline showing
// "Running" while it wrote nothing, forever. This check reads the connector's own
// status before the run is reported as started and fails the run with the task's
// error instead.
//
// What it does NOT catch: errors Debezium retries keep the task RUNNING, so they
// look healthy here. Probed on Debezium 3.1 against MongoDB, that includes a
// standalone mongod (no change streams) and a user without change-stream
// privileges — both retried without limit. The privilege case is prevented at the
// config instead (the debezium MCP connector scopes the stream to the captured
// database). An unreachable host and a wrong password never get this far: Connect
// rejects the config at create (HTTP 400), so start_sync itself fails. The runtime
// dependency probe keeps watching the connector after start.

// Package vars so tests can shrink the timings.
var (
	// cdcStartCheckWindow bounds the whole check. When it elapses without a verdict
	// (Connect unreachable, tasks still UNASSIGNED) the run proceeds as before.
	cdcStartCheckWindow = 30 * time.Second
	// cdcStartCheckSettle is how long the connector and every task must stay RUNNING
	// before the check passes: Connect marks a task RUNNING before its start() has
	// connected to the source, so the first RUNNING is not yet proof.
	cdcStartCheckSettle = 10 * time.Second
	cdcStartCheckPoll   = 2 * time.Second
)

// cdcStartTraceMaxRunes caps the task error carried into the run's error text.
const cdcStartTraceMaxRunes = 400

type connectStatusDoc struct {
	Connector struct {
		State string `json:"state"`
		Trace string `json:"trace"`
	} `json:"connector"`
	Tasks []struct {
		ID    int    `json:"id"`
		State string `json:"state"`
		Trace string `json:"trace"`
	} `json:"tasks"`
}

// cdcStartState is one reading of the connector status.
type cdcStartState int

const (
	cdcStartUnknown cdcStartState = iota // unreadable, no tasks yet, UNASSIGNED, PAUSED, …
	cdcStartRunning                      // connector and every task RUNNING
	cdcStartFailed                       // connector or any task FAILED
)

// classifyConnectStatus reads a status document. For FAILED it also returns the
// scrubbed first line of the failing trace (the exception and its message; the Java
// stack frames below it are noise to a user).
func classifyConnectStatus(doc connectStatusDoc) (cdcStartState, string) {
	if strings.EqualFold(strings.TrimSpace(doc.Connector.State), "FAILED") {
		return cdcStartFailed, "connector FAILED: " + traceHeadline(doc.Connector.Trace)
	}
	for _, t := range doc.Tasks {
		if strings.EqualFold(strings.TrimSpace(t.State), "FAILED") {
			return cdcStartFailed, fmt.Sprintf("task %d FAILED: %s", t.ID, traceHeadline(t.Trace))
		}
	}
	if !strings.EqualFold(strings.TrimSpace(doc.Connector.State), "RUNNING") || len(doc.Tasks) == 0 {
		return cdcStartUnknown, ""
	}
	for _, t := range doc.Tasks {
		if !strings.EqualFold(strings.TrimSpace(t.State), "RUNNING") {
			return cdcStartUnknown, ""
		}
	}
	return cdcStartRunning, ""
}

// traceHeadline keeps the first non-empty line of a Connect trace, prefers the
// deepest "Caused by:" line when there is one (that is where the source's own error
// is), and scrubs it — the trace can quote connection strings and hosts.
func traceHeadline(trace string) string {
	head := ""
	for _, ln := range strings.Split(trace, "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		if head == "" {
			head = ln
			continue
		}
		if strings.HasPrefix(ln, "Caused by:") {
			head = strings.TrimSpace(strings.TrimPrefix(ln, "Caused by:"))
		}
	}
	if head == "" {
		return "no error detail reported by Kafka Connect"
	}
	return llmscrub.ScrubMax(head, cdcStartTraceMaxRunes)
}

func kafkaConnectStatusURL(connectorName string) string {
	base := strings.TrimRight(os.Getenv("KAFKA_CONNECT_URL"), "/")
	if base == "" {
		base = "http://kafka-connect:8083"
	}
	return fmt.Sprintf("%s/connectors/%s/status", base, connectorName)
}

func readConnectStatus(ctx context.Context, statusURL string) (connectStatusDoc, bool) {
	var doc connectStatusDoc
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, statusURL, nil)
	if err != nil {
		return doc, false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return doc, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return doc, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || json.Unmarshal(body, &doc) != nil {
		return doc, false
	}
	return doc, true
}

// restartFailedConnectorTasks asks Connect to restart only the FAILED instances.
// Used once, when start_sync was a no-op on an identical config: a user who fixed
// the cause outside rsync (credentials, privileges) and pressed Run again would
// otherwise meet the same FAILED task, because an unchanged config never restarts it.
func restartFailedConnectorTasks(ctx context.Context, statusURL string) error {
	restartURL := strings.TrimSuffix(statusURL, "/status") + "/restart?includeTasks=true&onlyFailed=true"
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, restartURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("restart returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// verifyCDCConnectorStarted polls the connector status after start_sync. It returns
// a non-empty, scrubbed failure message when the connector or a task is FAILED, and
// "" when the connector held RUNNING for the settle period or no verdict was
// reached inside the window (the pre-check behaviour: proceed).
func verifyCDCConnectorStarted(ctx context.Context, statusURL string, alreadyRunning bool) string {
	start := time.Now()
	deadline := start.Add(cdcStartCheckWindow)
	var runningSince time.Time
	restarted := false
	for {
		if doc, ok := readConnectStatus(ctx, statusURL); ok {
			state, reason := classifyConnectStatus(doc)
			switch state {
			case cdcStartFailed:
				if alreadyRunning && !restarted {
					restarted = true
					runningSince = time.Time{}
					if err := restartFailedConnectorTasks(ctx, statusURL); err == nil {
						log.WithField("status_url", statusURL).
							Info("CDC start check: existing connector had FAILED tasks; restarted them once")
						break
					}
				}
				return reason
			case cdcStartRunning:
				if runningSince.IsZero() {
					runningSince = time.Now()
				}
				if time.Since(runningSince) >= cdcStartCheckSettle {
					return ""
				}
			default:
				runningSince = time.Time{}
			}
		} else {
			runningSince = time.Time{}
		}
		if !time.Now().Before(deadline) {
			log.WithFields(log.Fields{
				"status_url": statusURL,
				"waited":     time.Since(start).Round(time.Second).String(),
			}).Warn("CDC start check: no verdict from Kafka Connect inside the window; continuing (the dependency probe keeps watching the connector)")
			return ""
		}
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(cdcStartCheckPoll):
		}
	}
}

// startResultBool reads a bool from an MCP start_sync result, top level or nested
// under "result" (the same two shapes connector_name is read from).
func startResultBool(result map[string]interface{}, key string) bool {
	if result == nil {
		return false
	}
	if v, ok := result[key].(bool); ok {
		return v
	}
	if nested, ok := result["result"].(map[string]interface{}); ok {
		if v, ok := nested[key].(bool); ok {
			return v
		}
	}
	return false
}
