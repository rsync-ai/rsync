package workflows

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// The table picker names the database it lists ("Tables in orders_db"). The
// executor puts that name in its pause result as source_database; these tests
// prove the adapter carries it, unchanged, into the PIPELINE_WAITING details the
// state writer stores and the picker reads.

type sourceDatabaseHarness struct {
	mu sync.Mutex

	// pauseOutput is what the executor's first call returns with its
	// waiting_for_table_selection status.
	pauseOutput map[string]interface{}

	executorCalls  int
	waitingDetails []map[string]interface{}
}

func (h *sourceDatabaseHarness) executor(_ context.Context, _ map[string]interface{}) (ExecutorNativeResult, error) {
	h.mu.Lock()
	h.executorCalls++
	n := h.executorCalls
	h.mu.Unlock()
	if n == 1 {
		return ExecutorNativeResult{
			Status: "waiting_for_table_selection",
			Error:  "select tables to continue",
			Output: h.pauseOutput,
		}, nil
	}
	return ExecutorNativeResult{Status: "success", Output: map[string]interface{}{"rows": 10}}, nil
}

func (h *sourceDatabaseHarness) stateUpdate(_ context.Context, in StateUpdateInput) error {
	if in.EventType != "PIPELINE_WAITING" {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.waitingDetails = append(h.waitingDetails, in.Metadata)
	return nil
}

func (h *sourceDatabaseHarness) pipelineStatus(_ context.Context, _ string, _ string, _ string, _ string) error {
	return nil
}

func (h *sourceDatabaseHarness) domainEvent(_ context.Context, _ map[string]interface{}) error {
	return nil
}

// runToTableSelection starts the real workflow, lets the executor pause for
// table selection, answers with a selection an hour later, and returns the
// details of every PIPELINE_WAITING state write.
func runToTableSelection(t *testing.T, pauseOutput map[string]interface{}) []map[string]interface{} {
	t.Helper()
	h := &sourceDatabaseHarness{pauseOutput: pauseOutput}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	env.RegisterWorkflowWithOptions(NLPipelineWorkflowV2, workflow.RegisterOptions{Name: NLPipelineWorkflowV2Name})
	env.RegisterActivityWithOptions(h.executor, activity.RegisterOptions{Name: "ExecutorNativeActivity"})
	env.RegisterActivityWithOptions(h.stateUpdate, activity.RegisterOptions{Name: "StateUpdateActivity"})
	env.RegisterActivityWithOptions(h.pipelineStatus, activity.RegisterOptions{Name: "UpdatePipelineStatusActivity"})
	env.RegisterActivityWithOptions(h.domainEvent, activity.RegisterOptions{Name: "EmitDomainEventActivity"})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalTablesSelected, TablesSelectedPayload{
			PipelineID:     "pipe-srcdb-1",
			SelectedTables: []string{"orders"},
		})
	}, time.Hour)
	env.ExecuteWorkflow(NLPipelineWorkflowV2, NLPipelineWorkflowV2Input{
		PipelineID:              "pipe-srcdb-1",
		ExecutionID:             "exec-srcdb-1",
		UserID:                  "user-1",
		Message:                 "move my orders collection",
		SourceConnectionID:      "conn-mongo-orders",
		DestinationConnectionID: "conn-gcs-lake",
		SelectedTables:          []string{"orders"},
		ExecutorDispatch:        ExecutorDispatchTemporal,
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned an error: %v", err)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.executorCalls != 2 {
		t.Fatalf("expected the executor to pause once and run again after the selection (2 calls), got %d", h.executorCalls)
	}
	if len(h.waitingDetails) != 1 {
		t.Fatalf("expected exactly one PIPELINE_WAITING state write, got %d — the test proves nothing without it", len(h.waitingDetails))
	}
	return h.waitingDetails
}

func TestNLPipelineV2_TableSelectionWaitingDetailsNameTheSourceDatabase(t *testing.T) {
	details := runToTableSelection(t, map[string]interface{}{
		"available_tables": []interface{}{
			map[string]interface{}{"name": "orders", "schema": "orders_db"},
			map[string]interface{}{"name": "customers", "schema": "orders_db"},
		},
		"source_type":     "mongodb",
		"source_database": "orders_db",
	})[0]

	if got := details["source_database"]; got != "orders_db" {
		t.Fatalf("PIPELINE_WAITING details must carry the executor's source_database %q, got %#v (details %v)", "orders_db", got, details)
	}
	// Control: the details the picker needs next to the database name are there too.
	if details["request_type"] != "table_selection" || details["source_type"] != "mongodb" {
		t.Fatalf("waiting details lost the table-selection shape: %v", details)
	}
	if details["source_connection_id"] != "conn-mongo-orders" || details["destination_connection_id"] != "conn-gcs-lake" {
		t.Fatalf("waiting details must name both connections, got %v", details)
	}
	if tables, ok := details["available_tables"].([]interface{}); !ok || len(tables) != 2 {
		t.Fatalf("waiting details must list the 2 discovered tables, got %#v", details["available_tables"])
	}
}

// A source with no database (an API connector) reports an empty name. The key
// must still be written: the state writer merges details into the stored row,
// so leaving it out would keep showing the database from an earlier pause.
func TestNLPipelineV2_TableSelectionWaitingDetailsKeepAnEmptySourceDatabase(t *testing.T) {
	details := runToTableSelection(t, map[string]interface{}{
		"available_tables": []interface{}{
			map[string]interface{}{"name": "contacts"},
		},
		"source_type":     "hubspot",
		"source_database": "",
	})[0]

	got, present := details["source_database"]
	if !present || got != "" {
		t.Fatalf("PIPELINE_WAITING details must write an empty source_database, got present=%v value=%#v", present, got)
	}
}

func TestClassifyExecutorResponse_TableSelectionKeepsTheSourceDatabase(t *testing.T) {
	output := map[string]interface{}{
		"available_tables": []interface{}{map[string]interface{}{"name": "orders", "schema": "orders_db"}},
		"source_type":      "mongodb",
		"source_database":  "orders_db",
	}
	res, aerr := classifyExecutorResponse("waiting_for_table_selection", "select tables", output, "corr-1", nil)
	if res != nil {
		t.Fatalf("a table-selection pause is not a success, got %v", res)
	}
	if aerr == nil || aerr.Code != PolicyCodeTableSelectionRequired {
		t.Fatalf("expected the table-selection policy error, got %v", aerr)
	}
	if got := aerr.Metadata["source_database"]; got != "orders_db" {
		t.Fatalf("the pause metadata must keep source_database %q, got %#v", "orders_db", got)
	}
	if got := aerr.Metadata["source_type"]; got != "mongodb" {
		t.Fatalf("the pause metadata must keep source_type, got %#v", got)
	}
}
