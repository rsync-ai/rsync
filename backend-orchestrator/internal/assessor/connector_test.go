package assessor

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// fakeExecutor records every MCP call and replies via a per-test responder.
type fakeExecutor struct {
	calls   []mcp.ExecuteRequest
	respond func(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error)
}

func (f *fakeExecutor) Execute(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	f.calls = append(f.calls, req)
	return f.respond(req)
}

func (f *fakeExecutor) exportCalls() []mcp.ExecuteRequest {
	out := []mcp.ExecuteRequest{}
	for _, c := range f.calls {
		if c.Operation == "export" {
			out = append(out, c)
		}
	}
	return out
}

// KI-1-extend: ConnectorAssessor must probe per-selected-table read access with
// export(limit=1) after test_connection, blocking ONLY on a permission/scope wall.
func TestConnectorAssessor_PerTableReadProbe(t *testing.T) {
	base := Input{SourceType: "shopify", ConnectionConfig: map[string]string{"connector_type": "shopify"}}
	okConn := func(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
		return &mcp.ExecuteResponse{Success: true}, nil
	}

	t.Run("all tables readable -> no block, one export probe per table with limit=1", func(t *testing.T) {
		fe := &fakeExecutor{respond: okConn}
		in := base
		in.Tables = []string{"shopify.products", "shopify.orders"}
		r, err := NewConnectorAssessor(fe).Assess(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		if r.BlocksStart() {
			t.Fatalf("healthy readable tables must not block: %+v", r.Checks)
		}
		ex := fe.exportCalls()
		if len(ex) != 2 {
			t.Fatalf("expected 2 export probes, got %d", len(ex))
		}
		for _, c := range ex {
			if v, ok := c.Params["limit"].(int); !ok || v != 1 {
				t.Fatalf("export probe must use limit=1, got %v", c.Params["limit"])
			}
		}
	})

	t.Run("one table forbidden -> blocks with CONNECTOR_TABLE_READ_FORBIDDEN naming the table", func(t *testing.T) {
		fe := &fakeExecutor{respond: func(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
			if req.Operation == "export" && req.Params["table"] == "shopify.customers" {
				return &mcp.ExecuteResponse{Success: false, Error: "403 this app is not approved to access the Customer object"}, nil
			}
			return &mcp.ExecuteResponse{Success: true}, nil
		}}
		in := base
		in.Tables = []string{"shopify.products", "shopify.customers"}
		r, _ := NewConnectorAssessor(fe).Assess(context.Background(), in)
		if !r.BlocksStart() {
			t.Fatalf("a forbidden table must block start: %+v", r.Checks)
		}
		found := false
		for _, c := range r.Checks {
			if c.Code == "CONNECTOR_TABLE_READ_FORBIDDEN" {
				found = true
				if c.Severity != SeverityError {
					t.Fatalf("forbidden must be error severity, got %v", c.Severity)
				}
				if !strings.Contains(c.Message, "customers") {
					t.Fatalf("message must name the denied table: %s", c.Message)
				}
			}
		}
		if !found {
			t.Fatalf("expected CONNECTOR_TABLE_READ_FORBIDDEN, got %+v", r.Checks)
		}
	})

	t.Run("export transport error -> advisory, no block", func(t *testing.T) {
		fe := &fakeExecutor{respond: func(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
			if req.Operation == "export" {
				return nil, fmt.Errorf("dial tcp: connection refused")
			}
			return &mcp.ExecuteResponse{Success: true}, nil
		}}
		in := base
		in.Tables = []string{"shopify.products"}
		r, _ := NewConnectorAssessor(fe).Assess(context.Background(), in)
		if r.BlocksStart() {
			t.Fatalf("a transport error probing must be advisory, not blocking: %+v", r.Checks)
		}
	})

	t.Run("non-permission read failure -> advisory, no block (no false block on connector quirks)", func(t *testing.T) {
		fe := &fakeExecutor{respond: func(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
			if req.Operation == "export" {
				return &mcp.ExecuteResponse{Success: false, Error: "table not found"}, nil
			}
			return &mcp.ExecuteResponse{Success: true}, nil
		}}
		in := base
		in.Tables = []string{"shopify.unknown"}
		r, _ := NewConnectorAssessor(fe).Assess(context.Background(), in)
		if r.BlocksStart() {
			t.Fatalf("non-permission read failure must be advisory: %+v", r.Checks)
		}
	})

	t.Run("no tables selected -> only test_connection, zero export probes", func(t *testing.T) {
		fe := &fakeExecutor{respond: okConn}
		_, _ = NewConnectorAssessor(fe).Assess(context.Background(), base)
		if len(fe.exportCalls()) != 0 {
			t.Fatalf("must not probe export when no tables selected")
		}
		if len(fe.calls) != 1 {
			t.Fatalf("expected exactly one test_connection call, got %d", len(fe.calls))
		}
	})
}
