package websocket

import (
	"encoding/json"
	"testing"
	"time"
)

// newTestClient builds a Client that bypasses the real websocket.Conn so the
// hub's broadcast path can be exercised in isolation.
func newTestClient(h *Hub, userID string) *Client {
	return &Client{
		hub:    h,
		send:   make(chan []byte, 8),
		userID: userID,
	}
}

func startTestHub(t *testing.T) (*Hub, func()) {
	t.Helper()
	h := NewHub()
	go h.Run()
	// Give Run() a chance to enter its select loop before tests push messages.
	time.Sleep(20 * time.Millisecond)
	return h, h.Stop
}

func registerAndWait(t *testing.T, h *Hub, c *Client) {
	t.Helper()
	h.register <- c
	// Wait long enough for Run() to process the register before the next
	// broadcast tick; the hub's batch flush ticker is 100ms.
	time.Sleep(30 * time.Millisecond)
}

// drain reads up to maxWait for one payload on the client's send channel.
// Returns ("", false) on timeout.
func drain(c *Client, maxWait time.Duration) (string, bool) {
	select {
	case msg := <-c.send:
		return string(msg), true
	case <-time.After(maxWait):
		return "", false
	}
}

func TestBroadcastDomainEvent_OnlyDeliversToOwner(t *testing.T) {
	h, stop := startTestHub(t)
	defer stop()

	owner := newTestClient(h, "user-A")
	other := newTestClient(h, "user-B")
	registerAndWait(t, h, owner)
	registerAndWait(t, h, other)

	const pipelineID = "11111111-1111-1111-1111-111111111111"
	h.cachePipelineOwner(pipelineID, "user-A")

	h.BroadcastDomainEvent(pipelineID, map[string]interface{}{
		"event_type":  "STAGE_COMPLETED",
		"pipeline_id": pipelineID,
		"secret":      "do-not-leak",
	}, "trace-1")

	// Wait at least one batch flush tick.
	if got, ok := drain(owner, 250*time.Millisecond); !ok {
		t.Fatalf("owner did not receive scoped domain event")
	} else {
		var ev map[string]interface{}
		if err := json.Unmarshal([]byte(got), &ev); err != nil {
			t.Fatalf("owner received non-JSON payload: %v", err)
		}
		if ev["type"] != "domain_event" {
			t.Fatalf("owner received wrong event type: %v", ev["type"])
		}
	}
	if got, ok := drain(other, 200*time.Millisecond); ok {
		t.Fatalf("non-owner received scoped domain event, payload=%s", got)
	}
}

func TestBroadcastDomainEvent_DropsWhenOwnerUnresolved(t *testing.T) {
	h, stop := startTestHub(t)
	defer stop()

	a := newTestClient(h, "user-A")
	b := newTestClient(h, "user-B")
	registerAndWait(t, h, a)
	registerAndWait(t, h, b)

	// No cachePipelineOwner call; with no DB available this will fail-closed.
	h.BroadcastDomainEvent("22222222-2222-2222-2222-222222222222", map[string]interface{}{
		"event_type": "STAGE_COMPLETED",
	}, "trace-2")

	if got, ok := drain(a, 200*time.Millisecond); ok {
		t.Fatalf("client A received an event with unresolved owner: %s", got)
	}
	if got, ok := drain(b, 50*time.Millisecond); ok {
		t.Fatalf("client B received an event with unresolved owner: %s", got)
	}
}

func TestBroadcastEvent_ScopesWhenDataCarriesPipelineID(t *testing.T) {
	h, stop := startTestHub(t)
	defer stop()

	owner := newTestClient(h, "user-A")
	other := newTestClient(h, "user-B")
	registerAndWait(t, h, owner)
	registerAndWait(t, h, other)

	const pipelineID = "33333333-3333-3333-3333-333333333333"
	h.cachePipelineOwner(pipelineID, "user-A")

	// BroadcastAgentActivity puts pipeline_id into the data map, so the
	// generic BroadcastEvent path must scope it by owner via extractPipelineID.
	h.BroadcastAgentActivity("planner", "processing", pipelineID, map[string]interface{}{
		"message": "thinking",
	}, "trace-3")

	if _, ok := drain(owner, 250*time.Millisecond); !ok {
		t.Fatalf("owner did not receive agent_activity event")
	}
	if got, ok := drain(other, 150*time.Millisecond); ok {
		t.Fatalf("non-owner received agent_activity event: %s", got)
	}
}

func TestBroadcastEvent_GlobalFallbackWhenNoPipelineID(t *testing.T) {
	h, stop := startTestHub(t)
	defer stop()

	a := newTestClient(h, "user-A")
	b := newTestClient(h, "user-B")
	registerAndWait(t, h, a)
	registerAndWait(t, h, b)

	// SystemHealth-style event with no pipeline_id should reach everyone.
	h.BroadcastEvent(EventSystemHealth, map[string]interface{}{
		"status": "ok",
	}, "trace-4")

	if _, ok := drain(a, 250*time.Millisecond); !ok {
		t.Fatalf("client A did not receive global event")
	}
	if _, ok := drain(b, 250*time.Millisecond); !ok {
		t.Fatalf("client B did not receive global event")
	}
}

