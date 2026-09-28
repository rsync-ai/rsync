package handlers

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"api-gateway/internal/chat"

	"github.com/gin-gonic/gin"
)

// A user names their saved CONNECTIONS, not connector types: the demo seeds a
// destination called "Demo warehouse", and "sync sample data to Demo warehouse"
// is how anyone would ask for it. The deterministic parsers know only catalog
// ids, so the name used to reach the LLM (or none), come back as some spelling
// of "demo warehouse", and end as connector_missing ("generate Demo warehouse").
// Found on the v0.1.6 GKE test, 2026-09-27.
//
// The bug class: the fast paths recognise a connector only by its catalog
// spelling, so ANY saved connection name is invisible to them. These cases name
// connections of several connector types, on either side, in the spellings a
// user types, with no LLM, so only the deterministic reading can pass them.

var namedConnectionsWorkspace = []fakeConnectionRow{
	{"Sample data (demo)", "", "sample-data", "source"},
	{"Demo warehouse", "", "postgresql", "destination"},
	{"Orders DB", "", "mysql", "source"},
	{"lake", "analytics-lake", "aws-s3", "destination"},
	// Two connections share a name across connector types: naming it cannot
	// pick one, so it must not be rewritten to either.
	{"Analytics", "", "bigquery", "destination"},
	{"analytics", "", "snowflake", "destination"},
	// A connection named with an everyday word must not rewrite that word
	// wherever the user says it.
	{"data", "", "mysql", "source"},
}

func sendNewChatMessageInWorkspace(t *testing.T, msg string) ChatMessageResponse {
	t.Helper()
	c := newIntentTestContext(t)
	c.Set(ctxWorkspaceID, "ws-1")
	return (&ChatHandler{}).handleNewIntent(context.Background(), c,
		chat.NewConversationContext("u1", "s1"), msg, "trace", "s1", "u1")
}

func TestChatResolvesSavedConnectionNamesWithoutTheLLM(t *testing.T) {
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`

	cases := []struct {
		msg, wantType, src, dst string
	}{
		{"sync sample data to Demo warehouse", "confirmation", "sample-data", "postgresql"},
		{"Sync Sample data (demo) to Demo warehouse", "confirmation", "sample-data", "postgresql"},
		{"copy Orders DB to Demo warehouse", "confirmation", "mysql", "postgresql"},
		{"copy orders db into demo-warehouse", "confirmation", "mysql", "postgresql"},
		{"sync orders_db to demo_warehouse", "confirmation", "mysql", "postgresql"},
		{"move Orders DB to analytics-lake", "confirmation", "mysql", "aws-s3"},
		{"load data into Demo warehouse", "slot_filling", "", "postgresql"},
		// A catalog id beside a name keeps working exactly as before.
		{"sync mysql to Demo warehouse", "confirmation", "mysql", "postgresql"},
		// The everyday-word connection is not a rewrite target.
		{"sync sample data to postgres", "confirmation", "sample-data", "postgresql"},
	}
	for _, tc := range cases {
		t.Run(tc.msg, func(t *testing.T) {
			catalog := withFakeCatalog(t, demoCatalog...)
			catalog.connections = namedConnectionsWorkspace
			catalog.otherErrs = true
			calls := completionStub(t, http.StatusServiceUnavailable, gated)

			resp := sendNewChatMessageInWorkspace(t, tc.msg)

			if n := atomic.LoadInt64(calls); n != 0 {
				t.Fatalf("%q called the LLM %d time(s); it names saved connections, so it needs none (type=%q message=%q)",
					tc.msg, n, resp.Type, resp.Message)
			}
			if resp.Type != tc.wantType {
				t.Fatalf("%q: type=%q, want %q (message=%q)", tc.msg, resp.Type, tc.wantType, resp.Message)
			}
			if got, _ := resp.Data["source_type"].(string); got != tc.src {
				t.Errorf("%q: source_type=%q, want %q", tc.msg, got, tc.src)
			}
			if got, _ := resp.Data["destination_type"].(string); got != tc.dst {
				t.Errorf("%q: destination_type=%q, want %q", tc.msg, got, tc.dst)
			}
			if !catalog.wasAsked("<connections>") {
				t.Errorf("%q: the workspace's connections were never read", tc.msg)
			}
		})
	}
}

// A name two connections share is not guessed at: the message goes on to the
// LLM (here, gated) exactly as it did before, and no connector type is adopted.
func TestChatDoesNotGuessBetweenConnectionsThatShareAName(t *testing.T) {
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`
	catalog := withFakeCatalog(t, demoCatalog...)
	catalog.connections = namedConnectionsWorkspace
	catalog.otherErrs = true
	completionStub(t, http.StatusServiceUnavailable, gated)

	resp := sendNewChatMessageInWorkspace(t, "sync mysql to Analytics")

	for _, key := range []string{"source_type", "destination_type"} {
		if got, _ := resp.Data[key].(string); got == "bigquery" || got == "snowflake" {
			t.Fatalf("%s=%q: a name two connections share was resolved to one of them (type=%q message=%q)",
				key, got, resp.Type, resp.Message)
		}
	}
}

// With no active workspace there is nothing to read, and nothing is queried.
func TestNamedConnectionRewriteNeedsAWorkspace(t *testing.T) {
	catalog := withFakeCatalog(t, demoCatalog...)
	catalog.connections = namedConnectionsWorkspace
	if got := (&ChatHandler{}).rewriteNamedConnections("", "sync sample data to Demo warehouse"); got != "sync sample data to Demo warehouse" {
		t.Fatalf("rewrote without a workspace: %q", got)
	}
	if catalog.wasAsked("<connections>") {
		t.Fatal("queried connections with no workspace")
	}
}

// One-word names that are everyday words are never rewrite targets, whichever
// list calls them noise: "data" is a connector stopword, "warehouse" and
// "tables" are hasUnknownConnectorPeer's noise words. A multi-word name made of
// them ("Demo warehouse") is still specific, and is rewritten.
func TestNamedConnectionRewriteLeavesEverydayWordsAlone(t *testing.T) {
	catalog := withFakeCatalog(t, demoCatalog...)
	catalog.connections = []fakeConnectionRow{
		{"data", "", "mysql", "source"},
		{"warehouse", "", "bigquery", "destination"},
		{"Tables", "", "mysql", "source"},
		{"Demo warehouse", "", "postgresql", "destination"},
	}
	h := &ChatHandler{}
	for msg, want := range map[string]string{
		"sync the data tables to the warehouse":  "sync the data tables to the warehouse",
		"sync the data tables to demo warehouse": "sync the data tables to postgresql",
	} {
		if got := h.rewriteNamedConnections("ws-1", msg); got != want {
			t.Errorf("rewriteNamedConnections(%q) = %q, want %q", msg, got, want)
		}
	}
}

// The same class at every other turn that reads connectors out of a reply: the
// answer to "which destination?", and a new request typed while the chat waits
// on a role or a confirmation. Each knew connector types only.
func TestEveryChatTurnResolvesSavedConnectionNames(t *testing.T) {
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`
	setup := func(t *testing.T) (*gin.Context, *chat.ConversationContext) {
		t.Helper()
		catalog := withFakeCatalog(t, demoCatalog...)
		catalog.connections = namedConnectionsWorkspace
		catalog.otherErrs = true
		completionStub(t, http.StatusServiceUnavailable, gated)
		c := newIntentTestContext(t)
		c.Set(ctxWorkspaceID, "ws-1")
		return c, chat.NewConversationContext("u1", "s1")
	}

	t.Run("slot filling", func(t *testing.T) {
		c, conv := setup(t)
		conv.SetState(chat.StateAwaitingDestination)
		conv.SetPendingIntent(&chat.PendingIntent{
			Action: "data_sync", SourceType: "sample-data", OriginalRequest: "sync sample data",
		})
		resp := (&ChatHandler{}).handleSlotFilling(context.Background(), c, conv, "Demo warehouse", "trace", "s1")
		if got := conv.GetPendingIntent().DestinationType; got != "postgresql" {
			t.Fatalf(`answering "Demo warehouse" filled destination=%q, want postgresql (type=%q message=%q)`, got, resp.Type, resp.Message)
		}
		// checkConnections reads OriginalRequest to pick the concrete connection.
		if got := conv.GetPendingIntent().OriginalRequest; !strings.Contains(got, "Demo warehouse") {
			t.Errorf("OriginalRequest %q lost the connection the answer named", got)
		}
	})

	t.Run("edit while awaiting confirmation", func(t *testing.T) {
		c, conv := setup(t)
		conv.SetState(chat.StateAwaitingConfirmation)
		conv.SetPendingIntent(&chat.PendingIntent{
			Action: "data_sync", SourceType: "mysql", DestinationType: "aws-s3", OriginalRequest: "mysql to s3",
		})
		resp := (&ChatHandler{}).handleConfirmation(context.Background(), c, conv, "change the destination to Demo warehouse", "trace", "s1", "u1")
		pi := conv.GetPendingIntent()
		if pi == nil || pi.SourceType != "mysql" || pi.DestinationType != "postgresql" {
			t.Fatalf("the edit was not applied: pending=%+v (type=%q message=%q)", pi, resp.Type, resp.Message)
		}
	})

	for _, state := range []chat.ConversationState{chat.StateAwaitingConfirmation, chat.StateAwaitingRole} {
		t.Run("new request while "+string(state), func(t *testing.T) {
			c, conv := setup(t)
			conv.SetState(state)
			conv.SetPendingIntent(&chat.PendingIntent{
				Action: "data_sync", SourceType: "mysql", DestinationType: "aws-s3",
				OriginalRequest: "mysql to s3", UnresolvedConnector: "mysql",
			})
			h := &ChatHandler{}
			var resp ChatMessageResponse
			if state == chat.StateAwaitingRole {
				resp = h.handleRoleClarification(context.Background(), c, conv, "copy Orders DB to Demo warehouse", "trace", "s1", "u1")
			} else {
				resp = h.handleConfirmation(context.Background(), c, conv, "copy Orders DB to Demo warehouse", "trace", "s1", "u1")
			}
			pi := conv.GetPendingIntent()
			if pi == nil || pi.SourceType != "mysql" || pi.DestinationType != "postgresql" {
				t.Fatalf("the new request was not taken up: pending=%+v (type=%q message=%q)", pi, resp.Type, resp.Message)
			}
		})
	}
}
