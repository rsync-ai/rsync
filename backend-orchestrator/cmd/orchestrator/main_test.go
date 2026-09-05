package main

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// TestRemoteDatabaseViolation pins the fail-loud guard that prevents the
// "silent dev-postgres fallback" on the orchestrator: when wired to the local
// dev Postgres in a remote-DB deployment, the orchestrator comes up healthy but
// pointed at a database that holds none of the real pipelines. See
// requireRemoteDatabase. Mirrors the api-gateway + temporal-adapter guards.
func TestRemoteDatabaseViolation(t *testing.T) {
	cases := []struct {
		name          string
		requireRemote bool
		host          string
		wantViolation bool
	}{
		// Guard inactive (dev/e2e): local Postgres is legitimate, never flag.
		{"marker off ignores local service", false, "postgres", false},
		{"marker off ignores localhost", false, "localhost", false},
		{"marker off ignores loopback", false, "127.0.0.1", false},
		{"marker off ignores empty", false, "", false},
		{"marker off ignores remote", false, "db.example.com", false},

		// Guard active (staging/prod): local/empty host must fail loud.
		{"compose service host flagged", true, "postgres", true},
		{"localhost flagged", true, "localhost", true},
		{"loopback v4 flagged", true, "127.0.0.1", true},
		{"loopback v6 flagged", true, "::1", true},
		{"loopback v6 bracketed flagged", true, "[::1]", true},
		{"empty host flagged", true, "", true},
		{"whitespace-only host flagged", true, "   ", true},
		{"whitespace-padded local flagged", true, "  postgres  ", true},
		{"uppercase local flagged", true, "LOCALHOST", true},
		{"mixed-case padded local flagged", true, "  Postgres ", true},

		// Guard active + genuine remote host: must pass.
		{"managed remote ok", true, "pg-managed.example.com", false},
		{"other remote host ok", true, "db.internal.example.com", false},
		{"remote with whitespace ok", true, "  db.example.com  ", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := remoteDatabaseViolation(tc.requireRemote, tc.host)
			if (got != "") != tc.wantViolation {
				t.Fatalf("remoteDatabaseViolation(%v, %q) = %q; wantViolation=%v",
					tc.requireRemote, tc.host, got, tc.wantViolation)
			}
		})
	}
}

// TestCDCControlOutcome pins KI-CDC-CONTROL-ACTIONS-FALSE-SUCCESS-TOAST: the CDC
// restart/pause/resume handlers answered HTTP 200 whatever Kafka Connect said,
// putting the real verdict only in the body's `success` field. api-gateway
// forwards the upstream status verbatim and the UI branches on `response.ok`, so
// a refused action produced a green "restarted"/"paused" toast for something that
// never happened. Any non-2xx from Connect must now map to 502 with a message.
func TestCDCControlOutcome(t *testing.T) {
	cases := []struct {
		name           string
		connectStatus  int
		wantOK         bool
		wantHTTPStatus int
	}{
		// Kafka Connect accepted it — the caller sees a real success.
		{"restart accepted 200", http.StatusOK, true, http.StatusOK},
		{"pause accepted 202", http.StatusAccepted, true, http.StatusOK},
		{"accepted 204", http.StatusNoContent, true, http.StatusOK},
		{"accepted 299 upper edge", 299, true, http.StatusOK},

		// The regression itself: every one of these used to answer 200.
		{"connector not found 404", http.StatusNotFound, false, http.StatusBadGateway},
		{"rebalancing 409", http.StatusConflict, false, http.StatusBadGateway},
		{"connect internal error 500", http.StatusInternalServerError, false, http.StatusBadGateway},
		{"connect unavailable 503", http.StatusServiceUnavailable, false, http.StatusBadGateway},
		{"bad request 400", http.StatusBadRequest, false, http.StatusBadGateway},
		{"redirect 302 is not success", http.StatusFound, false, http.StatusBadGateway},
		{"informational 100 is not success", http.StatusContinue, false, http.StatusBadGateway},
		{"zero status is not success", 0, false, http.StatusBadGateway},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, status, errMsg := cdcControlOutcome("restart", "cdc-2cb685ed", tc.connectStatus)
			if ok != tc.wantOK {
				t.Fatalf("cdcControlOutcome(_, _, %d) ok = %v; want %v", tc.connectStatus, ok, tc.wantOK)
			}
			if status != tc.wantHTTPStatus {
				t.Fatalf("cdcControlOutcome(_, _, %d) status = %d; want %d",
					tc.connectStatus, status, tc.wantHTTPStatus)
			}
			// A failure the UI can render needs a non-empty message; a success
			// must not carry one (the toast prefers `error` when present).
			if tc.wantOK && errMsg != "" {
				t.Fatalf("success carried an error message: %q", errMsg)
			}
			if !tc.wantOK && errMsg == "" {
				t.Fatal("refusal carried no error message; the UI toast would be blank")
			}
		})
	}
}

// TestCDCControlOutcomeMessageNamesActionAndConnector keeps the operator-facing
// message specific: "kafka connect refused the pause of cdc-abc (HTTP 409)" is
// actionable, a bare "request failed" is not.
func TestCDCControlOutcomeMessageNamesActionAndConnector(t *testing.T) {
	for _, action := range []string{"restart", "pause", "resume"} {
		_, _, errMsg := cdcControlOutcome(action, "cdc-2cb685ed", http.StatusConflict)
		for _, want := range []string{action, "cdc-2cb685ed", "409"} {
			if !strings.Contains(errMsg, want) {
				t.Errorf("cdcControlOutcome(%q, …) message %q does not mention %q", action, errMsg, want)
			}
		}
	}
}

// TestTopologyRoutesRequireAPrincipal pins the auth gate on the topology group
// in setupRouter. /api/v1 has no global auth, and the group is publicly
// reachable through the Traefik /orchestrator route, so with the middleware
// gone an anonymous internet caller reaches DELETE /topics/:name — a
// broker-level destructive verb against a compacted, infinite-retention CDC
// topic. Sibling groups were already gated; this one was the omission.
//
// The assertion is deliberately two-part. Status 401 alone is weak: six of the
// seven handlers now carry their own principal check as defence in depth and
// would keep answering 401 with the middleware deleted. So the test also pins
// WHICH layer refused — requirePrincipal answers {"error":...} while the
// handlers answer {"success":false,"error":...}. A 401 carrying "success" means
// the request got past the router group and only the inner check saved it.
//
// GET /calculate-partitions is the case that fails on status alone: it is pure
// arithmetic, touching neither the manager nor the db, so it has no handler
// gate to fall back on and answers 200 the moment the middleware is dropped.
//
// One subtest does NOT discriminate and is here for coverage only: the
// provision route delegates to cdcPrincipal, whose 401 body is byte-identical
// to the middleware's. Deleting requirePrincipal takes the other six RED.
func TestTopologyRoutesRequireAPrincipal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// Production-like so requirePrincipal's X-User-ID dev fallback is off, and
	// an empty internal secret so the X-Internal-Secret branch cannot match an
	// absent header.
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("INTERNAL_SERVICE_SECRET", "")

	// A real but never-connecting handle: setupRouter starts the healthwatch
	// goroutine, so db must be non-nil-safe. Anonymous requests never reach it
	// anyway — requirePrincipal short-circuits on an empty token before it
	// queries sessions.
	db, err := sql.Open("postgres", "postgres://127.0.0.1:1/rsync_none?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	router := setupRouter(nil, &kafka.TopologyManager{}, nil, nil, nil, db, nil, nil)

	const topic = "rsync.cdc.abd8a64d"
	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		// The discriminating one: no handler-level gate behind it.
		{"calculate partitions", http.MethodGet, "/api/v1/topology/calculate-partitions?sync_mode=cdc&table_count=4", ""},
		{"list topics", http.MethodGet, "/api/v1/topology/topics", ""},
		{"read topic", http.MethodGet, "/api/v1/topology/topics/" + topic, ""},
		{"create topic", http.MethodPost, "/api/v1/topology/topics", `{"topic_name":"` + topic + `","partitions":3,"replication_factor":1}`},
		{"provision for pipeline", http.MethodPost, "/api/v1/topology/topics/pipeline", `{"pipeline_id":"abd8a64d-0000-0000-0000-000000000000","sync_mode":"cdc"}`},
		{"delete topic", http.MethodDelete, "/api/v1/topology/topics/" + topic, ""},
		{"repartition topic", http.MethodPut, "/api/v1/topology/topics/" + topic + "/partitions", `{"partitions":64}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Fatalf("anonymous %s %s: got %d, want 401 — the topology group is not gated",
					tc.method, tc.path, w.Code)
			}
			if strings.Contains(w.Body.String(), "success") {
				t.Fatalf("anonymous %s %s: the 401 came from the handler, not requirePrincipal (%s) — "+
					"the router-group gate is gone and only defence in depth answered",
					tc.method, tc.path, w.Body.String())
			}
		})
	}
}
