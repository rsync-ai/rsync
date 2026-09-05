package handlers

import (
	"database/sql/driver"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

const (
	scopeMe        = "11111111-1111-1111-1111-111111111111"
	scopeStranger  = "22222222-2222-2222-2222-222222222222"
	scopeWorkspace = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	scopeID8       = "abd8a64d"
)

// TestTopicPipelineID8 pins the join key. The whole tenant gate rests on
// recovering the pipeline id from a topic name, so a shape that stops parsing
// silently downgrades that route to "internal callers only" (deny) — and a
// shape that parses too eagerly points the gate at the wrong pipeline.
func TestTopicPipelineID8(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")

	scoped := map[string]string{
		"rsync.pipeline.abd8a64d.data":            scopeID8,
		"rsync.pipeline.abd8a64d.data.dlq":        scopeID8,
		"rsync.cdc.abd8a64d":                      scopeID8,
		"rsync.cdc-abd8a64d":                      scopeID8,
		"rsync.cdc-abd8a64d.inventory.orders":     scopeID8,
		"rsync.cdc-abd8a64d.inventory.orders.dlq": scopeID8,
		"rsync.schemahistory.cdc-abd8a64d":        scopeID8,
		"rsync.signals.abd8a64d":                  scopeID8,
		"rsync.pipeline.ABD8A64D.data":            scopeID8, // normalized like cdc_kafka_teardown.go:164
		"pipeline.abd8a64d.data":                  scopeID8, // un-namespaced deployment
		"rsync.cdc-abd8a64d.public.customers.dlq": scopeID8,
		"rsync.pipeline.0123abcd.data":            "0123abcd",
	}
	for name, want := range scoped {
		got, ok := topicPipelineID8(name)
		if !ok {
			t.Errorf("%q: not recognized as pipeline-scoped", name)
			continue
		}
		if got != want {
			t.Errorf("%q: id8 = %q; want %q", name, got, want)
		}
	}

	// Not pipeline-scoped. Every one of these must fall to the "shared platform
	// topic" branch rather than resolve to some pipeline.
	unscoped := []string{
		"rsync.agent.control.results",
		"rsync.pii.scan.request",
		"rsync.task.lifecycle",
		"_rsync-connect-offsets",
		"payments.transactions",
		"",
		// The planner names its CDC topic after the CONNECTION, not a pipeline
		// (llm-service/src/agents/planner/strategies.py:650) — there is no id8
		// in it to authorize against.
		"rsync.cdc.acme-prod-postgres",
		// Near-misses that must not be mistaken for an id8.
		"rsync.pipeline.abd8a64.data",   // 7 chars
		"rsync.pipeline.abd8a64dd.data", // 9 chars
		"rsync.cdc-zzzzzzzz",            // not hex
		"rsync.pipelines.abd8a64d.data", // different namespace
	}
	for _, name := range unscoped {
		if id8, ok := topicPipelineID8(name); ok {
			t.Errorf("%q: wrongly resolved to pipeline id8 %q", name, id8)
		}
	}
}

// TestTopicReadAllowed pins the read floor: any workspace role is enough (a
// viewer may look), a stranger is not, and an empty result set fails closed.
func TestTopicReadAllowed(t *testing.T) {
	cases := []struct {
		name string
		rows []resourceAccess
		want bool
	}{
		{"no matching pipeline fails closed", nil, false},
		{"viewer may read", []resourceAccess{{found: true, workspaceID: scopeWorkspace, owner: scopeStranger, memberRole: "viewer"}}, true},
		{"member may read", []resourceAccess{{found: true, workspaceID: scopeWorkspace, owner: scopeStranger, memberRole: "member"}}, true},
		{"non-member refused", []resourceAccess{{found: true, workspaceID: scopeWorkspace, owner: scopeStranger, memberRole: ""}}, false},
		{"legacy row, creator may read", []resourceAccess{{found: true, workspaceID: "", owner: scopeMe}}, true},
		{"legacy row, non-creator refused", []resourceAccess{{found: true, workspaceID: "", owner: scopeStranger}}, false},
		{"legacy row with no owner refused", []resourceAccess{{found: true, workspaceID: "", owner: ""}}, false},
		// id8 collision: one topic carries both pipelines, so partial access is
		// no access.
		{"id8 collision needs access to both", []resourceAccess{
			{found: true, workspaceID: scopeWorkspace, owner: scopeMe, memberRole: "owner"},
			{found: true, workspaceID: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb", owner: scopeStranger, memberRole: ""},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := topicReadAllowed(tc.rows, scopeMe); got != tc.want {
				t.Fatalf("topicReadAllowed = %v; want %v", got, tc.want)
			}
		})
	}
}

// TestRequireTopicPipelineScope is the gate itself: it must resolve the id8 out
// of the topic name, ask the database who owns that pipeline, and refuse a
// caller who holds a perfectly valid session in some OTHER workspace. That
// caller is the live multi-tenant hole — requirePrincipal admits them, and
// before this gate nothing else asked a second question.
func TestRequireTopicPipelineScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")

	newCtx := func(w *httptest.ResponseRecorder, method string) *gin.Context {
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(method, "/api/v1/topology/topics/rsync.cdc."+scopeID8, nil)
		return c
	}

	t.Run("stranger with a valid session is refused", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		// LEFT JOIN yields a NULL role for a caller with no membership row.
		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(scopeID8, scopeStranger).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(scopeWorkspace, scopeMe, nil))

		w := httptest.NewRecorder()
		c := newCtx(w, http.MethodDelete)
		c.Set("auth_user_id", scopeStranger)

		h := &TopologyHandler{db: db}
		if h.requireTopicPipelineScope(c, "rsync.cdc."+scopeID8) {
			t.Fatal("another tenant's compacted CDC topic was deletable")
		}
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d; want 403 (body %s)", w.Code, w.Body.String())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("ownership was never queried: %v", err)
		}
	})

	t.Run("workspace member passes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(scopeID8, scopeMe).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(scopeWorkspace, scopeStranger, "member"))

		w := httptest.NewRecorder()
		c := newCtx(w, http.MethodDelete)
		c.Set("auth_user_id", scopeMe)

		h := &TopologyHandler{db: db}
		if !h.requireTopicPipelineScope(c, "rsync.cdc."+scopeID8) {
			t.Fatalf("a member of the owning workspace was refused; body=%s", w.Body.String())
		}
	})

	t.Run("viewer may read but not repartition", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(scopeID8, scopeMe).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
				AddRow(scopeWorkspace, scopeMe, "viewer"))

		w := httptest.NewRecorder()
		c := newCtx(w, http.MethodPut)
		c.Set("auth_user_id", scopeMe)

		h := &TopologyHandler{db: db}
		if h.requireTopicPipelineScope(c, "rsync.cdc."+scopeID8) {
			t.Fatal("a viewer was allowed to mutate a topic")
		}
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d; want 403", w.Code)
		}
	})

	t.Run("shared platform topic is internal-only", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		w := httptest.NewRecorder()
		c := newCtx(w, http.MethodDelete)
		c.Set("auth_user_id", scopeMe)

		h := &TopologyHandler{db: db}
		// rsync.agent.* carries every tenant's control plane; there is no single
		// tenant who may delete it.
		if h.requireTopicPipelineScope(c, "rsync.agent.control.results") {
			t.Fatal("a user principal was allowed to delete a shared control topic")
		}
		if w.Code != http.StatusForbidden {
			t.Fatalf("status = %d; want 403", w.Code)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("the shared-topic refusal hit the database: %v", err)
		}
	})

	t.Run("orphan topic with no owning pipeline is refused", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		mock.ExpectQuery(`FROM pipelines p`).
			WithArgs(scopeID8, scopeMe).
			WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}))

		w := httptest.NewRecorder()
		c := newCtx(w, http.MethodDelete)
		c.Set("auth_user_id", scopeMe)

		h := &TopologyHandler{db: db}
		// Reclaiming an orphan is POST /cdc/kafka-teardown's job (internal
		// only); a tenant guessing id8s must not get there.
		if h.requireTopicPipelineScope(c, "rsync.cdc."+scopeID8) {
			t.Fatal("a topic with no owning pipeline was deletable by a tenant")
		}
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d; want 404", w.Code)
		}
	})

	t.Run("internal callers pass without a query", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()

		w := httptest.NewRecorder()
		c := newCtx(w, http.MethodPost)
		c.Set("auth_internal", true)

		h := &TopologyHandler{db: db}
		// The planner POSTs here with X-Internal-Secret (strategies.py:766) and
		// has already applied its own gate.
		if !h.requireTopicPipelineScope(c, "rsync.agent.control.results") {
			t.Fatal("a trusted internal caller was refused")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("internal caller hit the database: %v", err)
		}
	})

	t.Run("anonymous context is refused", func(t *testing.T) {
		w := httptest.NewRecorder()
		c := newCtx(w, http.MethodDelete)

		h := &TopologyHandler{db: nil}
		if h.requireTopicPipelineScope(c, "rsync.cdc."+scopeID8) {
			t.Fatal("a context with no principal was allowed")
		}
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d; want 401", w.Code)
		}
	})
}

// TestMutatingTopologyRoutesRefuseAnotherTenantsTopic drives the real routes.
// The handler holds a nil *kafka.TopologyManager, so anything that got past the
// gate would panic rather than answer — which is exactly the pre-fix behaviour
// for these in-namespace names, since the prefix guard waves them through.
func TestMutatingTopologyRoutesRefuseAnotherTenantsTopic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
	t.Setenv(envAllowLegacyUnprefixedTopics, "")

	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		query      string // which table the gate must consult
		args       []driver.Value
		wantStatus int
	}{
		{
			name: "delete another tenant's compacted CDC topic", method: http.MethodDelete,
			path:  "/api/v1/topology/topics/rsync.cdc." + scopeID8,
			query: `FROM pipelines p`, args: []driver.Value{scopeID8, scopeStranger},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "repartition another tenant's batch topic", method: http.MethodPut,
			path:  "/api/v1/topology/topics/rsync.pipeline." + scopeID8 + ".data/partitions",
			body:  `{"partitions":64}`,
			query: `FROM pipelines p`, args: []driver.Value{scopeID8, scopeStranger},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "create a topic in another tenant's pipeline namespace", method: http.MethodPost,
			path:  "/api/v1/topology/topics",
			body:  `{"topic_name":"rsync.pipeline.` + scopeID8 + `.data","partitions":3,"replication_factor":1}`,
			query: `FROM pipelines p`, args: []driver.Value{scopeID8, scopeStranger},
			wantStatus: http.StatusForbidden,
		},
		{
			// CreateTopicForPipeline was confined only by name construction: it
			// never checked the supplied pipeline_id at all.
			name: "provision a topic for another tenant's pipeline", method: http.MethodPost,
			path:  "/api/v1/topology/topics/pipeline",
			body:  `{"pipeline_id":"33333333-3333-3333-3333-333333333333","sync_mode":"cdc","table_count":4}`,
			query: `FROM pipelines p`, args: []driver.Value{"33333333-3333-3333-3333-333333333333", scopeStranger},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "read another tenant's topic config", method: http.MethodGet,
			path:  "/api/v1/topology/topics/rsync.cdc." + scopeID8,
			query: `FROM pipelines p`, args: []driver.Value{scopeID8, scopeStranger},
			wantStatus: http.StatusNotFound, // read side does not confirm existence
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer db.Close()

			mock.ExpectQuery(tc.query).
				WithArgs(tc.args...).
				WillReturnRows(sqlmock.NewRows([]string{"workspace_id", "created_by", "role"}).
					AddRow(scopeWorkspace, scopeMe, nil))

			h := NewTopologyHandler(nil, db) // nil manager: reaching it panics
			r := gin.New()
			// Stand in for requirePrincipal: an authenticated stranger.
			r.Use(func(c *gin.Context) { c.Set("auth_user_id", scopeStranger) })
			h.RegisterRoutes(r.Group("/api/v1/topology"))

			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("%s %s: got %d, want %d (body %s)",
					tc.method, tc.path, w.Code, tc.wantStatus, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("the route did not consult the tenancy oracle: %v", err)
			}
		})
	}
}

// TestVisibleTopicsConfinesTheListing covers the enumeration route. ListTopics
// consulted neither the allowlist nor the caller: it returned the broker's whole
// topic list, which on a BYO/EKS cluster is a directory of the customer's
// unrelated applications, plus every other tenant's pipeline id8.
func TestVisibleTopicsConfinesTheListing(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	t.Setenv("KAFKA_OWNED_TOPIC_PREFIXES", "")
	t.Setenv(envAllowLegacyUnprefixedTopics, "")

	const otherID8 = "beefcafe"
	broker := map[string]*kafka.TopicInfo{
		"rsync.cdc." + scopeID8:                {Name: "rsync.cdc." + scopeID8},
		"rsync.pipeline." + scopeID8 + ".data": {Name: "rsync.pipeline." + scopeID8 + ".data"},
		"rsync.cdc." + otherID8:                {Name: "rsync.cdc." + otherID8},                // another tenant
		"rsync.pipeline." + otherID8 + ".data": {Name: "rsync.pipeline." + otherID8 + ".data"}, // another tenant
		"rsync.agent.control.results":          {Name: "rsync.agent.control.results"},          // shared platform infra
		"payments.transactions":                {Name: "payments.transactions"},                // the customer's own app
		"customer.billing.events":              {Name: "customer.billing.events"},              // the customer's own app
		"__consumer_offsets":                   {Name: "__consumer_offsets", IsInternal: true}, // kafka internal
	}
	// ListTopics returns a map, so the handler's output order is not defined;
	// sort before comparing rather than pinning an accident.
	names := func(in []*kafka.TopicInfo) []string {
		out := make([]string, 0, len(in))
		for _, t := range in {
			out = append(out, t.Name)
		}
		sort.Strings(out)
		return out
	}

	t.Run("user principal sees only their own pipelines plus shared infra", func(t *testing.T) {
		got := names(visibleTopics(broker, false, false, map[string]bool{scopeID8: true}))
		want := []string{
			"rsync.agent.control.results",
			"rsync.cdc." + scopeID8,
			"rsync.pipeline." + scopeID8 + ".data",
		}
		assertSameStrings(t, got, want)
	})

	t.Run("include_internal cannot escape the namespace", func(t *testing.T) {
		got := names(visibleTopics(broker, true, false, map[string]bool{scopeID8: true}))
		for _, n := range got {
			if n == "__consumer_offsets" || n == "payments.transactions" {
				t.Fatalf("include_internal=true leaked a foreign topic: %v", got)
			}
		}
	})

	t.Run("internal principal still never sees the customer's own topics", func(t *testing.T) {
		got := names(visibleTopics(broker, false, true, nil))
		for _, n := range got {
			if n == "payments.transactions" || n == "customer.billing.events" {
				t.Fatalf("the confinement leaked a foreign topic to an internal caller: %v", got)
			}
		}
		// An internal caller does see every tenant's topic — that is what the
		// teardown/provision services need.
		if len(got) != 5 {
			t.Fatalf("internal listing = %v; want all 5 platform topics", got)
		}
	})

	t.Run("a caller with no reachable pipelines sees no pipeline topics", func(t *testing.T) {
		got := names(visibleTopics(broker, false, false, nil))
		assertSameStrings(t, got, []string{"rsync.agent.control.results"})
	})
}

func assertSameStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v; want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v; want %v", got, want)
		}
	}
}

// TestListTopicsRefusesAnonymousAndFailsClosedOnLookupError proves the list
// route resolves the caller's reachable set BEFORE it talks to the broker: a
// failed lookup must not fall through to an unfiltered listing.
func TestListTopicsRefusesAnonymousAndFailsClosedOnLookupError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("anonymous", func(t *testing.T) {
		h := NewTopologyHandler(nil, nil) // nil manager: reaching the broker panics
		r := gin.New()
		h.RegisterRoutes(r.Group("/api/v1/topology"))

		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/topology/topics", nil))
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d; want 401 (body %s)", w.Code, w.Body.String())
		}
	})

	t.Run("lookup failure does not fall through to the broker", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(`FROM pipelines p`).WillReturnError(errLookup)

		h := NewTopologyHandler(nil, db)
		r := gin.New()
		r.Use(func(c *gin.Context) { c.Set("auth_user_id", scopeMe) })
		h.RegisterRoutes(r.Group("/api/v1/topology"))

		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/topology/topics", nil))
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d; want 500 (body %s)", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "authorization check failed") {
			t.Errorf("unexpected body: %s", w.Body.String())
		}
	})
}

var errLookup = &lookupError{}

type lookupError struct{}

func (*lookupError) Error() string { return "connection refused" }
