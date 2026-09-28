package handlers

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/http"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"api-gateway/internal/chat"
	"api-gateway/internal/db"
)

// The zero-credential first-run demo seeds a sample-data source and a postgresql
// destination, then hands off to the chat with a prompt naming both. A fresh
// install has no LLM, so that prompt must be read by the deterministic paths
// alone: sample-data needs a connector_catalog row (migration 120), a spoken
// "sample data" must fold to the connector id, and the slot-filling fallback must
// accept any catalog connector rather than a hard-coded list that omitted it.
//
// "sample data" is also ordinary English, and migration 120 makes sample-data an
// active connector on every install. So the fold is only a second reading: a
// connector the user named next to the phrase keeps its role ("copy my postgres
// sample data to s3" is postgresql -> aws-s3, never the demo source).

// demoChatPrompt is the message FirstRunOnboarding.tsx sends, with demo.go's
// connection names (demoSourceName / demoDestinationName).
const demoChatPrompt = "Sync sample-data to postgresql; source connection: Sample data (demo); destination connection: Demo warehouse"

// fakeCatalog stands in for connector_catalog behind db.DB. It answers the one
// query isKnownConnector sends, by name, and records every name it was asked
// about. The order and number of those lookups are incidental (the plain reading
// of a message runs before the "sample data" fold), so unlike sqlmock it does
// not pin them. Any other SQL is an error the test reports: on a query error
// isKnownConnector answers from its hard-coded common set, not the catalog, so a
// stray query would let a test pass without the catalog deciding anything.
type fakeCatalog struct {
	active map[string]bool
	// broken makes every catalog lookup fail, as a DB outage would.
	broken bool
	// connections, when set, answers the workspace connection listing the chat
	// reads to recognise a saved connection's NAME (namedConnectionsQueryRe).
	connections []fakeConnectionRow
	// otherErrs makes any other SQL fail quietly instead of being reported, for
	// end-to-end tests whose later steps (checkConnections) query the DB too.
	otherErrs bool

	mu         sync.Mutex
	asked      map[string]bool
	unexpected []string
}

var catalogCountQueryRe = regexp.MustCompile(`SELECT COUNT\(\*\) FROM connector_catalog\s+WHERE name = \$1 AND status = 'active'`)

// withFakeCatalog swaps db.DB for a catalog whose active connectors are exactly
// the names given, for the length of the test.
func withFakeCatalog(t *testing.T, active ...string) *fakeCatalog {
	t.Helper()
	fc := &fakeCatalog{active: map[string]bool{}, asked: map[string]bool{}}
	for _, name := range active {
		fc.active[name] = true
	}
	sqlDB := sql.OpenDB(fc)
	prev := db.DB
	db.DB = sqlDB
	t.Cleanup(func() {
		db.DB = prev
		sqlDB.Close()
		fc.mu.Lock()
		defer fc.mu.Unlock()
		if len(fc.unexpected) > 0 {
			t.Errorf("unexpected catalog SQL, which isKnownConnector answers from its fallback set: %q", fc.unexpected)
		}
	})
	return fc
}

func (fc *fakeCatalog) wasAsked(name string) bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.asked[name]
}

func (fc *fakeCatalog) Connect(context.Context) (driver.Conn, error) { return fakeCatalogConn{fc}, nil }
func (fc *fakeCatalog) Driver() driver.Driver                        { return fakeCatalogDriver{fc} }

type fakeCatalogDriver struct{ fc *fakeCatalog }

func (d fakeCatalogDriver) Open(string) (driver.Conn, error) { return fakeCatalogConn{d.fc}, nil }

type fakeCatalogConn struct{ fc *fakeCatalog }

func (fakeCatalogConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fake catalog: prepared statements are not supported")
}
func (fakeCatalogConn) Close() error { return nil }
func (fakeCatalogConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake catalog: transactions are not supported")
}

func (c fakeCatalogConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	fc := c.fc
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.connections != nil && namedConnectionsQueryRe.MatchString(query) {
		fc.asked["<connections>"] = true
		return &fakeConnectionRows{rows: fc.connections}, nil
	}
	if fc.otherErrs && !catalogCountQueryRe.MatchString(query) {
		return nil, errors.New("fake catalog: not modelled")
	}
	name, _ := func() (string, bool) {
		if len(args) != 1 {
			return "", false
		}
		s, ok := args[0].Value.(string)
		return s, ok
	}()
	if !catalogCountQueryRe.MatchString(query) || name == "" {
		fc.unexpected = append(fc.unexpected, query)
		return nil, errors.New("fake catalog: unexpected query")
	}
	fc.asked[name] = true
	if fc.broken {
		return nil, errors.New("fake catalog: connection reset")
	}
	var count int64
	if fc.active[name] {
		count = 1
	}
	return &fakeCountRows{count: count}, nil
}

var namedConnectionsQueryRe = regexp.MustCompile(`FROM connections\s+WHERE workspace_id = \$1 AND status = 'active'`)

// fakeConnectionRow is one saved connection: name, alias, connector_type, type.
type fakeConnectionRow struct{ name, alias, connectorType, direction string }

type fakeConnectionRows struct {
	rows []fakeConnectionRow
	i    int
}

func (*fakeConnectionRows) Columns() []string {
	return []string{"name", "alias", "connector_type", "type"}
}
func (*fakeConnectionRows) Close() error { return nil }
func (r *fakeConnectionRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.i]
	r.i++
	dest[0], dest[1], dest[2], dest[3] = row.name, row.alias, row.connectorType, row.direction
	return nil
}

type fakeCountRows struct {
	count int64
	done  bool
}

func (*fakeCountRows) Columns() []string { return []string{"count"} }
func (*fakeCountRows) Close() error      { return nil }
func (r *fakeCountRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.count
	return nil
}

// demoCatalog is every connector these tests name, all active, as on a real
// install once migration 120 has run.
var demoCatalog = []string{"sample-data", "postgresql", "mysql", "aws-s3", "snowflake", "bigquery"}

// With a real catalog the pair fast path asks connector_catalog about both sides.
// The fake reports any query it does not recognise, so the fail-open branch (a
// catalog error accepts any name) cannot make this pass, and without the fold
// "Sample Data to Postgres" never asks about sample-data at all.
func TestSampleDataPairParsesAgainstTheCatalog(t *testing.T) {
	cases := []struct {
		msg      string
		src, dst string
	}{
		{"sync sample-data to postgresql", "sample-data", "postgresql"},
		{"sample data to postgres", "sample-data", "postgresql"},
		{"Sample Data to Postgres", "sample-data", "postgresql"},
		{"load sample data into postgres", "sample-data", "postgresql"},
		{demoChatPrompt, "sample-data", "postgresql"},
		// "sample data" as an ordinary phrase must not steal a side from a
		// connector the user named: an explicit "from X to Y", or a connector
		// that qualifies the phrase ("my postgres sample data").
		{"load sample data from mysql to s3", "mysql", "aws-s3"},
		{"copy my postgres sample data to s3", "postgresql", "aws-s3"},
		{"move mysql sample data to snowflake", "mysql", "snowflake"},
		{"sync postgres sample data to bigquery", "postgresql", "bigquery"},
		{"mysql sample data into snowflake", "mysql", "snowflake"},
	}
	for _, tc := range cases {
		t.Run(tc.msg, func(t *testing.T) {
			catalog := withFakeCatalog(t, demoCatalog...)

			intent := (&ChatHandler{}).quickParseDataSyncIntent(tc.msg)
			if intent == nil {
				t.Fatalf("%q is not read by the fast path with the catalog", tc.msg)
			}
			if intent.SourceType != tc.src || intent.DestinationType != tc.dst {
				t.Fatalf("%q parsed as %q -> %q, want %q -> %q",
					tc.msg, intent.SourceType, intent.DestinationType, tc.src, tc.dst)
			}
			for _, side := range []string{tc.src, tc.dst} {
				if !catalog.wasAsked(side) {
					t.Errorf("%q: the catalog was never asked about %q", tc.msg, side)
				}
			}
		})
	}
}

// The fold is word-bounded, and it is a separate reading: the plain
// canonicalization every parse starts from leaves the phrase alone.
func TestSampleDataFoldIsASecondWordBoundedReading(t *testing.T) {
	if got, want := canonicalizeForPairParse("Sample  Data to Postgres"), "sample data to postgres"; got != want {
		t.Errorf("canonicalizeForPairParse folds the phrase: got %q, want %q", got, want)
	}
	for in, want := range map[string]string{
		"Sample  Data to Postgres":   "sample-data to postgres",
		"sample data (demo)":         "sample-data (demo)",
		"resample data to postgres":  "",
		"sample dataset to postgres": "",
		"mysql to s3":                "",
	} {
		if got := foldSampleData(in); got != want {
			t.Errorf("foldSampleData(%q) = %q, want %q", in, got, want)
		}
	}
}

// The prompt the onboarding card sends reaches the pipeline confirmation with an
// LLM that is not set up, and never calls it. No catalog DB here, so this also
// pins the fallback map's sample-data entry.
func TestSampleDataReachesConfirmationWithoutTheLLM(t *testing.T) {
	pinNoCatalogDB(t)
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`

	for _, msg := range []string{demoChatPrompt, "sample data to postgres"} {
		t.Run(msg, func(t *testing.T) {
			calls := completionStub(t, http.StatusServiceUnavailable, gated)
			resp := sendNewChatMessage(t, msg)
			if n := atomic.LoadInt64(calls); n != 0 {
				t.Fatalf("%q called the LLM %d time(s); the demo must work without one", msg, n)
			}
			if resp.Type != "confirmation" {
				t.Fatalf("%q did not reach the pipeline confirmation: type=%q message=%q", msg, resp.Type, resp.Message)
			}
			if resp.Data["source_type"] != "sample-data" || resp.Data["destination_type"] != "postgresql" {
				t.Fatalf("%q parsed as %v -> %v", msg, resp.Data["source_type"], resp.Data["destination_type"])
			}
		})
	}
}

// A request that names a real connector next to the words "sample data" is read
// exactly as it was before sample-data became a connector, and still without the
// LLM: a pair keeps both named connectors, and a lone connector keeps its role
// and the chat asks for the other side. Only when no other connector is named
// does the phrase stand for the demo source ("sync sample data").
func TestSampleDataNeverOutranksANamedConnectorWithoutTheLLM(t *testing.T) {
	pinNoCatalogDB(t)
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`

	cases := []struct {
		msg, wantType, src, dst string
	}{
		{"copy my postgres sample data to s3", "confirmation", "postgresql", "aws-s3"},
		{"mysql sample data into snowflake", "confirmation", "mysql", "snowflake"},
		{"sync mysql sample data", "slot_filling", "mysql", ""},
		{"export sample data from mysql", "slot_filling", "mysql", ""},
		{"sync sample data", "slot_filling", "sample-data", ""},
	}
	for _, tc := range cases {
		t.Run(tc.msg, func(t *testing.T) {
			calls := completionStub(t, http.StatusServiceUnavailable, gated)
			resp := sendNewChatMessage(t, tc.msg)
			if n := atomic.LoadInt64(calls); n != 0 {
				t.Fatalf("%q called the LLM %d time(s); it names its connectors, so it needs none", tc.msg, n)
			}
			if resp.Type != tc.wantType {
				t.Fatalf("%q: type=%q, want %q (message=%q)", tc.msg, resp.Type, tc.wantType, resp.Message)
			}
			if got, _ := resp.Data["source_type"].(string); got != tc.src {
				t.Errorf("%q: source_type=%q, want %q (message=%q)", tc.msg, got, tc.src, resp.Message)
			}
			if tc.dst != "" {
				if got, _ := resp.Data["destination_type"].(string); got != tc.dst {
					t.Errorf("%q: destination_type=%q, want %q", tc.msg, got, tc.dst)
				}
			} else if got := resp.Data["missing_slot"]; got != "destination" {
				t.Errorf("%q: missing_slot=%v, want destination (message=%q)", tc.msg, got, resp.Message)
			}
		})
	}
}

// Slot filling without an LLM falls back to extractConnectorFromMessage. It must
// accept any catalog connector by its id or display spelling, and still refuse a
// reply that is not a connector.
func TestSlotFillingWithoutLLMAcceptsCatalogConnectors(t *testing.T) {
	gated := `{"error":"llm_not_configured","message":"` + chatGatedSentence + `"}`

	fill := func(t *testing.T, msg string) (ChatMessageResponse, *chat.ConversationContext) {
		t.Helper()
		completionStub(t, http.StatusServiceUnavailable, gated)
		conv := chat.NewConversationContext("u1", "s1")
		conv.SetState(chat.StateAwaitingSource)
		conv.SetPendingIntent(&chat.PendingIntent{
			Action:          "data_sync",
			DestinationType: "postgresql",
			OriginalRequest: "sync to postgresql",
		})
		resp := (&ChatHandler{}).handleSlotFilling(context.Background(), newIntentTestContext(t), conv, msg, "trace", "s1")
		return resp, conv
	}

	t.Run("no catalog", func(t *testing.T) {
		pinNoCatalogDB(t)
		for msg, want := range map[string]string{
			"sample-data":         "sample-data",
			"Sample Data":         "sample-data",
			"PostgreSQL":          "postgresql",
			"use postgres please": "postgresql",
			// A connector named beside the phrase is the answer, not the demo,
			// wherever it sits relative to the phrase.
			"my mysql sample data":     "mysql",
			"the sample data in mysql": "mysql",
		} {
			resp, conv := fill(t, msg)
			if resp.Type != "confirmation" {
				t.Errorf("%q did not fill the source slot: type=%q message=%q", msg, resp.Type, resp.Message)
				continue
			}
			if got := conv.GetPendingIntent().SourceType; got != want {
				t.Errorf("%q filled source = %q, want %q", msg, got, want)
			}
		}
		for _, msg := range []string{"sure", "data", "I am not sure yet"} {
			resp, conv := fill(t, msg)
			if resp.Type == "confirmation" {
				t.Errorf("%q is not a connector but filled the slot with %q", msg, conv.GetPendingIntent().SourceType)
			}
			if got := conv.GetPendingIntent().SourceType; got != "" {
				t.Errorf("%q set source = %q, want it left empty", msg, got)
			}
		}
	})

	// databricks is a catalog connector (migration 076) that the old hard-coded
	// fallback list never named, so this is only accepted by the catalog lookup.
	t.Run("catalog", func(t *testing.T) {
		catalog := withFakeCatalog(t, "databricks", "postgresql")
		resp, conv := fill(t, "Databricks")
		if resp.Type != "confirmation" || conv.GetPendingIntent().SourceType != "databricks" {
			t.Fatalf("catalog connector not accepted: type=%q source=%q message=%q",
				resp.Type, conv.GetPendingIntent().SourceType, resp.Message)
		}
		if !catalog.wasAsked("databricks") {
			t.Fatal("the catalog was never asked about databricks")
		}
	})

	// A failing catalog query falls back to the common set. It used to answer
	// "yes" for every token, so any reply filled the slot with its first word.
	t.Run("catalog errors", func(t *testing.T) {
		catalog := withFakeCatalog(t)
		catalog.broken = true
		for _, msg := range []string{"I am not sure yet", "sure"} {
			resp, conv := fill(t, msg)
			if got := conv.GetPendingIntent().SourceType; resp.Type == "confirmation" || got != "" {
				t.Errorf("%q during a catalog error: type=%q source=%q, want the slot left empty", msg, resp.Type, got)
			}
		}
		resp, conv := fill(t, "use postgres please")
		if resp.Type != "confirmation" || conv.GetPendingIntent().SourceType != "postgresql" {
			t.Errorf("a common connector during a catalog error: type=%q source=%q, want postgresql",
				resp.Type, conv.GetPendingIntent().SourceType)
		}
		if !catalog.wasAsked("i") {
			t.Fatal("the catalog was never asked, so the error path did not run")
		}
	})
}
