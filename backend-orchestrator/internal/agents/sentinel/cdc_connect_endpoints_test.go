package sentinel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// recordingConnect stands in for Kafka Connect and remembers every path asked of it.
type recordingConnect struct {
	mu    sync.Mutex
	paths []string
}

func (r *recordingConnect) record(p string) {
	r.mu.Lock()
	r.paths = append(r.paths, p)
	r.mu.Unlock()
}

func (r *recordingConnect) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.paths))
	copy(out, r.paths)
	return out
}

// The CDC sentinel asked Kafka Connect for /connectors/<name>/metrics on every monitoring
// pass, for every running CDC pipeline. That endpoint is not part of the Kafka Connect
// REST API — Debezium publishes its per-table counters over JMX, and this deployment runs
// no JMX exporter (docker-compose.yml kafka-connect sets no JMX env at all) — so the
// request 404s every time and always will.
//
// The work it fed was equally imaginary: extractTableMetrics had an empty body, so the
// per-table stats map it was meant to fill stayed empty and the 10s tableStatsLoop
// flushing that map emitted nothing, for every CDC pipeline, forever. Real CDC per-table
// stats already come from internal/agents/cdcstats, which consumes the Debezium topics
// directly and emits TABLE_STATS events (main.go:790, ENABLE_CDC_TABLE_STATS).
func TestCDCSentinelDoesNotPollAKafkaConnectEndpointThatDoesNotExist(t *testing.T) {
	rec := &recordingConnect{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rec.record(req.URL.Path)
		if req.URL.Path == "/connectors" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"cdc-abcd1234":{"status":{"connector":{"state":"RUNNING"},"tasks":[]}}}`))
			return
		}
		// Everything else is what the real broker does with an unknown route.
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`SELECT id\s+FROM pipelines`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("abcd1234-0000-0000-0000-000000000001"))

	s := &CDCSentinel{
		db:         db,
		httpClient: srv.Client(),
		connectURL: srv.URL,
	}

	s.checkActivePipelines(context.Background())

	for _, p := range rec.seen() {
		if strings.Contains(p, "/metrics") {
			t.Errorf("sentinel requested %q — Kafka Connect has no metrics endpoint, so this 404s on every tick of every CDC pipeline and feeds a table-stats path that emits nothing", p)
		}
	}
}
