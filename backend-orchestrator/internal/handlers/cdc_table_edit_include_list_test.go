package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
)

// fakeConnectConfig serves GET/PUT /connectors/<name>/config backed by an
// in-memory config, so a table edit followed by a sink respawn read can be
// observed end to end without Kafka Connect.
type fakeConnectConfig struct {
	mu  sync.Mutex
	cfg map[string]interface{}
}

func (f *fakeConnectConfig) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/connectors/cdc-conn/config" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(f.cfg)
		case http.MethodPut:
			var next map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			f.cfg = next
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(next)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestUpdateConnectorTableList_MongoWritesCollectionIncludeList(t *testing.T) {
	fake := &fakeConnectConfig{cfg: map[string]interface{}{
		"connector.class":         "io.debezium.connector.mongodb.MongoDbConnector",
		"topic.prefix":            "rsync.cdc-aaa0ded3",
		"database.include.list":   "datingapp",
		"collection.include.list": "datingapp.users",
	}}
	srv := fake.server(t)

	if err := updateConnectorTableList(context.Background(), srv.URL, "cdc-conn", []string{"datingapp.matches", "messages"}); err != nil {
		t.Fatalf("updateConnectorTableList: %v", err)
	}

	cfg, err := fetchKafkaConnectConfig(context.Background(), srv.URL, "cdc-conn")
	if err != nil {
		t.Fatalf("fetch config: %v", err)
	}
	if got := connectorConfigString(cfg, "collection.include.list"); got != "datingapp.matches,datingapp.messages" {
		t.Fatalf("collection.include.list = %q, want the edited collections", got)
	}
	if _, ok := cfg["table.include.list"]; ok {
		t.Fatalf("table.include.list written on a MongoDB connector: %v", cfg["table.include.list"])
	}

	// The sink respawn path (restartCDCSinkWorker) derives its topics this way.
	topics := deriveCDCSinkTopics(connectorConfigString(cfg, "topic.prefix"), connectorIncludeList(cfg))
	want := []string{"rsync.cdc-aaa0ded3.datingapp.matches", "rsync.cdc-aaa0ded3.datingapp.messages"}
	if !reflect.DeepEqual(topics, want) {
		t.Fatalf("respawn topics = %v, want %v", topics, want)
	}
}

func TestUpdateConnectorTableList_MongoDropsStaleTableIncludeList(t *testing.T) {
	// A connector edited before the fix carries a stray table.include.list,
	// which connectorIncludeList would prefer over the real collection list.
	fake := &fakeConnectConfig{cfg: map[string]interface{}{
		"connector.class":         "io.debezium.connector.mongodb.MongoDbConnector",
		"topic.prefix":            "rsync.cdc-aaa0ded3",
		"collection.include.list": "datingapp.users",
		"table.include.list":      "datingapp.users",
	}}
	srv := fake.server(t)

	if err := updateConnectorTableList(context.Background(), srv.URL, "cdc-conn", []string{"datingapp.matches"}); err != nil {
		t.Fatalf("updateConnectorTableList: %v", err)
	}
	cfg, err := fetchKafkaConnectConfig(context.Background(), srv.URL, "cdc-conn")
	if err != nil {
		t.Fatalf("fetch config: %v", err)
	}
	if _, ok := cfg["table.include.list"]; ok {
		t.Fatalf("stale table.include.list kept: %v", cfg["table.include.list"])
	}
	if got := connectorIncludeList(cfg); !reflect.DeepEqual(got, []string{"datingapp.matches"}) {
		t.Fatalf("include list = %v, want [datingapp.matches]", got)
	}
}

func TestUpdateConnectorTableList_PostgresUnchanged(t *testing.T) {
	fake := &fakeConnectConfig{cfg: map[string]interface{}{
		"connector.class":    "io.debezium.connector.postgresql.PostgresConnector",
		"topic.prefix":       "rsync.cdc-12345678",
		"database.dbname":    "shop",
		"table.include.list": "public.orders",
	}}
	srv := fake.server(t)

	if err := updateConnectorTableList(context.Background(), srv.URL, "cdc-conn", []string{"public.orders", "customers"}); err != nil {
		t.Fatalf("updateConnectorTableList: %v", err)
	}
	cfg, err := fetchKafkaConnectConfig(context.Background(), srv.URL, "cdc-conn")
	if err != nil {
		t.Fatalf("fetch config: %v", err)
	}
	// Postgres names are written verbatim, as before.
	if got := connectorConfigString(cfg, "table.include.list"); got != "public.orders,customers" {
		t.Fatalf("table.include.list = %q, want verbatim join", got)
	}
	if _, ok := cfg["collection.include.list"]; ok {
		t.Fatalf("collection.include.list written on a Postgres connector")
	}
	topics := deriveCDCSinkTopics(connectorConfigString(cfg, "topic.prefix"), connectorIncludeList(cfg))
	want := []string{"rsync.cdc-12345678.public.orders", "rsync.cdc-12345678.customers"}
	if !reflect.DeepEqual(topics, want) {
		t.Fatalf("respawn topics = %v, want %v", topics, want)
	}
}
