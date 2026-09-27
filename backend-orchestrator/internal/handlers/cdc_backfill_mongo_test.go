package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// A MongoDB connector as the executor now creates it: the Kafka signal channel,
// no read.only (a relational property), no signal.data.collection.
var cfgMongoKafka = map[string]interface{}{
	"connector.class":         "io.debezium.connector.mongodb.MongoDbConnector",
	"topic.prefix":            "cdc-637a0df2",
	"collection.include.list": "shop.orders,shop.customers",
	"database.include.list":   "shop",
	"signal.enabled.channels": "kafka",
	"signal.kafka.topic":      "rsync.signals.637a0df2",
}

type recordedSignal struct {
	topic      string
	key, value []byte
}

type fakeSignalProducer struct{ sent []recordedSignal }

func (f *fakeSignalProducer) EnsureSignalTopic(string) error { return nil }

func (f *fakeSignalProducer) ProduceWithContext(_ context.Context, topic string, key, value []byte) error {
	f.sent = append(f.sent, recordedSignal{topic: topic, key: key, value: value})
	return nil
}

// A MongoDB config carries none of the relational keys, and the database.dbname
// fallback reads a missing key as "<nil>" — so it used to infer "postgresql",
// and the backfill PK-validated MongoDB collections as PostgreSQL tables.
func TestInferDebeziumDatabaseTypeMongoDB(t *testing.T) {
	if got := inferDebeziumDatabaseType(cfgMongoKafka); got != "mongodb" {
		t.Fatalf("MongoDB connector inferred as %q, want mongodb", got)
	}
	classless := map[string]interface{}{"collection.include.list": "shop.orders"}
	if got := inferDebeziumDatabaseType(classless); got != "mongodb" {
		t.Fatalf("class-less MongoDB config inferred as %q, want mongodb", got)
	}
	// Control: the relational engines are unchanged.
	if got := inferDebeziumDatabaseType(cfgPostgresIncremental); got != "postgresql" {
		t.Fatalf("PostgreSQL connector inferred as %q", got)
	}
	if got := inferDebeziumDatabaseType(cfgMySQL); got != "mysql" {
		t.Fatalf("MySQL connector inferred as %q", got)
	}
}

func TestBackfillModes(t *testing.T) {
	mongoWithSignalCollection := map[string]interface{}{}
	for k, v := range cfgMongoKafka {
		mongoWithSignalCollection[k] = v
	}
	mongoWithSignalCollection["signal.data.collection"] = "shop.debezium_signal"

	cases := []struct {
		name string
		cfg  map[string]interface{}
		want []string
	}{
		{"mongodb over kafka is blocking only", cfgMongoKafka, []string{"blocking"}},
		// Someone who configured a writable signal collection has what an
		// incremental MongoDB snapshot needs; don't take it away.
		{"mongodb with a signal collection keeps incremental", mongoWithSignalCollection, []string{"incremental", "blocking"}},
		{"postgres over kafka defaults to incremental", cfgPostgresIncremental, []string{"incremental", "blocking"}},
		{"mysql source table defaults to incremental", cfgMySQL, []string{"incremental", "blocking"}},
		{"no channel, no modes", cfgPostgresBlocking, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := backfillModes(tc.cfg, backfillSignalChannel(tc.cfg)); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("backfillModes = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestGetCDCBackfillCapabilityReportsModes(t *testing.T) {
	cases := []struct {
		name        string
		cfg         map[string]interface{}
		wantModes   []interface{}
		wantDefault interface{}
	}{
		{"mongodb", cfgMongoKafka, []interface{}{"blocking"}, "blocking"},
		{"postgres", cfgPostgresIncremental, []interface{}{"incremental", "blocking"}, "incremental"},
		{"unsupported", cfgPostgresBlocking, []interface{}{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, r := capabilityHarness(t, tc.cfg)
			code, body := getCapability(t, r)
			if code != http.StatusOK {
				t.Fatalf("status = %d (body %v)", code, body)
			}
			if !reflect.DeepEqual(body["modes"], tc.wantModes) {
				t.Fatalf("modes = %#v; want %#v", body["modes"], tc.wantModes)
			}
			if body["default_mode"] != tc.wantDefault {
				t.Fatalf("default_mode = %v; want %v", body["default_mode"], tc.wantDefault)
			}
		})
	}
}

// postBackfill runs the POST against a harness whose DB answers the connector,
// source-connection and destination lookups; destType "postgresql" makes the
// destination relational, so the primary-key check runs.
func postBackfill(t *testing.T, cfg map[string]interface{}, destType, body string) (int, map[string]interface{}, *fakeSignalProducer) {
	t.Helper()
	signals := &fakeSignalProducer{}
	h, r := capabilityHarnessWith(t, cfg, signals)
	h.mock.MatchExpectationsInOrder(false)
	h.mock.ExpectQuery("SELECT source_connection_id").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"source_connection_id"}).AddRow("11111111-1111-1111-1111-111111111111"))
	h.mock.ExpectQuery("SELECT c.connector_type").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow(destType))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/cdc/pipelines/"+capabilityTestPipelineID+"/backfill", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	var out map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out, signals
}

// The default a caller that names no mode gets (the gateway's Edit tables save
// and CDC auto-pickup both send none): BLOCKING on MongoDB, with bare collection
// names qualified by the connector's database, keyed by topic.prefix.
func TestBackfillMongoDefaultsToBlocking(t *testing.T) {
	code, body, signals := postBackfill(t, cfgMongoKafka, "postgresql", `{"tables":["orders","shop.customers"]}`)
	if code != http.StatusOK {
		t.Fatalf("status = %d (body %v); want 200", code, body)
	}
	if body["snapshot_mode"] != "blocking" {
		t.Fatalf("snapshot_mode = %v; want blocking", body["snapshot_mode"])
	}
	if len(signals.sent) != 1 {
		t.Fatalf("signals sent = %d; want 1", len(signals.sent))
	}
	sent := signals.sent[0]
	if sent.topic != "rsync.signals.637a0df2" || string(sent.key) != "cdc-637a0df2" {
		t.Fatalf("sent to %q with key %q", sent.topic, sent.key)
	}
	var sig struct {
		Type string `json:"type"`
		Data struct {
			Type        string   `json:"type"`
			Collections []string `json:"data-collections"`
		} `json:"data"`
	}
	if err := json.Unmarshal(sent.value, &sig); err != nil {
		t.Fatalf("unmarshal %s: %v", sent.value, err)
	}
	if sig.Type != "execute-snapshot" || sig.Data.Type != "BLOCKING" {
		t.Fatalf("signal = %s; want execute-snapshot BLOCKING", sent.value)
	}
	if !reflect.DeepEqual(sig.Data.Collections, []string{"shop.orders", "shop.customers"}) {
		t.Fatalf("data-collections = %v; want db-qualified collections", sig.Data.Collections)
	}
}

// An explicit incremental request on MongoDB is refused before anything is sent,
// and the message says why and what to do instead.
func TestBackfillMongoRefusesIncremental(t *testing.T) {
	code, body, signals := postBackfill(t, cfgMongoKafka, "postgresql", `{"tables":["orders"],"mode":"incremental"}`)
	if code != http.StatusBadRequest || body["error"] != "cdc_backfill_mode_not_supported" {
		t.Fatalf("got %d %v; want 400 cdc_backfill_mode_not_supported", code, body)
	}
	if len(signals.sent) != 0 {
		t.Fatalf("a refused request still sent %d signal(s)", len(signals.sent))
	}
	msg, _ := body["message"].(string)
	for _, want := range []string{"blocking", "pauses", "write"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q does not mention %q", msg, want)
		}
	}
	if !reflect.DeepEqual(body["modes"], []interface{}{"blocking"}) {
		t.Fatalf("modes = %v; want [blocking]", body["modes"])
	}
}

// Control: PostgreSQL's default is still incremental — the MongoDB rule did not
// turn into a global blocking default.
func TestBackfillPostgresStillDefaultsToIncremental(t *testing.T) {
	code, body, signals := postBackfill(t, cfgPostgresIncremental, "bigquery", `{"tables":["public.users"]}`)
	if code != http.StatusOK || body["snapshot_mode"] != "incremental" {
		t.Fatalf("got %d %v; want 200 snapshot_mode=incremental", code, body)
	}
	if len(signals.sent) != 1 || !strings.Contains(string(signals.sent[0].value), `"INCREMENTAL"`) {
		t.Fatalf("signals = %+v; want one INCREMENTAL signal", signals.sent)
	}
}
