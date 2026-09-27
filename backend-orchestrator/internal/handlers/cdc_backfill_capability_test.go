package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
)

const capabilityTestPipelineID = "637a0df2-eb7d-45ea-8d0b-4e9ca954d1f8"

// Connector configs shaped like the three cases prod actually has: the
// blocking-snapshot Postgres connector a small pipeline gets (no channel), the
// incremental one a ≥ threshold pipeline gets (Kafka channel), and MySQL.
var (
	cfgPostgresBlocking = map[string]interface{}{
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"snapshot.mode":   "initial",
	}
	cfgPostgresIncremental = map[string]interface{}{
		"connector.class":         "io.debezium.connector.postgresql.PostgresConnector",
		"snapshot.mode":           "no_data",
		"signal.enabled.channels": "source,kafka",
		"signal.kafka.topic":      "rsync.signals.637a0df2",
	}
	cfgMySQL = map[string]interface{}{
		"connector.class": "io.debezium.connector.mysql.MySqlConnector",
	}
)

func TestBackfillSignalChannel(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
		want string
	}{
		{"blocking postgres has no channel", cfgPostgresBlocking, backfillChannelNone},
		{"incremental postgres signals over kafka", cfgPostgresIncremental, backfillChannelKafka},
		{"mysql falls back to the source signal table", cfgMySQL, backfillChannelSource},
		// A topic alone is not a channel: Debezium only reads it when "kafka" is
		// among the enabled channels.
		{"topic without the kafka channel enabled", map[string]interface{}{
			"connector.class":         "io.debezium.connector.postgresql.PostgresConnector",
			"signal.enabled.channels": "source",
			"signal.kafka.topic":      "rsync.signals.x",
		}, backfillChannelNone},
		{"empty config", map[string]interface{}{}, backfillChannelNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := backfillSignalChannel(tc.cfg); got != tc.want {
				t.Fatalf("backfillSignalChannel = %q; want %q", got, tc.want)
			}
		})
	}
}

// capabilityHarness wires the handler to a sqlmock DB (connector-name lookup)
// and a fake Kafka Connect that serves `cfg` as the connector's config.
func capabilityHarness(t *testing.T, cfg map[string]interface{}) (*capabilityMock, *gin.Engine) {
	t.Helper()
	return capabilityHarnessWith(t, cfg, nil)
}

// capabilityHarnessWith is capabilityHarness with a signal producer for the POST,
// so a test can see what the Kafka channel was sent.
func capabilityHarnessWith(t *testing.T, cfg map[string]interface{}, signals cdcSignalProducer) (*capabilityMock, *gin.Engine) {
	t.Helper()
	return backfillHarness(t, cfg, signals, false)
}

// backfillHarness builds the router; withQueue gives the POST a snapshot
// request store on the same mock DB, so its queueing can be asserted.
func backfillHarness(t *testing.T, cfg map[string]interface{}, signals cdcSignalProducer, withQueue bool) (*capabilityMock, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	connect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/connectors/cdc-637a0df2/config" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(cfg)
	}))
	t.Cleanup(connect.Close)
	t.Setenv("KAFKA_CONNECT_URL", connect.URL)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("FROM cdc_resources").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("cdc-637a0df2"))

	r := gin.New()
	// The gateway calls the orchestrator as a trusted internal service.
	r.Use(func(c *gin.Context) { c.Set("auth_internal", true) })
	r.GET("/cdc/pipelines/:pipeline_id/backfill", GetCDCBackfillCapability(db))
	var queue *cdcsnapshot.Store
	if withQueue {
		queue = cdcsnapshot.NewStore(db)
	}
	r.POST("/cdc/pipelines/:pipeline_id/backfill", BackfillCDCTables(db, signals, queue))
	return &capabilityMock{mock: mock}, r
}

type capabilityMock struct{ mock sqlmock.Sqlmock }

func getCapability(t *testing.T, r *gin.Engine) (int, map[string]interface{}) {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/cdc/pipelines/"+capabilityTestPipelineID+"/backfill", nil))
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	return w.Code, body
}

func TestGetCDCBackfillCapabilityReportsNoChannel(t *testing.T) {
	h, r := capabilityHarness(t, cfgPostgresBlocking)
	code, body := getCapability(t, r)
	if code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (body %v)", code, body)
	}
	if body["supported"] != false {
		t.Fatalf("supported = %v; want false", body["supported"])
	}
	if body["error"] != "cdc_backfill_not_supported" {
		t.Fatalf("error = %v; want cdc_backfill_not_supported", body["error"])
	}
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Control: the same handler says yes when the connector has the channel, so the
// "no" above is the config speaking, not a handler that always refuses.
func TestGetCDCBackfillCapabilityReportsKafkaChannel(t *testing.T) {
	_, r := capabilityHarness(t, cfgPostgresIncremental)
	code, body := getCapability(t, r)
	if code != http.StatusOK || body["supported"] != true || body["signal_channel"] != "kafka" {
		t.Fatalf("got %d %v; want 200 supported=true signal_channel=kafka", code, body)
	}
	if _, has := body["error"]; has {
		t.Fatalf("a supported pipeline carried an error: %v", body["error"])
	}
}

func TestGetCDCBackfillCapabilityReportsMySQLSourceChannel(t *testing.T) {
	_, r := capabilityHarness(t, cfgMySQL)
	code, body := getCapability(t, r)
	if code != http.StatusOK || body["supported"] != true || body["signal_channel"] != "source" {
		t.Fatalf("got %d %v; want 200 supported=true signal_channel=source", code, body)
	}
}

// The GET and the POST must agree: a pipeline the GET calls unsupported is one
// the POST refuses, with the same code and the same (corrected) message.
func TestBackfillPostRefusesWhatCapabilityCallsUnsupported(t *testing.T) {
	h, r := capabilityHarness(t, cfgPostgresBlocking)
	_, capBody := getCapability(t, r)

	h.mock.ExpectQuery("FROM cdc_resources").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("cdc-637a0df2"))
	h.mock.ExpectQuery("SELECT source_connection_id").
		WithArgs(capabilityTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"source_connection_id"}).AddRow("11111111-1111-1111-1111-111111111111"))

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/cdc/pipelines/"+capabilityTestPipelineID+"/backfill",
		bytes.NewBufferString(`{"tables":["public.users"],"mode":"incremental"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	var post map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &post)
	if w.Code != http.StatusBadRequest || post["error"] != "cdc_backfill_not_supported" {
		t.Fatalf("POST got %d %v; want 400 cdc_backfill_not_supported", w.Code, post)
	}
	if post["message"] != capBody["message"] {
		t.Fatalf("POST message %q differs from GET message %q", post["message"], capBody["message"])
	}
	// The old hint named a setting no create path accepts.
	if msg, _ := post["message"].(string); strings.Contains(msg, "snapshot_strategy=incremental") {
		t.Fatalf("message still tells the operator to set snapshot_strategy: %q", msg)
	}
	// Source size no longer decides whether a PostgreSQL connector has a channel,
	// so the message must not send the operator after the row threshold.
	if msg, _ := post["message"].(string); strings.Contains(msg, "1,000,000") || strings.Contains(msg, "MIN_ROWS") {
		t.Fatalf("message still blames the incremental-snapshot row threshold: %q", msg)
	}
	if err := h.mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
