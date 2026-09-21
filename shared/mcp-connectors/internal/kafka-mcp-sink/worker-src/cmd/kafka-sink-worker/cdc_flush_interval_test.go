package main

// The object-storage CDC flush interval is set per destination with
// max_file_interval_seconds. Before this, the only knob was
// kafka_sink_worker.cdc_batching.flush_interval_seconds, which nothing upstream ever
// sent, so every object-storage CDC pipeline was pinned to 30s.
//
// These tests cover the whole worker side: how the value is parsed and refused (and
// that the connector, which checks first, agrees case by case), that the batcher's
// timer really uses it, that a file is still written before its offsets are committed,
// that the file name does not depend on when the flush happens, that the shutdown
// drain still writes a batch held for the longest allowed interval, and that the stall
// watchdog cannot restart the worker while a batch is legitimately waiting.

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// flushIntervalFixture is testdata/flush_interval_cases.json, the case list the
// kafka-mcp-sink connector's test runs too. It is decoded the way loadConfig decodes
// CONFIG (plain json.Unmarshal), so numbers arrive as float64 exactly as they do in
// production.
type flushIntervalFixture struct {
	MinSeconds int64 `json:"min_seconds"`
	MaxSeconds int64 `json:"max_seconds"`
	Cases      []struct {
		Name   string      `json:"name"`
		Value  interface{} `json:"value"`
		Expect interface{} `json:"expect"`
	} `json:"cases"`
}

func loadFlushIntervalFixture(t *testing.T) flushIntervalFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "flush_interval_cases.json"))
	if err != nil {
		t.Fatalf("read shared cases: %v", err)
	}
	var fx flushIntervalFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("decode shared cases: %v", err)
	}
	return fx
}

func TestObjectFlushIntervalOverride_SharedCasesWithConnector(t *testing.T) {
	fx := loadFlushIntervalFixture(t)
	if fx.MinSeconds != minObjectFlushIntervalSeconds || fx.MaxSeconds != maxObjectFlushIntervalSeconds {
		t.Fatalf("shared cases are for %d-%d s but the worker allows %d-%d s; update the fixture, main.go and connector.py together",
			fx.MinSeconds, fx.MaxSeconds, minObjectFlushIntervalSeconds, maxObjectFlushIntervalSeconds)
	}
	kinds := map[string]int{}
	for _, tc := range fx.Cases {
		switch want := tc.Expect.(type) {
		case float64:
			kinds["accepted"]++
		case string:
			kinds[want]++
		}
	}
	if kinds["accepted"] < 5 || kinds["unset"] < 5 || kinds["refused"] < 20 || len(fx.Cases) != kinds["accepted"]+kinds["unset"]+kinds["refused"] {
		t.Fatalf("shared cases look truncated or malformed: %v of %d", kinds, len(fx.Cases))
	}

	for _, tc := range fx.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			d, ok, err := objectFlushIntervalOverride(map[string]interface{}{objectFlushIntervalKey: tc.Value})
			switch tc.Expect {
			case "unset":
				if err != nil || ok || d != 0 {
					t.Fatalf("%#v: got (%v, %v, %v), want unset (0, false, nil)", tc.Value, d, ok, err)
				}
			case "refused":
				if err == nil || ok || d != 0 {
					t.Fatalf("%#v: got (%v, %v, %v), want refused", tc.Value, d, ok, err)
				}
			default:
				secs, isNum := tc.Expect.(float64)
				if !isNum {
					t.Fatalf("fixture case has an unknown expect %#v", tc.Expect)
				}
				if want := time.Duration(secs) * time.Second; err != nil || !ok || d != want {
					t.Fatalf("%#v: got (%v, %v, %v), want (%v, true, nil)", tc.Value, d, ok, err, want)
				}
			}

			// The same value through the real CONFIG path: refused ones stop the worker
			// before it starts, everything else starts.
			cfgJSON, err := json.Marshal(map[string]interface{}{
				"topic": "cdc.shop.orders", "consumer_group": "g", "destination_connector": "gcs",
				"destination_config": map[string]interface{}{"bucket": "b", objectFlushIntervalKey: tc.Value},
				"metrics_port":       1,
			})
			if err != nil {
				t.Fatalf("marshal CONFIG: %v", err)
			}
			t.Setenv("CONFIG", string(cfgJSON))
			_, loadErr := loadConfig()
			if refused := tc.Expect == "refused"; refused != (loadErr != nil) {
				t.Fatalf("%#v: loadConfig error = %v, want refused=%v", tc.Value, loadErr, refused)
			}
		})
	}
}

func TestObjectFlushIntervalOverride_AcceptsWholeSecondsInRange(t *testing.T) {
	cases := []struct {
		name string
		raw  interface{}
		want time.Duration
	}{
		{"json number from the connector", float64(120), 120 * time.Second},
		{"string from the orchestrator", "45", 45 * time.Second},
		{"string with spaces", " 60 ", 60 * time.Second},
		{"lower bound", float64(1), time.Second},
		{"upper bound as a json number", float64(240), 240 * time.Second},
		{"upper bound as a string", "240", 240 * time.Second},
		{"int", 200, 200 * time.Second},
		{"int at the lower bound", 1, time.Second},
		{"int at the upper bound", 240, 240 * time.Second},
		{"int64", int64(15), 15 * time.Second},
		{"int64 at the upper bound", int64(240), 240 * time.Second},
		{"json.Number", json.Number("90"), 90 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, ok, err := objectFlushIntervalOverride(map[string]interface{}{"max_file_interval_seconds": tc.raw})
			if err != nil || !ok || d != tc.want {
				t.Fatalf("got (%v, %v, %v), want (%v, true, nil)", d, ok, err, tc.want)
			}
		})
	}
}

func TestObjectFlushIntervalOverride_UnsetKeepsDefault(t *testing.T) {
	for name, cfg := range map[string]map[string]interface{}{
		"nil config":             nil,
		"key absent":             {"bucket": "b"},
		"json null":              {"max_file_interval_seconds": nil},
		"empty string":           {"max_file_interval_seconds": ""},
		"orchestrator's null":    {"max_file_interval_seconds": "<nil>"},
		"literal null string":    {"max_file_interval_seconds": "null"},
		"upper-case null string": {"max_file_interval_seconds": "NULL"},
		"padded null string":     {"max_file_interval_seconds": " null "},
		"whitespace-only string": {"max_file_interval_seconds": "   "},
	} {
		t.Run(name, func(t *testing.T) {
			d, ok, err := objectFlushIntervalOverride(cfg)
			if err != nil || ok || d != 0 {
				t.Fatalf("got (%v, %v, %v), want (0, false, nil)", d, ok, err)
			}
		})
	}
}

func TestObjectFlushIntervalOverride_RefusesBadValuesWithAClearError(t *testing.T) {
	cases := map[string]interface{}{
		"zero":                     float64(0),
		"zero string":              "0",
		"negative":                 float64(-5),
		"negative string":          "-5",
		"above max":                float64(241),
		"above max string":         "241",
		"old 900 ceiling":          float64(900),
		"old 900 ceiling string":   "900",
		"fraction above max":       float64(240.5),
		"fraction below min":       float64(0.9999),
		"absurd":                   float64(1e12),
		"fraction":                 float64(30.5),
		"fraction string":          "30.5",
		"not a number":             "abc",
		"underscore":               "1_000",
		"underscore in range":      "1_0",
		"hex":                      "0x1E",
		"negative zero":            "-0",
		"lone plus":                "+",
		"lone minus":               "-",
		"bool":                     true,
		"list":                     []interface{}{float64(30)},
		"object":                   map[string]interface{}{"seconds": float64(30)},
		"int zero":                 0,
		"int above max":            241,
		"int negative":             -5,
		"int64 zero":               int64(0),
		"int64 above max":          int64(241),
		"json.Number above max":    json.Number("241"),
		"json.Number zero":         json.Number("0"),
		"json.Number with a point": json.Number("1.5"),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			d, ok, err := objectFlushIntervalOverride(map[string]interface{}{"max_file_interval_seconds": raw})
			if err == nil {
				t.Fatalf("value %#v was accepted as %v; it must be refused", raw, d)
			}
			if ok || d != 0 {
				t.Fatalf("a refused value must not also return an interval: (%v, %v)", d, ok)
			}
			msg := err.Error()
			for _, want := range []string{"max_file_interval_seconds", "whole number of seconds from 1 to 240", "30-second default"} {
				if !strings.Contains(msg, want) {
					t.Errorf("error %q does not say %q", msg, want)
				}
			}
		})
	}
}

func TestObjectFlushIntervalOverride_ErrorShowsTheBadValueBounded(t *testing.T) {
	_, _, err := objectFlushIntervalOverride(map[string]interface{}{"max_file_interval_seconds": "abc"})
	if err == nil || !strings.Contains(err.Error(), `(got "abc")`) {
		t.Fatalf("error does not show the value that was refused: %v", err)
	}
	_, _, err = objectFlushIntervalOverride(map[string]interface{}{"max_file_interval_seconds": strings.Repeat("x", 500)})
	if err == nil {
		t.Fatal("a 500-character value must be refused")
	}
	if len(err.Error()) > 200 {
		t.Fatalf("error echoes the whole value (%d chars); it must be truncated", len(err.Error()))
	}
}

func objectBatcherFor(t *testing.T, destCfg map[string]interface{}, batching *CDCBatchingConfig, rt http.RoundTripper) (*cdcObjectBatcher, *highWaterTracker) {
	t.Helper()
	cfg := &WorkerConfig{
		PipelineID:           "p-flush-interval",
		DestinationConnector: "gcs",
		DestinationVersion:   "v1.0.0",
		DestinationConfig:    destCfg,
	}
	if batching != nil {
		cfg.KafkaSinkWorker = &KafkaSinkWorkerConfig{CDCBatching: batching}
	}
	hw := newHighWaterTracker()
	b := newCDCObjectBatcher(cfg, "gcs", &kafka.Reader{}, hw, nil, &http.Client{Transport: rt},
		&kafka.Writer{}, nil, &Metrics{}, 0, &sync.Map{}, &sync.Map{}, &sync.Map{}, &sync.Map{})
	if b == nil {
		t.Fatal("newCDCObjectBatcher returned nil for a gcs destination")
	}
	return b, hw
}

func TestNewCDCObjectBatcher_FlushIntervalComesFromDestinationConfig(t *testing.T) {
	cases := []struct {
		name     string
		destCfg  map[string]interface{}
		batching *CDCBatchingConfig
		want     time.Duration
	}{
		{"unset keeps the 30s default", map[string]interface{}{"bucket": "b"}, nil, 30 * time.Second},
		{"json number", map[string]interface{}{"bucket": "b", "max_file_interval_seconds": float64(120)}, nil, 120 * time.Second},
		{"string", map[string]interface{}{"bucket": "b", "max_file_interval_seconds": "45"}, nil, 45 * time.Second},
		{"orchestrator null keeps the default", map[string]interface{}{"bucket": "b", "max_file_interval_seconds": "<nil>"}, nil, 30 * time.Second},
		{"destination setting wins over cdc_batching", map[string]interface{}{"max_file_interval_seconds": "120"}, &CDCBatchingConfig{FlushIntervalSeconds: 10}, 120 * time.Second},
		{"cdc_batching still applies when the destination is silent", map[string]interface{}{}, &CDCBatchingConfig{FlushIntervalSeconds: 10}, 10 * time.Second},
		{"file-roll caps do not disturb the interval", map[string]interface{}{"max_file_rows": float64(50), "max_file_interval_seconds": float64(200)}, nil, 200 * time.Second},
		// loadConfig refuses these first; a batcher built without that check must still
		// run on the interval it would have had, not on some other value.
		{"invalid value keeps the 30s default", map[string]interface{}{"bucket": "b", "max_file_interval_seconds": "abc"}, nil, 30 * time.Second},
		{"invalid value keeps the cdc_batching interval", map[string]interface{}{"max_file_interval_seconds": float64(0)}, &CDCBatchingConfig{FlushIntervalSeconds: 10}, 10 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := objectBatcherFor(t, tc.destCfg, tc.batching, nil)
			if b.params.flushInterval != tc.want {
				t.Fatalf("flushInterval = %v, want %v", b.params.flushInterval, tc.want)
			}
		})
	}
}

// recordingDest is a fake destination MCP server. At the moment each import_data call
// arrives, it records whether the batch's last offset had already been marked written:
// the only safe answer is no.
type recordingDest struct {
	mu            sync.Mutex
	hw            *highWaterTracker
	watchTopic    string
	watchPart     int
	watchOffset   int64
	importKeys    []string
	markedEarlier bool
}

func (r *recordingDest) RoundTrip(req *http.Request) (*http.Response, error) {
	var body struct {
		Params struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
		} `json:"params"`
	}
	raw, _ := io.ReadAll(req.Body)
	_ = json.Unmarshal(raw, &body)
	r.mu.Lock()
	if strings.HasSuffix(body.Params.Name, "_import_data") {
		key, _ := body.Params.Arguments["key"].(string)
		r.importKeys = append(r.importKeys, key)
		if r.hw.seen(r.watchTopic, r.watchPart, r.watchOffset) {
			r.markedEarlier = true
		}
	}
	r.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":{"success":true}}`)),
		Request:    req,
	}, nil
}

func (r *recordingDest) imports() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.importKeys...)
}

func cdcTestMessages() ([]kafka.Message, []*SinkMessage) {
	msgs := []kafka.Message{
		{Topic: "cdc.shop.orders", Partition: 2, Offset: 70},
		{Topic: "cdc.shop.orders", Partition: 2, Offset: 71},
	}
	sms := []*SinkMessage{
		{PipelineID: "p-flush-interval", Table: "shop.orders", CDCOp: "c", SourceTS: 1_700_000_000_000,
			PK: map[string]interface{}{"id": 1}, After: map[string]interface{}{"id": 1}},
		{PipelineID: "p-flush-interval", Table: "shop.orders", CDCOp: "u", SourceTS: 1_700_000_000_500,
			PK: map[string]interface{}{"id": 2}, After: map[string]interface{}{"id": 2}},
	}
	return msgs, sms
}

func onlyBatch(t *testing.T, b *cdcObjectBatcher) *cdcObjectBatch {
	t.Helper()
	if len(b.batches) != 1 {
		t.Fatalf("expected exactly one open batch, have %d", len(b.batches))
	}
	for _, batch := range b.batches {
		return batch
	}
	return nil
}

func TestObjectBatcher_TimerUsesConfiguredIntervalAndCommitsOnlyAfterWrite(t *testing.T) {
	dest := &recordingDest{watchTopic: "cdc.shop.orders", watchPart: 2, watchOffset: 71}
	destCfg := map[string]interface{}{"bucket": "b", "file_format": "jsonl", "max_file_interval_seconds": "120"}
	b, hw := objectBatcherFor(t, destCfg, nil, dest)
	dest.hw = hw

	msgs, sms := cdcTestMessages()
	ctx := t.Context()
	for i := range msgs {
		b.add(ctx, msgs[i], sms[i])
	}
	created := onlyBatch(t, b).createdAt

	// At the old hardcoded 30s, and just short of the configured 120s, nothing is due.
	for _, age := range []time.Duration{30 * time.Second, 119 * time.Second} {
		b.flushDue(ctx, created.Add(age))
		if got := dest.imports(); len(got) != 0 {
			t.Fatalf("flushed at %v with a 120s interval: %v", age, got)
		}
		if hw.seen("cdc.shop.orders", 2, 70) {
			t.Fatalf("offsets marked written at %v although nothing was written", age)
		}
		onlyBatch(t, b)
	}

	b.flushDue(ctx, created.Add(120*time.Second))
	got := dest.imports()
	if len(got) != 1 {
		t.Fatalf("expected one file written at 120s, got %d: %v", len(got), got)
	}
	if dest.markedEarlier {
		t.Fatal("offsets were marked written before the destination write was made")
	}
	if !hw.seen("cdc.shop.orders", 2, 71) {
		t.Fatal("after a successful write the batch's last offset must be marked written")
	}
	if len(b.batches) != 0 {
		t.Fatalf("the written batch is still held: %d open", len(b.batches))
	}
}

func TestObjectBatcher_FileNameDoesNotDependOnFlushTiming(t *testing.T) {
	// A restart redelivers the same records, possibly under a different interval. The
	// file they produce must land on the same key so the rewrite replaces it instead of
	// duplicating it.
	keyFor := func(interval string, age time.Duration) string {
		dest := &recordingDest{}
		b, hw := objectBatcherFor(t, map[string]interface{}{"bucket": "b", "file_format": "jsonl", "max_file_interval_seconds": interval}, nil, dest)
		dest.hw = hw
		msgs, sms := cdcTestMessages()
		for i := range msgs {
			b.add(t.Context(), msgs[i], sms[i])
		}
		b.flushDue(t.Context(), onlyBatch(t, b).createdAt.Add(age))
		got := dest.imports()
		if len(got) != 1 || got[0] == "" {
			t.Fatalf("interval %s: expected one named file, got %v", interval, got)
		}
		return got[0]
	}
	first := keyFor("5", 5*time.Second)
	second := keyFor("240", 4*time.Minute)
	if first != second {
		t.Fatalf("the same records produced different file names:\n  %s\n  %s", first, second)
	}
	if !strings.Contains(first, "-p2") || !strings.Contains(first, "70") {
		t.Fatalf("file name %q does not carry the partition and first offset", first)
	}
}

func TestObjectBatcher_ShutdownDrainWritesBatchHeldForLongestInterval(t *testing.T) {
	dest := &recordingDest{watchTopic: "cdc.shop.orders", watchPart: 2, watchOffset: 70}
	max := map[string]interface{}{"bucket": "b", "file_format": "jsonl", "max_file_interval_seconds": float64(maxObjectFlushIntervalSeconds)}
	b, hw := objectBatcherFor(t, max, nil, dest)
	dest.hw = hw
	msgs, sms := cdcTestMessages()
	b.add(t.Context(), msgs[0], sms[0])

	// The worker is told to stop the instant the batch opens: the youngest batch the
	// drain can meet, under the longest interval. It must still be written, and only
	// then marked done.
	b.drainForShutdown(t.Context(), onlyBatch(t, b).createdAt)
	if got := dest.imports(); len(got) != 1 {
		t.Fatalf("the shutdown drain skipped a batch held at the longest allowed interval (%ds): %v", maxObjectFlushIntervalSeconds, got)
	}
	if dest.markedEarlier || !hw.seen("cdc.shop.orders", 2, 70) {
		t.Fatalf("drained batch offsets: marked before write=%v, marked after=%v", dest.markedEarlier, hw.seen("cdc.shop.orders", 2, 70))
	}
	if len(b.batches) != 0 {
		t.Fatalf("the drained batch is still held: %d open", len(b.batches))
	}
}

func TestStallWindowCoveringFlush(t *testing.T) {
	cases := []struct {
		name          string
		stall, flush  time.Duration
		want          time.Duration
		mustOutlastIt bool
	}{
		{"default interval keeps the default 60s window", 60 * time.Second, 30 * time.Second, 60 * time.Second, true},
		{"longer interval widens the window", 60 * time.Second, 120 * time.Second, 150 * time.Second, true},
		{"longest interval", 60 * time.Second, 240 * time.Second, 270 * time.Second, true},
		{"operator's short window is widened", 30 * time.Second, 30 * time.Second, 60 * time.Second, true},
		{"operator's wide window is kept", 3600 * time.Second, 240 * time.Second, 3600 * time.Second, true},
		{"disabled watchdog stays disabled", 0, 120 * time.Second, 0, false},
		{"no interval leaves the window alone", 60 * time.Second, 0, 60 * time.Second, false},
		{"no interval leaves even a short window alone", 10 * time.Second, 0, 10 * time.Second, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stallWindowCoveringFlush(tc.stall, tc.flush)
			if got != tc.want {
				t.Fatalf("stallWindowCoveringFlush(%v, %v) = %v, want %v", tc.stall, tc.flush, got, tc.want)
			}
			if !tc.mustOutlastIt {
				return
			}
			assertNoRestartBeforeFlush(t, got, tc.flush)
		})
	}
}

// assertNoRestartBeforeFlush checks window against the real restart decision: a quiet
// topic whose last record arrived just before a batch of age flush is due must not be
// restarted.
func assertNoRestartBeforeFlush(t *testing.T, window, flush time.Duration) {
	t.Helper()
	last := time.Unix(1_700_000_000, 0)
	snap := stallSnapshot{now: last.Add(flush + consumeLoopAliveWindow - time.Second), lastPoll: last.Add(flush + consumeLoopAliveWindow - 2*time.Second), lastMessage: last, dataWaiting: true}
	if shouldRestartForStall(snap, window) {
		t.Fatalf("with window %v the watchdog restarts the worker while a %v batch is still being written", window, flush)
	}
}

func TestConsumerStallWindow(t *testing.T) {
	b120, _ := objectBatcherFor(t, map[string]interface{}{"bucket": "b", "max_file_interval_seconds": "120"}, nil, nil)
	bDefault, _ := objectBatcherFor(t, map[string]interface{}{"bucket": "b"}, nil, nil)
	bMax, _ := objectBatcherFor(t, map[string]interface{}{"bucket": "b", "max_file_interval_seconds": float64(maxObjectFlushIntervalSeconds)}, nil, nil)

	cases := []struct {
		name    string
		env     string
		batcher *cdcObjectBatcher
		flush   time.Duration
		want    time.Duration
	}{
		{"not an object-storage CDC worker: the operator's window as is", "", nil, 0, 60 * time.Second},
		{"not an object-storage CDC worker: a set window as is", "45", nil, 0, 45 * time.Second},
		{"30s default interval: the 60s default window", "", bDefault, 30 * time.Second, 60 * time.Second},
		{"120s interval widens the default window to 150s", "", b120, 120 * time.Second, 150 * time.Second},
		{"longest interval widens it to 270s", "", bMax, 240 * time.Second, 270 * time.Second},
		{"120s interval widens a short set window", "45", b120, 120 * time.Second, 150 * time.Second},
		{"a wider set window is kept", "600", b120, 120 * time.Second, 600 * time.Second},
		{"a disabled watchdog stays disabled", "0", b120, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("RSYNC_SINK_STALL_WATCHDOG_SECONDS", tc.env)
			got := consumerStallWindow(tc.batcher)
			if got != tc.want {
				t.Fatalf("consumerStallWindow = %v, want %v", got, tc.want)
			}
			if tc.flush > 0 {
				assertNoRestartBeforeFlush(t, got, tc.flush)
			}
		})
	}
}

func TestLoadConfig_RefusesBadFlushIntervalBeforeStarting(t *testing.T) {
	base := func(dest string, destCfg map[string]interface{}) string {
		raw, _ := json.Marshal(map[string]interface{}{
			"topic": "cdc.shop.orders", "consumer_group": "g", "destination_connector": dest,
			"destination_config": destCfg, "metrics_port": 1,
		})
		return string(raw)
	}

	t.Setenv("CONFIG", base("gcs", map[string]interface{}{"bucket": "b", "max_file_interval_seconds": 200}))
	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("a valid interval was refused: %v", err)
	}
	b, _ := objectBatcherFor(t, cfg.DestinationConfig, nil, nil)
	if b.params.flushInterval != 200*time.Second {
		t.Fatalf("interval did not survive the CONFIG round trip: %v", b.params.flushInterval)
	}

	t.Setenv("CONFIG", base("gcs", map[string]interface{}{"bucket": "b"}))
	if _, err := loadConfig(); err != nil {
		t.Fatalf("an unset interval was refused: %v", err)
	}

	// Refused for every destination, including relational ones that do not use it, so
	// a typo is caught wherever it is made.
	for _, tc := range []struct {
		dest string
		raw  interface{}
	}{{"gcs", 0}, {"gcs", 241}, {"aws-s3", -1}, {"azure-blob", "9999"}, {"postgresql", "abc"}} {
		t.Setenv("CONFIG", base(tc.dest, map[string]interface{}{"max_file_interval_seconds": tc.raw}))
		cfg, err := loadConfig()
		if err == nil {
			t.Fatalf("%s with %v started with interval config %v", tc.dest, tc.raw, cfg.DestinationConfig)
		}
		if !strings.Contains(err.Error(), "max_file_interval_seconds must be a whole number of seconds from 1 to 240") {
			t.Fatalf("%s with %v: unclear error %q", tc.dest, tc.raw, err)
		}
	}
}
