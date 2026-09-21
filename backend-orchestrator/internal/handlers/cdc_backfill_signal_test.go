package handlers

import (
	"encoding/json"
	"reflect"
	"testing"
)

// The Kafka signal channel is the only backfill path for a connector started
// with snapshot.mode=no_data, and Debezium ignores a signal it cannot parse —
// a wrong shape means "history never arrives" with no error anywhere. Pin it.
func TestBuildExecuteSnapshotSignalShape(t *testing.T) {
	raw, err := buildExecuteSnapshotSignal("incremental", []string{"public.orders", "public.items"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	var got struct {
		Type string `json:"type"`
		Data struct {
			Type        string   `json:"type"`
			Collections []string `json:"data-collections"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal %s: %v", raw, err)
	}
	if got.Type != "execute-snapshot" {
		t.Fatalf("signal type = %q, want execute-snapshot", got.Type)
	}
	if got.Data.Type != "INCREMENTAL" {
		t.Fatalf("snapshot type = %q, want INCREMENTAL", got.Data.Type)
	}
	if !reflect.DeepEqual(got.Data.Collections, []string{"public.orders", "public.items"}) {
		t.Fatalf("data-collections = %v", got.Data.Collections)
	}
}

func TestBuildExecuteSnapshotSignalModes(t *testing.T) {
	for mode, want := range map[string]string{
		"incremental": "INCREMENTAL",
		"blocking":    "BLOCKING",
		"BLOCKING":    "BLOCKING",
		" blocking ":  "BLOCKING",
		"":            "INCREMENTAL",
	} {
		raw, err := buildExecuteSnapshotSignal(mode, []string{"public.orders"})
		if err != nil {
			t.Fatalf("mode %q: %v", mode, err)
		}
		var got struct {
			Data struct {
				Type string `json:"type"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("mode %q: unmarshal: %v", mode, err)
		}
		if got.Data.Type != want {
			t.Fatalf("mode %q gave snapshot type %q, want %q", mode, got.Data.Type, want)
		}
	}

	if _, err := buildExecuteSnapshotSignal("incremental", nil); err == nil {
		t.Fatal("a signal with no tables would snapshot nothing; expected an error")
	}
}

// fmt.Sprint(nil) is "<nil>", so a naive read makes an ABSENT signal topic look
// configured — and the backfill would produce to a topic named "<nil>" instead
// of falling through to the source-signal-table path.
func TestConnCfgStringTreatsMissingKeysAsEmpty(t *testing.T) {
	cfg := map[string]interface{}{
		"signal.kafka.topic": "  signals.abc123  ",
		"topic.prefix":       nil,
		"read.only":          true,
	}
	if got := connCfgString(cfg, "signal.kafka.topic"); got != "signals.abc123" {
		t.Fatalf("got %q, want the trimmed topic", got)
	}
	if got := connCfgString(cfg, "topic.prefix"); got != "" {
		t.Fatalf("a nil value read as %q, want empty", got)
	}
	if got := connCfgString(cfg, "signal.enabled.channels"); got != "" {
		t.Fatalf("a missing key read as %q, want empty", got)
	}
	if got := connCfgString(cfg, "read.only"); got != "true" {
		t.Fatalf("got %q, want true", got)
	}
}

// An unqualified table name must be qualified with the namespace its own engine
// uses — schema for PostgreSQL, database for MySQL — or Debezium matches no
// collection and the snapshot is a no-op.
func TestPKNamespaceForPicksTheEnginesNamespace(t *testing.T) {
	if got := pkNamespaceFor("postgresql", "appdb", "sales"); got != "sales" {
		t.Fatalf("postgresql namespace = %q, want the schema", got)
	}
	if got := pkNamespaceFor("mysql", "appdb", "sales"); got != "appdb" {
		t.Fatalf("mysql namespace = %q, want the database", got)
	}
	// An engine with no registered provider still has to produce something
	// usable rather than an empty prefix.
	if got := pkNamespaceFor("nonesuch", "appdb", "sales"); got != "sales" {
		t.Fatalf("unknown engine namespace = %q, want the schema fallback", got)
	}
	if got := pkNamespaceFor("nonesuch", "appdb", ""); got != "appdb" {
		t.Fatalf("unknown engine namespace = %q, want the database fallback", got)
	}
}
