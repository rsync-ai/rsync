package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

// KI-CDC-SINK-GUESSED-KEY-COLLAPSES-ROWS. Bug class: "the sink upserts on a key
// nobody declared". A row's `id` or lone `*_id` column names a column; it does not
// make it unique. A key may come only from the source (the Kafka message key,
// sm.KeyFields) or from the destination config (key_fields & co.).

// callLog is a destination that accepts every write and records what the sink
// asked for: tool, key_fields / primary_keys and synthetic_pk.
type callLog struct {
	mu    sync.Mutex
	calls []loggedCall
}

type loggedCall struct {
	tool        string
	keyFields   []string
	primaryKeys []string
	syntheticPK bool
}

func (l *callLog) RoundTrip(r *http.Request) (*http.Response, error) {
	var req struct {
		Params struct {
			Name      string `json:"name"`
			Arguments struct {
				Data        []map[string]interface{} `json:"data"`
				KeyFields   []string                 `json:"key_fields"`
				PrimaryKeys []string                 `json:"primary_keys"`
				SyntheticPK bool                     `json:"synthetic_pk"`
			} `json:"arguments"`
		} `json:"params"`
	}
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &req)
	a := req.Params.Arguments
	l.mu.Lock()
	l.calls = append(l.calls, loggedCall{req.Params.Name, a.KeyFields, a.PrimaryKeys, a.SyntheticPK})
	l.mu.Unlock()
	n := len(a.Data)
	result := map[string]interface{}{"success": true, "rows_inserted": n, "rows_upserted": n, "rows_merged": n}
	body, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": result})
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
}

func (l *callLog) writes() []loggedCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []loggedCall
	for _, c := range l.calls {
		if strings.HasSuffix(c.tool, "_import_data") || strings.HasSuffix(c.tool, "_upsert_data") ||
			strings.HasSuffix(c.tool, "_merge") || strings.HasSuffix(c.tool, "_load") {
			out = append(out, c)
		}
	}
	return out
}

// Rows that share a value in the column a guess would pick. Both shapes the
// guess knew: an `id` column, and exactly one `*_id` column.
var guessableKeylessTables = map[string][]map[string]interface{}{
	"lone *_id column": {{"user_id": 1, "note": "a"}, {"user_id": 1, "note": "b"}, {"user_id": 1, "note": "c"}},
	"id column":        {{"id": 1, "note": "a"}, {"id": 1, "note": "b"}, {"id": 1, "note": "c"}},
}

// On MongoDB a guessed key collapses the rows into one document per value.
func TestKeylessCDCRow_IsNeverUpsertedOnAGuessedKey_EveryWritePath(t *testing.T) {
	for shape, rows := range guessableKeylessTables {
		for _, p := range keylessWritePaths {
			t.Run(shape+"/"+p.name, func(t *testing.T) {
				captureFailClosed(t)
				store := &docStore{}
				sms, msgs := keylessMessages(rows)
				p.deliver(t, "mongodb", nil, store, sms, msgs)
				if got := store.count(); got != 3 {
					t.Errorf("3 distinct keyless changes left %d documents, want 3 — upserted on a guessed key (tools %v keys %v)", got, store.tools, store.keys)
				}
				for i, k := range store.keys {
					for _, f := range k {
						if f == "user_id" || f == "id" {
							t.Errorf("call %d (%s) keyed on %v — a column nobody declared a key", i, store.tools[i], k)
						}
					}
				}
			})
		}
	}
}

// On a relational destination a keyless table is written on the synthetic row
// hash (synthetic_pk), never on a guessed column, on every lane.
func TestKeylessCDCRow_RelationalDestinationUsesTheRowHashNotAGuess_EveryWritePath(t *testing.T) {
	for shape, rows := range guessableKeylessTables {
		for _, p := range keylessWritePaths[:3] { // append mode is a plain INSERT, no key
			t.Run(shape+"/"+p.name, func(t *testing.T) {
				captureFailClosed(t)
				log := &callLog{}
				sms, msgs := keylessMessages(rows)
				p.deliver(t, "postgresql", nil, log, sms, msgs)
				w := log.writes()
				if len(w) == 0 {
					t.Fatal("no write reached the destination")
				}
				for _, c := range w {
					if len(c.keyFields) > 0 {
						t.Errorf("%s keyed on %v — a column nobody declared a key", c.tool, c.keyFields)
					}
					if !strings.HasSuffix(c.tool, "_upsert_data") || !c.syntheticPK {
						t.Errorf("%s synthetic_pk=%v — a keyless upsert must ask for the row hash (else the connector falls back to `id`)", c.tool, c.syntheticPK)
					}
				}
			})
		}
	}
}

// A warehouse merge takes a declared key from the config or the source; with
// neither it refuses rather than merging on `id`.
func TestKeylessCDCRow_WarehouseMergeNeverGuessesTheKey(t *testing.T) {
	sms, msgs := keylessMessages(guessableKeylessTables["id column"])
	cfg := &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc", DestinationConnector: "bigquery", DestinationConfig: map[string]interface{}{}}
	log := &callLog{}
	_, _, err := writeCDCToDestination(context.Background(), &http.Client{Transport: log}, cfg,
		&DDLSupport{Enabled: false, resolved: true}, msgs[0], sms[0], "upsert")
	if err == nil || !strings.Contains(err.Error(), "primary_keys") {
		t.Errorf("keyless warehouse merge: err=%v, calls=%+v — want a missing-primary-keys refusal, not a merge on a guessed `id`", err, log.writes())
	}

	// The source's declared key (Debezium message key) is honoured.
	sms, msgs = keylessMessages([]map[string]interface{}{{"id": 1, "order_id": 7, "note": "a"}})
	sms[0].KeyFields = []string{"order_id"}
	log = &callLog{}
	if _, _, err := writeCDCToDestination(context.Background(), &http.Client{Transport: log}, cfg,
		&DDLSupport{Enabled: false, resolved: true}, msgs[0], sms[0], "upsert"); err != nil {
		t.Fatalf("keyed warehouse merge: %v", err)
	}
	w := log.writes()
	if len(w) != 1 || strings.Join(w[0].primaryKeys, ",") != "order_id" {
		t.Errorf("keyed warehouse merge sent %+v, want one merge on primary_keys [order_id] (the source key, not `id`)", w)
	}
}

// Object storage: the bronze `pk` of a keyless row is empty, not a guessed column.
func TestKeylessCDCRow_BronzePKIsNotGuessed(t *testing.T) {
	sm := &SinkMessage{CDCOp: "c", After: map[string]interface{}{"id": 1, "note": "a"}}
	if pk := pkObjectForCDC(sm, "upsert"); pk != nil {
		t.Errorf("keyless row got bronze pk %v — a guessed key", pk)
	}
	sm.KeyFields = []string{"id"}
	if pk := pkObjectForCDC(sm, "upsert"); pk["id"] != 1 {
		t.Errorf("declared key: pk = %v, want {id:1}", pk)
	}
}

// Positive control: a key the destination config or the source declares IS an
// upsert key, so the fix above cannot pass by never keying anything.
func TestDeclaredCDCKey_IsStillTheUpsertKey_EveryWritePath(t *testing.T) {
	rows := guessableKeylessTables["lone *_id column"]
	declare := map[string]func(sms []*SinkMessage) map[string]interface{}{
		"destination config key_fields": func([]*SinkMessage) map[string]interface{} {
			return map[string]interface{}{"key_fields": []interface{}{"user_id"}}
		},
		"source message key": func(sms []*SinkMessage) map[string]interface{} {
			for _, sm := range sms {
				sm.KeyFields = []string{"user_id"}
			}
			return nil
		},
	}
	for how, set := range declare {
		for _, p := range keylessWritePaths[:3] {
			t.Run(how+"/"+p.name, func(t *testing.T) {
				captureFailClosed(t)
				store := &docStore{}
				sms, msgs := keylessMessages(rows)
				destCfg := set(sms)
				p.deliver(t, "mongodb", destCfg, store, sms, msgs)
				if got := store.count(); got != 1 {
					t.Errorf("3 changes to one declared key left %d documents, want 1 (keys %v)", got, store.keys)
				}
			})
		}
	}
}
