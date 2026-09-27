package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSourceFamily(t *testing.T) {
	mongo := map[string]interface{}{"source": map[string]interface{}{"connector": "mongodb", "db": "app"}}
	if got := sourceFamily(mongo); got != "mongodb" {
		t.Errorf("sourceFamily(mongo)=%q, want mongodb", got)
	}
	pg := map[string]interface{}{"source": map[string]interface{}{"connector": "PostgreSQL"}}
	if got := sourceFamily(pg); got != "postgresql" {
		t.Errorf("sourceFamily(pg)=%q, want postgresql (lowercased)", got)
	}
	if got := sourceFamily(map[string]interface{}{}); got != "" {
		t.Errorf("sourceFamily(no source)=%q, want empty", got)
	}
}

func TestNormalizeMongoID(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
	}{
		{"objectid-map", map[string]interface{}{"$oid": "5f1a2b3c4d5e6f7a8b9c0d1e"}, "5f1a2b3c4d5e6f7a8b9c0d1e"},
		{"objectid-string", `{"$oid":"5f1a2b3c4d5e6f7a8b9c0d1e"}`, "5f1a2b3c4d5e6f7a8b9c0d1e"},
		{"numberlong-map", map[string]interface{}{"$numberLong": "9007199254740993"}, "9007199254740993"},
		{"quoted-string", `"user-42"`, "user-42"},
		{"plain-string", "user-42", "user-42"},
		{"empty", "", ""},
		{"nil", nil, ""},
	}
	for _, tc := range cases {
		if got := normalizeMongoID(tc.in); got != tc.want {
			t.Errorf("normalizeMongoID(%s)=%q, want %q", tc.name, got, tc.want)
		}
	}
}

// The core silent-drop guard: a Mongo create whose 'after' is a JSON *string*
// must land a packed row (_id + document), not RowCount=0.
func TestDecodeMongoDocument_Create(t *testing.T) {
	cfg := &WorkerConfig{DestinationConnector: "postgresql"}
	sm := &SinkMessage{PK: map[string]interface{}{"id": `{"$oid":"5f1a2b3c4d5e6f7a8b9c0d1e"}`}}
	payload := map[string]interface{}{
		"after": `{"_id":{"$oid":"5f1a2b3c4d5e6f7a8b9c0d1e"},"name":"alice","age":30}`,
	}
	if err := decodeMongoDocument(cfg, sm, payload, "c"); err != nil {
		t.Fatalf("decodeMongoDocument(create) errored: %v", err)
	}
	if sm.RowCount != 1 || len(sm.Data) != 1 {
		t.Fatalf("RowCount=%d len(Data)=%d, want 1/1 (silent-drop guard)", sm.RowCount, len(sm.Data))
	}
	row := sm.Data[0]
	if row["_id"] != "5f1a2b3c4d5e6f7a8b9c0d1e" {
		t.Errorf("_id=%v, want normalized ObjectId hex", row["_id"])
	}
	doc, ok := row["document"].(map[string]interface{})
	if !ok {
		t.Fatalf("document column is %T, want map (lossless doc)", row["document"])
	}
	if doc["name"] != "alice" {
		t.Errorf("document.name=%v, want alice", doc["name"])
	}
	if len(sm.KeyFields) != 1 || sm.KeyFields[0] != "_id" {
		t.Errorf("KeyFields=%v, want [_id] (packed PK, not Debezium key field 'id')", sm.KeyFields)
	}
	if !sm.ColumnTypesAreDDL || sm.ColumnTypes["_id"] != "TEXT" || sm.ColumnTypes["document"] != "JSONB" {
		t.Errorf("ColumnTypes=%v DDL=%v, want fixed TEXT/JSONB packed DDL", sm.ColumnTypes, sm.ColumnTypesAreDDL)
	}
}

// Delete carries only the key (no before-image unless pre-images enabled): the
// _id must come from the key so the destination can remove the row.
func TestDecodeMongoDocument_DeleteFromKey(t *testing.T) {
	cfg := &WorkerConfig{DestinationConnector: "postgresql"}
	sm := &SinkMessage{PK: map[string]interface{}{"id": `{"$oid":"5f1a2b3c4d5e6f7a8b9c0d1e"}`}}
	payload := map[string]interface{}{"before": nil}
	if err := decodeMongoDocument(cfg, sm, payload, "d"); err != nil {
		t.Fatalf("decodeMongoDocument(delete) errored: %v", err)
	}
	if sm.RowCount != 1 || sm.Data[0]["_id"] != "5f1a2b3c4d5e6f7a8b9c0d1e" {
		t.Errorf("delete row=%v RowCount=%d, want _id from key with RowCount 1", sm.Data, sm.RowCount)
	}
}

// MySQL cannot key on TEXT without a prefix length, so the packed PK must be a
// bounded VARCHAR and the document column JSON (not JSONB).
func TestDecodeMongoDocument_MySQLDDL(t *testing.T) {
	cfg := &WorkerConfig{DestinationConnector: "mysql"}
	sm := &SinkMessage{PK: map[string]interface{}{"id": "1001"}}
	payload := map[string]interface{}{"after": `{"_id":1001,"name":"bob"}`}
	if err := decodeMongoDocument(cfg, sm, payload, "c"); err != nil {
		t.Fatalf("decodeMongoDocument(mysql) errored: %v", err)
	}
	if sm.ColumnTypes["_id"] != "VARCHAR(255)" || sm.ColumnTypes["document"] != "JSON" {
		t.Errorf("mysql ColumnTypes=%v, want VARCHAR(255)/JSON", sm.ColumnTypes)
	}
}

// Fail loud: a create with no 'after' document, or a corrupt JSON string, must
// return an error (dead-letter) rather than a silent zero-row no-op.
func TestDecodeMongoDocument_FailLoud(t *testing.T) {
	cfg := &WorkerConfig{DestinationConnector: "postgresql"}

	sm := &SinkMessage{PK: map[string]interface{}{"id": `{"$oid":"5f1a"}`}}
	if err := decodeMongoDocument(cfg, sm, map[string]interface{}{}, "c"); err == nil {
		t.Error("create with no after document should fail loud, got nil error")
	}

	sm2 := &SinkMessage{PK: map[string]interface{}{"id": `{"$oid":"5f1a"}`}}
	if err := decodeMongoDocument(cfg, sm2, map[string]interface{}{"after": `{not valid json`}, "c"); err == nil {
		t.Error("create with corrupt after JSON should fail loud, got nil error")
	}
}

// TestNormalizeMongoIDNumericKeysAreLiteral locks the primary-key contract for
// NUMERIC MongoDB _id values. A key is an identity, not a measurement: it must be
// the same characters the source holds, at every magnitude.
//
// TestNormalizeMongoID above already covers _id shapes, but every numeric case it
// has arrives as {"$numberLong": "..."} — an Extended-JSON *string*, which takes
// the correct branch. A bare JSON number does not, and that is the gap this fills:
// a bare number decodes to float64, which reached fmt.Sprint's %g and flipped to
// exponent form once the decimal exponent hit 6. An _id of 9000001 was written to
// pk_json, and to the destination's _id primary-key column, as "9.000001e+06"
// (observed in prod 2026-09-23), while 20931 was untouched — so the corruption
// began at exactly 1e6 and nothing below it ever showed a symptom.
func TestNormalizeMongoIDNumericKeysAreLiteral(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
	}{
		// The boundary: %g flips to exponent form at 1e6, so these two sat on
		// opposite sides of the bug with nothing to distinguish them.
		{"float below the 1e6 threshold", float64(999999), "999999"},
		{"float at the 1e6 threshold", float64(1000000), "1000000"},
		{"float above the threshold", float64(9000001), "9000001"},
		{"float with many significant digits", float64(12345678), "12345678"},
		{"small float unaffected", float64(20931), "20931"},

		// json.Number is the exact decimal literal off the wire; never reformat it.
		{"json.Number integer", json.Number("9000001"), "9000001"},
		{"json.Number beyond 2^53", json.Number("9007199254740993"), "9007199254740993"},

		// int64 is what normalizeJSONNumbers yields for an exact integer.
		{"int64 stays exact", int64(9007199254740993), "9007199254740993"},

		// A genuine non-integral value is a real double; keep it readable.
		{"non-integral float", float64(1.5), "1.5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMongoID(tc.in); got != tc.want {
				t.Errorf("normalizeMongoID(%v)=%q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestDecodeMongoDocumentKeepsNumericIDExact drives the whole decode path the way
// a Debezium change event arrives, because the bug was not in the formatter alone:
// decodeMongoDocument prefers the DOCUMENT's own _id over the message key, and
// parsed that document with a plain json.Unmarshal, which forces every JSON number
// through float64. The message key was always correct, so the sink shipped a wrong
// key while holding the right one — which is why a key-only test could not see it.
func TestDecodeMongoDocumentKeepsNumericIDExact(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"numeric id above 1e6", `{"_id":9000001,"body":"probe"}`, "9000001"},
		{"numeric id at the threshold", `{"_id":1000000,"body":"probe"}`, "1000000"},
		{"numeric id below the threshold", `{"_id":20931,"body":"probe"}`, "20931"},
		// float64 cannot hold this exactly; UseNumber parses the literal instead.
		{"numeric id beyond 2^53", `{"_id":9007199254740993,"body":"probe"}`, "9007199254740993"},
		{"objectid id still works", `{"_id":{"$oid":"6ab36853017b1d5358ac4512"},"body":"probe"}`, "6ab36853017b1d5358ac4512"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &WorkerConfig{DestinationConnector: "postgresql"}
			// A deliberately wrong message key: the document's _id must win, and
			// must win in its exact form.
			sm := &SinkMessage{PK: map[string]interface{}{"id": "key-not-used"}}
			if err := decodeMongoDocument(cfg, sm, map[string]interface{}{"after": tc.doc}, "c"); err != nil {
				t.Fatalf("decodeMongoDocument: %v", err)
			}

			// The key written to pk_json and to the destination _id column.
			if got, _ := sm.PK["_id"].(string); got != tc.want {
				t.Errorf("sm.PK[_id]=%q, want %q", got, tc.want)
			}
			if got, _ := sm.After["_id"].(string); got != tc.want {
				t.Errorf("sm.After[_id]=%v, want %q", sm.After["_id"], tc.want)
			}

			// The packed document column must round-trip the _id too: float64
			// silently rewrote 9007199254740993 as ...992 inside the payload,
			// where no exponent notation made it visible.
			b, err := json.Marshal(sm.After["document"])
			if err != nil {
				t.Fatalf("marshal document: %v", err)
			}
			if !strings.Contains(string(b), tc.want) {
				t.Errorf("document column %s lost the exact _id %q", b, tc.want)
			}
		})
	}
}
