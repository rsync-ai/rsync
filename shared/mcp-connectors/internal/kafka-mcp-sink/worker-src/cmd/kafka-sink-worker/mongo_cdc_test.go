package main

import "testing"

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
