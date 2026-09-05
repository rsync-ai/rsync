package main

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

// vsdField builds the Kafka Connect schema entry for a Debezium
// io.debezium.data.VariableScaleDecimal column. Unlike a fixed-scale
// org.apache.kafka.connect.data.Decimal (type "bytes", scale in schema
// parameters), a VariableScaleDecimal is a STRUCT whose per-value payload
// carries {"scale": <int>, "value": "<base64 two's-complement>"} — the scale
// is NOT in the schema parameters (see mapConnectFieldToDDLType's precision()
// comment: "Returns 0,false for VariableScaleDecimal"). Debezium emits this for
// Oracle unscaled NUMBER (typical integer PK) and Postgres unbounded NUMERIC.
func vsdField(name string) map[string]interface{} {
	return map[string]interface{}{
		"field":    name,
		"type":     "struct",
		"optional": false,
		"name":     "io.debezium.data.VariableScaleDecimal",
		"version":  1,
		"fields": []interface{}{
			map[string]interface{}{"field": "scale", "type": "int32", "optional": false},
			map[string]interface{}{"field": "value", "type": "bytes", "optional": false},
		},
	}
}

// TestCoerceCDCRowValues_VariableScaleDecimal is the regression guard for the
// Oracle-CDC-to-Postgres total-data-loss bug. Debezium encodes an unscaled
// Oracle NUMBER (the common integer PK) as an io.debezium.data.VariableScaleDecimal
// STRUCT: {"scale":0,"value":"Aw=="} (base64 of byte 0x03 == 3). The value-decode
// path only handled the fixed-scale form (`t == "bytes" && name contains
// "decimal"`); a struct is `t == "struct"`, so the raw JSON object was forwarded
// verbatim as the column value -> Postgres "invalid input syntax for type
// numeric: {"scale": 0, "value": "Aw=="}" -> 3 retries -> .dlq -> offset
// committed -> pipeline reports Streaming/lag-0/healthy while dropping 100% of
// rows. The struct must be decoded to a plain numeric literal (mirroring the
// fixed-scale decimal branch, which already produces a string the relational
// sink accepts).
func TestCoerceCDCRowValues_VariableScaleDecimal(t *testing.T) {
	cfg := &WorkerConfig{
		PipelineID:           "test-pipeline",
		Topic:                "dbserver.RSYNCTEST.ORA_CDC",
		SinkMode:             "cdc",
		DestinationConnector: "postgresql",
	}

	cases := []struct {
		name  string
		scale int
		b64   string // base64 of the unscaled two's-complement magnitude
		want  string
	}{
		// The exact prod repro: unscaled NUMBER PK value 3.
		{name: "unscaled_positive", scale: 0, b64: "Aw==", want: "3"},
		// Variable scale: unscaled bigint 1234 with scale 2 -> 12.34.
		{name: "scaled_positive", scale: 2, b64: "BNI=", want: "12.34"},
		// Negative two's-complement single byte 0xFD == -3, unscaled.
		{name: "negative", scale: 0, b64: "/Q==", want: "-3"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fields := []interface{}{vsdField("id")}
			raw := cdcEnvelopeWithFields("c", map[string]interface{}{
				"id": map[string]interface{}{"scale": c.scale, "value": c.b64},
			}, fields)

			sm, err := parseSinkMessage(cfg, kafka.Message{Topic: cfg.Topic, Value: raw, Offset: 1})
			if err != nil {
				t.Fatalf("parseSinkMessage: %v", err)
			}
			got, ok := sm.After["id"].(string)
			if !ok {
				t.Fatalf("VariableScaleDecimal not decoded: want string %q, got %T (%#v)",
					c.want, sm.After["id"], sm.After["id"])
			}
			if got != c.want {
				t.Fatalf("VariableScaleDecimal decode: got %q, want %q", got, c.want)
			}
		})
	}
}
