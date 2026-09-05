package main

// Unit tests for reconcileTransformedColumnTypes: value-mutating consumer transforms
// (mask_pii -> string, type_convert -> target) must override the destination column type
// so ensure_table does not CREATE a typed column the post-transform string value cannot
// be inserted into. Regression net for the shopify->pg customers silent drop where a
// mask_pii on the boolean `verifiedEmail` produced a "sha256:..." string that failed to
// insert into a BOOLEAN column.

import (
	"testing"

	"github.com/rsync-ai/shared/transforms"
)

func TestReconcileColTypes_MaskBooleanCDCVocab(t *testing.T) {
	sm := &SinkMessage{
		ColumnTypes:       map[string]string{"verifiedEmail": "BOOLEAN", "id": "BIGINT"},
		ColumnTypesAreDDL: true, // CDC path -> final DDL tokens
	}
	reconcileTransformedColumnTypes(sm, []transforms.CanonicalTransform{
		{Type: "mask_pii", Config: map[string]any{"column": "customers.verifiedEmail"}},
	})
	if got := sm.ColumnTypes["verifiedEmail"]; got != "TEXT" {
		t.Fatalf("masked boolean column should become TEXT (DDL vocab), got %q", got)
	}
	if got := sm.ColumnTypes["id"]; got != "BIGINT" {
		t.Fatalf("non-masked column must be left untouched, got %q", got)
	}
}

func TestReconcileColTypes_MaskNumericBatchVocab(t *testing.T) {
	sm := &SinkMessage{
		ColumnTypes:       map[string]string{"salary": "integer"},
		ColumnTypesAreDDL: false, // batch/executor path -> canonical tokens
	}
	reconcileTransformedColumnTypes(sm, []transforms.CanonicalTransform{
		{Type: "mask_pii", Config: map[string]any{"column": "employees.salary"}},
	})
	if got := sm.ColumnTypes["salary"]; got != "string" {
		t.Fatalf("masked numeric column should become canonical string, got %q", got)
	}
}

func TestReconcileColTypes_MaskColumnsList(t *testing.T) {
	sm := &SinkMessage{
		ColumnTypes:       map[string]string{"a": "BOOLEAN", "b": "INTEGER"},
		ColumnTypesAreDDL: true,
	}
	reconcileTransformedColumnTypes(sm, []transforms.CanonicalTransform{
		{Type: "mask_pii", Config: map[string]any{"columns": []any{"t.a", "t.b"}}},
	})
	if sm.ColumnTypes["a"] != "TEXT" || sm.ColumnTypes["b"] != "TEXT" {
		t.Fatalf("both masked columns should become TEXT, got a=%q b=%q", sm.ColumnTypes["a"], sm.ColumnTypes["b"])
	}
}

func TestReconcileColTypes_TypeConvertAndPassthrough(t *testing.T) {
	sm := &SinkMessage{
		ColumnTypes:       map[string]string{"qty": "TEXT", "flag": "TEXT"},
		ColumnTypesAreDDL: true,
	}
	reconcileTransformedColumnTypes(sm, []transforms.CanonicalTransform{
		{Type: "type_convert", Config: map[string]any{"column": "t.qty", "to": "int"}},
		{Type: "json_flatten", Config: map[string]any{"column": "t.payload"}}, // value-preserving: no override
	})
	if got := sm.ColumnTypes["qty"]; got != "BIGINT" {
		t.Fatalf("type_convert to int should set BIGINT, got %q", got)
	}
	if _, exists := sm.ColumnTypes["payload"]; exists {
		t.Fatalf("json_flatten must not add a type override")
	}
}

func TestReconcileColTypes_NilMapInitialized(t *testing.T) {
	sm := &SinkMessage{ColumnTypesAreDDL: false} // nil ColumnTypes
	reconcileTransformedColumnTypes(sm, []transforms.CanonicalTransform{
		{Type: "mask_pii", Config: map[string]any{"column": "customers.email"}},
	})
	if got := sm.ColumnTypes["email"]; got != "string" {
		t.Fatalf("nil ColumnTypes should be initialized and set, got %q", got)
	}
}
