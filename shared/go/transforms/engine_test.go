package transforms

import (
	"context"
	"testing"
)

func TestSimpleTransformEngine_RenameColumns(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"email": "a@b.com", "name": "Alice"}}

	out, err := e.Apply(ctx, data, Transform{
		Type: "rename_columns",
		Config: map[string]interface{}{
			"mappings": map[string]interface{}{"email": "user_email"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := out[0]["email"]; ok {
		t.Fatalf("expected old key removed")
	}
	if got := out[0]["user_email"]; got != "a@b.com" {
		t.Fatalf("expected user_email to be renamed value, got %#v", got)
	}

	out2, err := e.Apply(ctx, data, Transform{
		Type: "rename_columns",
		Config: map[string]interface{}{
			"from": "users.email",
			"to":   "email_address",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := out2[0]["email"]; ok {
		t.Fatalf("expected old key removed (qualified mapping)")
	}
	if got := out2[0]["email_address"]; got != "a@b.com" {
		t.Fatalf("expected email_address value, got %#v", got)
	}
}

func TestSimpleTransformEngine_ExcludeColumns(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"id": 1, "password": "secret", "email": "a@b.com"}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "exclude_columns",
		Config: map[string]interface{}{
			"columns": []interface{}{"password", "missing"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := out[0]["password"]; ok {
		t.Fatalf("expected password dropped")
	}
	if out[0]["email"] != "a@b.com" || out[0]["id"] != 1 {
		t.Fatalf("expected other fields preserved: %#v", out[0])
	}
}

func TestSimpleTransformEngine_NullHandle_Default(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"age": nil}, {"age": 5}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "null_handle",
		Config: map[string]interface{}{
			"column":        "age",
			"strategy":      "default",
			"default_value": 0,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out[0]["age"]; got != 0 {
		t.Fatalf("expected default value applied, got %#v", got)
	}
	if got := out[1]["age"]; got != 5 {
		t.Fatalf("expected non-null preserved, got %#v", got)
	}
}

func TestSimpleTransformEngine_NullHandle_DropRow(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"age": nil}, {"age": 5}, {"name": "no-age"}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "null_handle",
		Config: map[string]interface{}{
			"column":   "age",
			"strategy": "drop_row",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 row after drop_row, got %d", len(out))
	}
	if got := out[0]["age"]; got != 5 {
		t.Fatalf("expected remaining row age=5, got %#v", got)
	}
}

func TestSimpleTransformEngine_Truncate(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"desc": "abcdef"}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "truncate",
		Config: map[string]interface{}{
			"column":     "desc",
			"max_length": 3,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out[0]["desc"]; got != "abc" {
		t.Fatalf("expected truncated string, got %#v", got)
	}

	data2 := []Row{{"desc": "áéíóú"}}
	out2, err := e.Apply(ctx, data2, Transform{
		Type: "truncate",
		Config: map[string]interface{}{
			"column":     "desc",
			"max_length": 2,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out2[0]["desc"]; got != "áé" {
		t.Fatalf("expected rune-safe truncation, got %#v", got)
	}
}

func TestSimpleTransformEngine_TypeConvert(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"n": "42"}, {"n": 4.7}, {"n": "abc"}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "type_convert",
		Config: map[string]interface{}{
			"column":   "n",
			"to":       "int",
			"on_error": "null",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out[0]["n"] != 42 {
		t.Fatalf("expected 42, got %#v", out[0]["n"])
	}
	if out[1]["n"] != 4 {
		t.Fatalf("expected 4, got %#v", out[1]["n"])
	}
	if out[2]["n"] != nil {
		t.Fatalf("expected nil on error, got %#v", out[2]["n"])
	}

	out2, err := e.Apply(ctx, data, Transform{
		Type: "type_convert",
		Config: map[string]interface{}{
			"column":   "n",
			"to":       "int",
			"on_error": "skip",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out2[2]["n"] != "abc" {
		t.Fatalf("expected original preserved on skip, got %#v", out2[2]["n"])
	}

	_, err = e.Apply(ctx, data, Transform{
		Type: "type_convert",
		Config: map[string]interface{}{
			"column":   "n",
			"to":       "int",
			"on_error": "error",
		},
	})
	if err == nil {
		t.Fatalf("expected error on on_error=error")
	}
}
