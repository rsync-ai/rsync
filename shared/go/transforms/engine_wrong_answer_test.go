package transforms

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The three defects in this file share a shape: the code had enough information
// to give the right answer and gave a different one instead — a timeout reported
// as an empty result, an unset field read as a choice, an unknown table read as
// "every table".

// ------------------------------------------------- T14 preview timeout

// A preview that runs out of time returned (nil rows, warning, nil error). The
// handler saw no error, answered 200, and the UI drew an empty table — which the
// operator reads as "these transforms drop every row". A run that never finished
// cannot support that conclusion, so the timeout has to reach the caller as an
// error it can recognize.
func TestPreview_TimeoutIsAnErrorNotAnEmptyResult(t *testing.T) {
	coord := NewTransformCoordinator(NewSimpleTransformEngine(), NewDuckDBTransformEngine())
	exec := NewPreviewExecutor(coord)

	data := []Row{{"a": 1}, {"a": 2}}
	chain := []Transform{{Type: "rename_columns", Config: map[string]interface{}{
		"mappings": map[string]interface{}{"a": "b"},
	}}}

	// 1ns is already spent by the time the coordinator reaches its first ctx check.
	rows, warnings, err := exec.Preview(data, chain, time.Nanosecond)

	if err == nil {
		t.Fatalf("timed-out preview returned no error (rows=%d, warnings=%v); the handler would answer HTTP 200 with an empty table", len(rows), warnings)
	}
	if !errors.Is(err, ErrPreviewTimeout) {
		t.Fatalf("error is not recognizable as a timeout: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows alongside the timeout, got %d", len(rows))
	}
}

// A preview that finishes inside its budget must still return rows and no error.
func TestPreview_FastChainStillSucceeds(t *testing.T) {
	coord := NewTransformCoordinator(NewSimpleTransformEngine(), NewDuckDBTransformEngine())
	exec := NewPreviewExecutor(coord)

	rows, _, err := exec.Preview(
		[]Row{{"a": "x"}, {"a": "y"}},
		[]Transform{{Type: "rename_columns", Config: map[string]interface{}{
			"mappings": map[string]interface{}{"a": "b"},
		}}},
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["b"] != "x" {
		t.Fatalf("transform did not run: %v", rows[0])
	}
}

// A non-timeout failure must NOT be dressed up as a timeout.
func TestPreview_RealFailureIsNotReportedAsATimeout(t *testing.T) {
	coord := NewTransformCoordinator(NewSimpleTransformEngine(), NewDuckDBTransformEngine())
	exec := NewPreviewExecutor(coord)

	_, _, err := exec.Preview(
		[]Row{{"a": 1}},
		[]Transform{{Type: "no_such_transform_type", Config: map[string]interface{}{}}},
		5*time.Second,
	)
	if err == nil {
		t.Fatal("expected an error for an unknown transform type")
	}
	if errors.Is(err, ErrPreviewTimeout) {
		t.Fatalf("an unknown transform type was reported as a timeout: %v", err)
	}
}

// ------------------------------------------------- T15 empty mask_type

// The builder sends mask_type:"" for a rule whose dropdown was never opened. The
// parser took the empty string as a deliberate choice, applyMask's default
// branch turned it into "***", and the value was gone for good — where the
// documented default, hash, is a stable pseudonym you can still join on.
func TestMaskPII_EmptyMaskTypeFallsBackToHashNotRedact(t *testing.T) {
	out, err := applyRaw([]Row{{"email": "a@b.com"}}, "mask_pii", map[string]interface{}{
		"column":    "email",
		"mask_type": "",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, _ := out[0]["email"].(string)
	if got == "***" {
		t.Fatal(`mask_type:"" irreversibly redacted the value; the documented default is hash`)
	}
	if !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("expected a sha256 pseudonym, got %q", got)
	}
}

// Whitespace is no more a choice than an empty string is.
func TestMaskPII_BlankMaskTypeFallsBackToHash(t *testing.T) {
	out, err := applyRaw([]Row{{"email": "a@b.com"}}, "mask_pii", map[string]interface{}{
		"column":    "email",
		"mask_type": "   ",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, _ := out[0]["email"].(string); !strings.HasPrefix(got, "sha256:") {
		t.Fatalf("expected a sha256 pseudonym, got %q", got)
	}
}

// An explicit redact is still honoured — the fix must not swallow a real choice.
func TestMaskPII_ExplicitRedactStillRedacts(t *testing.T) {
	out, err := applyRaw([]Row{{"email": "a@b.com"}}, "mask_pii", map[string]interface{}{
		"column":    "email",
		"mask_type": "redact",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out[0]["email"] != "***" {
		t.Fatalf("explicit redact did not redact: %v", out[0]["email"])
	}
}

// ------------------------------------------------- T16 unknown table scope

func tableScoped(table, typ string, cfg map[string]any) []map[string]any {
	return []map[string]any{{
		"type":   typ,
		"config": cfg,
		"scope":  map[string]any{"table": table},
	}}
}

// The scope check was skipped entirely when the caller could not name the
// current table, so a rule the operator pinned to one table ran against
// whatever happened to be flowing. On a write path that is data loss, not a
// cosmetic mismatch.
func TestNormalize_TableScopedRuleWithUnknownTableIsRefusedOnWritePaths(t *testing.T) {
	for _, mode := range []NormalizeMode{NormalizeModeExecution, NormalizeModeCDC} {
		raw := tableScoped("customers", "filter", map[string]any{
			"column": "status", "operator": "equals", "value": "active",
		})
		out, _, err := NormalizeAndValidate(raw, "", mode)
		if err == nil {
			t.Fatalf("mode %s: a rule scoped to \"customers\" was accepted against an unknown table (%d transforms returned)", mode, len(out))
		}
		if !strings.Contains(err.Error(), "customers") {
			t.Fatalf("mode %s: error does not name the scope: %v", mode, err)
		}
	}
}

// Preview routinely runs on pasted sample_data with no table at all. Erroring
// there would break a legitimate workflow, so it warns and applies — but it has
// to say so, or the preview silently misrepresents what production will do.
func TestNormalize_TableScopedRuleWithUnknownTableWarnsInPreview(t *testing.T) {
	raw := tableScoped("customers", "rename_columns", map[string]any{
		"mappings": map[string]any{"name": "full_name"},
	})
	out, warnings, err := NormalizeAndValidate(raw, "", NormalizeModePreview)
	if err != nil {
		t.Fatalf("preview must not error on a missing table: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("preview should still apply the rule, got %d transforms", len(out))
	}
	if !strings.Contains(strings.Join(warnings, "\n"), "scope could not be checked") {
		t.Fatalf("preview applied a table-scoped rule without saying the scope was unchecked: %v", warnings)
	}
}

// The narrow trigger matters: an UNSCOPED rule on an unknown table is the
// ordinary case and must keep working untouched on every path.
func TestNormalize_UnscopedRuleWithUnknownTableIsUnaffected(t *testing.T) {
	for _, mode := range []NormalizeMode{NormalizeModeExecution, NormalizeModeCDC, NormalizeModePreview} {
		raw := []map[string]any{{
			"type":   "rename_columns",
			"config": map[string]any{"mappings": map[string]any{"name": "full_name"}},
		}}
		out, _, err := NormalizeAndValidate(raw, "", mode)
		if err != nil {
			t.Fatalf("mode %s: unscoped rule rejected: %v", mode, err)
		}
		if len(out) != 1 {
			t.Fatalf("mode %s: unscoped rule dropped", mode)
		}
	}
}

// Both existing scope outcomes survive: a match runs, a mismatch is skipped.
func TestNormalize_KnownTableStillMatchesAndSkips(t *testing.T) {
	cfg := map[string]any{"mappings": map[string]any{"name": "full_name"}}

	out, _, err := NormalizeAndValidate(tableScoped("customers", "rename_columns", cfg), "customers", NormalizeModeExecution)
	if err != nil || len(out) != 1 {
		t.Fatalf("matching table should keep the rule: out=%d err=%v", len(out), err)
	}

	out, _, err = NormalizeAndValidate(tableScoped("customers", "rename_columns", cfg), "orders", NormalizeModeExecution)
	if err != nil {
		t.Fatalf("non-matching table should not error: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("non-matching table should skip the rule, got %d", len(out))
	}
}

// Saving a table-scoped rule is not the same act as running one. The save path
// has no table by design, so it must not inherit execution mode's refusal - but
// it must keep every check that is intrinsic to the rule.
func TestNormalize_SaveCheckAcceptsAScopeAndStillRejectsABadRule(t *testing.T) {
	ok := tableScoped("customers", "rename_columns", map[string]any{
		"mappings": map[string]any{"name": "full_name"},
	})
	if _, _, err := NormalizeAndValidate(ok, "", NormalizeModeSaveCheck); err != nil {
		t.Fatalf("save-time validation rejected a legitimately scoped rule: %v", err)
	}

	bad := []map[string]any{{"type": "aggregate", "config": map[string]any{}}}
	if _, _, err := NormalizeAndValidate(bad, "", NormalizeModeSaveCheck); err == nil {
		t.Fatal("save-time validation accepted a transform type no engine can run")
	}

	full := []map[string]any{{
		"type":                  "rename_columns",
		"config":                map[string]any{"mappings": map[string]any{"a": "b"}},
		"requires_full_dataset": true,
	}}
	if _, _, err := NormalizeAndValidate(full, "", NormalizeModeSaveCheck); err == nil {
		t.Fatal("save-time validation accepted requires_full_dataset, which no path can run")
	}
}
