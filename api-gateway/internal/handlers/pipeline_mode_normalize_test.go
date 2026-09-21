package handlers

import (
	"strings"
	"testing"
)

// A batch pipeline created from chat must not persist the source connection's
// cdc_mode column default: the UI read it as "CDC" and badged the batch pipeline.
func TestNormalizePersistedPipelineModes(t *testing.T) {
	cases := []struct {
		sync, cdc, initial          string
		wantSync, wantCDC, wantInit string
	}{
		{"batch", "initial", "true", "batch", "", ""},
		{"BATCH", "streaming_only", "", "BATCH", "", ""},
		{"cdc", "initial", "true", "cdc", "initial", "true"},
		{"", "initial", "", "", "initial", ""},
	}
	for _, tc := range cases {
		s, c, i := normalizePersistedPipelineModes(tc.sync, tc.cdc, tc.initial)
		if s != tc.wantSync || c != tc.wantCDC || i != tc.wantInit {
			t.Errorf("normalizePersistedPipelineModes(%q,%q,%q) = (%q,%q,%q), want (%q,%q,%q)",
				tc.sync, tc.cdc, tc.initial, s, c, i, tc.wantSync, tc.wantCDC, tc.wantInit)
		}
	}
}

// An explicit sync_mode wins over a stray cdc_mode: a completed batch pipeline
// carrying cdc_mode='initial' was treated as CDC, so GetPipeline skipped its
// status reconciliation and the list badge stayed 'running'.
func TestPipelineModeIsCDC(t *testing.T) {
	s := func(v string) *string { return &v }
	cases := []struct {
		name      string
		sync, cdc *string
		want      bool
	}{
		{"batch with stray cdc_mode", s("batch"), s("initial"), false},
		{"explicit cdc", s("CDC"), nil, true},
		{"legacy cdc_mode only", nil, s("initial"), true},
		{"blank sync, cdc_mode", s("  "), s("streaming_only"), true},
		{"nothing", nil, nil, false},
	}
	for _, tc := range cases {
		if got := pipelineModeIsCDC(tc.sync, tc.cdc); got != tc.want {
			t.Errorf("%s: pipelineModeIsCDC = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The list SQL must not treat a batch row's cdc_mode, or the source
// connection's cdc_mode column default, as a CDC signal.
func TestPipelineCDCPredicatesIgnoreStrayCDCMode(t *testing.T) {
	for _, bad := range []string{"sc.cdc_mode IS NOT NULL", "(p.sync_mode = 'cdc' OR p.cdc_mode IS NOT NULL)"} {
		if strings.Contains(pipelineDerivedStatusCaseSQL, bad) || strings.Contains(pipelineListIsCDCSQL, bad) {
			t.Errorf("predicate still contains %q", bad)
		}
	}
	if !strings.Contains(pipelineRowIsCDCSQL, "COALESCE(TRIM(p.sync_mode), '') = '' AND p.cdc_mode IS NOT NULL") {
		t.Errorf("cdc_mode must only count when sync_mode is unset: %s", pipelineRowIsCDCSQL)
	}
}
