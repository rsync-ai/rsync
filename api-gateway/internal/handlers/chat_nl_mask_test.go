package handlers

import (
	"reflect"
	"testing"
)

func TestParseMaskingIntent(t *testing.T) {
	cases := []struct {
		name     string
		message  string
		wantCols []string
		wantPII  bool
	}{
		{
			name:     "no masking verb",
			message:  "sync postgres orders to s3 in real-time",
			wantCols: nil,
			wantPII:  false,
		},
		{
			name:     "explicit single column",
			message:  "mask email",
			wantCols: []string{"email"},
			wantPII:  false,
		},
		{
			// The exact phrasing from the staging test: mask must not swallow the
			// downstream "convert ... type" clause, and "other column" is noise.
			message:  "move orders from postgres to s3 real-time and mask email and other column and convert all string column to ai recommended data type",
			name:     "mask clause bounded before convert",
			wantCols: []string{"email"},
			wantPII:  false,
		},
		{
			name:     "multiple explicit columns",
			message:  "redact ssn and email",
			wantCols: []string{"ssn", "email"},
			wantPII:  false,
		},
		{
			name:     "generic pii",
			message:  "sync mysql to bigquery and mask all pii",
			wantCols: nil,
			wantPII:  true,
		},
		{
			name:     "generic sensitive",
			message:  "mask sensitive data",
			wantCols: nil,
			wantPII:  true,
		},
		{
			name:     "bare mask falls back to pii",
			message:  "mask the data",
			wantCols: nil,
			wantPII:  true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols, pii := parseMaskingIntent(tc.message)
			if pii != tc.wantPII {
				t.Errorf("maskPII = %v, want %v", pii, tc.wantPII)
			}
			if len(cols) == 0 && len(tc.wantCols) == 0 {
				return
			}
			if !reflect.DeepEqual(cols, tc.wantCols) {
				t.Errorf("maskColumns = %#v, want %#v", cols, tc.wantCols)
			}
		})
	}
}

func TestParseTypeConvertIntent(t *testing.T) {
	yes := []string{
		"convert all string column to ai recommended data type",
		"sync mysql to s3 and convert columns to recommended types",
		"apply type conversion",
		"convert string columns to their proper types",
		"mask email and convert all string column to the recommended type",
	}
	no := []string{
		"sync postgres to s3 in real-time",
		"mask email and phone",
		"copy the orders table to bigquery every hour",
	}
	for _, m := range yes {
		if !parseTypeConvertIntent(m) {
			t.Errorf("parseTypeConvertIntent(%q) = false, want true", m)
		}
	}
	for _, m := range no {
		if parseTypeConvertIntent(m) {
			t.Errorf("parseTypeConvertIntent(%q) = true, want false", m)
		}
	}
}

func TestNLTransformSpecEmpty(t *testing.T) {
	if !(nlTransformSpec{}).empty() {
		t.Fatalf("zero spec must be empty")
	}
	if (nlTransformSpec{MaskColumns: []string{"email"}}).empty() {
		t.Fatalf("spec with mask column must not be empty")
	}
	if (nlTransformSpec{MaskPII: true}).empty() {
		t.Fatalf("spec with MaskPII must not be empty")
	}
	if (nlTransformSpec{TypeConvert: true}).empty() {
		t.Fatalf("spec with TypeConvert must not be empty")
	}
}
