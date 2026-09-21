package namespacefilter

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// vectorsPath is the file shared with the Python twin
// (shared/mcp-connectors/tests/test_namespace_filter.py).
var vectorsPath = filepath.Join("..", "..", "..", "shared", "mcp-connectors", "public", "namespace_filter_vectors.json")

type vectorCase struct {
	Name           string            `json:"name"`
	Config         map[string]string `json:"config"`
	ExpectError    bool              `json:"expect_error"`
	ExpectMode     string            `json:"expect_mode"`
	ExpectPatterns []string          `json:"expect_patterns"`
	Candidates     []string          `json:"candidates"`
	SystemNames    []string          `json:"system_names"`
	ExpectKept     []string          `json:"expect_kept"`
	ExpectWarning  bool              `json:"expect_warning"`
}

func loadVectors(t *testing.T) []vectorCase {
	t.Helper()
	data, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatalf("read vectors %s: %v", vectorsPath, err)
	}
	var file struct {
		Cases []vectorCase `json:"cases"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return file.Cases
}

func TestNamespaceFilterVectors(t *testing.T) {
	cases := loadVectors(t)
	errCases := 0
	for _, c := range cases {
		if c.ExpectError {
			errCases++
		}
	}
	t.Logf("namespace filter vectors: %d (%d apply, %d error)", len(cases), len(cases)-errCases, errCases)
	if len(cases) == 0 || errCases == 0 || errCases == len(cases) {
		t.Fatalf("vector file must hold both apply and error cases; got %d total, %d error", len(cases), errCases)
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			f, err := Parse(c.Config)
			if c.ExpectError {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("Parse: want ErrInvalid, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if f.Mode != c.ExpectMode {
				t.Errorf("mode = %q, want %q", f.Mode, c.ExpectMode)
			}
			if !reflect.DeepEqual(f.Patterns, c.ExpectPatterns) {
				t.Errorf("patterns = %q, want %q", f.Patterns, c.ExpectPatterns)
			}
			r := f.Apply(c.Candidates, c.SystemNames)
			if !reflect.DeepEqual(r.Kept, c.ExpectKept) {
				t.Errorf("kept = %q, want %q", r.Kept, c.ExpectKept)
			}
			if r.Matched != len(c.ExpectKept) || r.Excluded != len(c.Candidates)-len(c.ExpectKept) {
				t.Errorf("matched/excluded = %d/%d", r.Matched, r.Excluded)
			}
			if (r.Warning != "") != c.ExpectWarning {
				t.Errorf("warning = %q, want warning=%v", r.Warning, c.ExpectWarning)
			}
		})
	}
}

func TestNamespaceFilterRejectsUnknownMode(t *testing.T) {
	_, err := Parse(map[string]string{ModeKey: "only", PatternsKey: "sales"})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func TestNamespaceFilterIncludeWithNoPatternsFailsClosed(t *testing.T) {
	_, err := Parse(map[string]string{ModeKey: "include"})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func TestNamespaceFilterMessagesCarryNoConfigValues(t *testing.T) {
	for _, cfg := range []map[string]string{
		{ModeKey: "secret_mode_value"},
		{ModeKey: "include", PatternsKey: strings.Repeat("secret_pattern_value", 10)},
	} {
		_, err := Parse(cfg)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("want a value-free error, got %v", err)
		}
	}
}
