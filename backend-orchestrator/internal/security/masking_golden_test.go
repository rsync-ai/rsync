package security

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestSensitiveKeysMatchSharedGolden pins SensitiveKeys to
// shared/sensitive_keys_golden.json, which api-gateway's copy of this list and
// llm-service's SENSITIVE_KEYS are pinned to as well. The lists were kept in step
// by a comment; a key added to one service's log redaction and not the other's
// leaks that field from the second service's logs with no failing test.
func TestSensitiveKeysMatchSharedGolden(t *testing.T) {
	path := filepath.Join("..", "..", "..", "shared", "sensitive_keys_golden.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	var golden struct {
		CredentialKeys []string `json:"credential_keys"`
		PIIKeys        []string `json:"pii_keys"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.CredentialKeys) == 0 || len(golden.PIIKeys) == 0 {
		t.Fatal("golden fixture has an empty key group")
	}
	want := append(append([]string{}, golden.CredentialKeys...), golden.PIIKeys...)
	got := append([]string{}, SensitiveKeys...)
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("SensitiveKeys drifted from the shared golden\n got: %v\nwant: %v", got, want)
	}
}
