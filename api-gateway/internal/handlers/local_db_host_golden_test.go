package handlers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestIsLocalDBHostMatchesSharedGolden pins isLocalDBHost to shared/local_db_host_golden.json,
// which every Python copy of the same check (_is_local_db_host in the DB
// connectors and the generator template) is pinned to by
// shared/mcp-connectors/tests/test_local_db_host_golden.py. A LOCAL host gets TLS
// off by default. A Kubernetes in-cluster name (*.svc.cluster.local) read as
// remote, so an in-cluster database with no TLS listener was dialled with TLS
// forced on (GKE, v0.1.7 RC).
func TestIsLocalDBHostMatchesSharedGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "shared", "local_db_host_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden struct {
		Local  []string `json:"local"`
		Remote []string `json:"remote"`
	}
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.Local) == 0 || len(golden.Remote) == 0 {
		t.Fatalf("golden has an empty list (local=%d remote=%d); the test would pass by checking nothing", len(golden.Local), len(golden.Remote))
	}
	for _, h := range golden.Local {
		if !isLocalDBHost(h) {
			t.Errorf("isLocalDBHost(%q) = false, want true (local)", h)
		}
	}
	for _, h := range golden.Remote {
		if isLocalDBHost(h) {
			t.Errorf("isLocalDBHost(%q) = true, want false (remote)", h)
		}
	}
}
