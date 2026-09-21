package chat

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// connectorNamesOnDisk returns every name the connector tree answers to: each
// connector's directory name, the `id`/`name` in its versioned metadata.json, and
// every entry of that metadata's `aliases` array -- all folded to one key shape.
// Canonical source is versions/<latest.json.current_version>/; there are no root
// copies, so nothing else is read.
func connectorNamesOnDisk(t *testing.T) map[string]string {
	t.Helper()
	root := filepath.Join("..", "..", "..", "shared", "mcp-connectors")
	if fi, err := os.Stat(root); err != nil || !fi.IsDir() {
		// Fatal, not Skip: a skipped guard is indistinguishable from a passing one.
		t.Fatalf("connector tree not found at %s (err=%v)", root, err)
	}
	fold := func(s string) string {
		return strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(strings.TrimSpace(s)), "_", "-"), " ", "-")
	}
	names := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "latest.json" {
			return err
		}
		dir := filepath.Dir(path)
		if filepath.Base(filepath.Dir(dir)) == "versions" {
			return nil
		}
		var manifest struct {
			CurrentVersion string `json:"current_version"`
		}
		b, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(b, &manifest) != nil || manifest.CurrentVersion == "" {
			return nil
		}
		mb, err := os.ReadFile(filepath.Join(dir, "versions", manifest.CurrentVersion, "metadata.json"))
		if err != nil {
			return nil
		}
		var meta struct {
			ID      string   `json:"id"`
			Name    string   `json:"name"`
			Aliases []string `json:"aliases"`
		}
		if json.Unmarshal(mb, &meta) != nil {
			return nil
		}
		owner := meta.ID
		if owner == "" {
			owner = filepath.Base(dir)
		}
		for _, n := range append([]string{filepath.Base(dir), meta.ID, meta.Name}, meta.Aliases...) {
			if k := fold(n); k != "" {
				names[k] = owner
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk connector tree: %v", err)
	}
	return names
}

// TestEveryNormalizerTargetHasAConnector closes the direction the orchestrator's
// registry alias guards leave open. Those iterate the connectors on disk and
// assert every alias a connector DECLARES resolves back to it -- they can never
// see a target the normalizer invents, because no connector declares it. "es" ->
// "elasticsearch" normalised cleanly onto a connector that has never existed.
func TestEveryNormalizerTargetHasAConnector(t *testing.T) {
	names := connectorNamesOnDisk(t)
	if len(names) < 40 {
		t.Fatalf("only %d connector names loaded from the tree; the assertion would be near-vacuous", len(names))
	}
	if len(connectorAliases) < 10 {
		t.Fatalf("only %d alias entries; the assertion would be near-vacuous", len(connectorAliases))
	}

	targets := map[string][]string{}
	for k, v := range connectorAliases {
		targets[v] = append(targets[v], k)
	}
	seen := make([]string, 0, len(targets))
	for v := range targets {
		seen = append(seen, v)
	}
	sort.Strings(seen)
	t.Logf("checking %d distinct normalizer targets against %d connector names on disk", len(seen), len(names))

	for _, target := range seen {
		sort.Strings(targets[target])
		if owner, ok := names[target]; !ok {
			t.Errorf("NormalizeConnectorName(%v) = %q, but no connector in shared/mcp-connectors is named or aliased %q",
				targets[target], target, target)
		} else {
			t.Logf("  %-24s -> %s", target, owner)
		}
	}

	// CONTROL: the lookup must also be able to say "present" and "absent".
	if _, ok := names["postgresql"]; !ok {
		t.Fatal("CONTROL FAILED: postgresql is not in the on-disk name set")
	}
	if _, ok := names["definitely-not-a-connector"]; ok {
		t.Fatal("CONTROL FAILED: a bogus name is in the on-disk name set")
	}
}
