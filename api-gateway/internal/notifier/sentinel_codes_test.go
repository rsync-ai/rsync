package notifier

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// The sentinel agents live in the other module and publish their own catalog
// codes (backend-orchestrator/internal/agents/sentinel/notify.go). Nothing
// links the two: a code renamed or added over there compiles fine here, and
// the only symptom is a degraded headline in somebody's inbox — the humanized
// event type instead of the sentence written for that failure.
//
// So read the producer and check every code it emits has copy here. This reads
// the file rather than a golden list on purpose: a golden list is one more
// thing to forget to update, and it would pass while the producer drifts.
func TestEverySentinelCodeHasCopy(t *testing.T) {
	path := filepath.Join("..", "..", "..", "backend-orchestrator",
		"internal", "agents", "sentinel", "notify.go")

	src, err := os.ReadFile(path)
	if err != nil {
		// Not a skip. A skip here is a green test covering nothing: the guard
		// would go quiet exactly when the producer was moved or deleted.
		t.Fatalf("cannot read the sentinel alert producer at %s: %v\n"+
			"If it moved, update this path — do not delete the check.", path, err)
	}

	codes := regexp.MustCompile(`(?m)^\s*code:\s*"([A-Z0-9_]+)"`).FindAllStringSubmatch(string(src), -1)
	if len(codes) == 0 {
		t.Fatalf("parsed 0 codes out of %s — the producer's shape changed and this "+
			"check silently stopped checking anything", path)
	}

	for _, m := range codes {
		code := m[1]
		entry, ok := catalog[code]
		if !ok {
			t.Errorf("the sentinel publishes %s but no catalog entry renders it; "+
				"the bell would fall back to a humanized event type", code)
			continue
		}
		if entry.Title == "" || entry.Impact == "" || entry.ActionLabel == "" {
			t.Errorf("%s has a catalog entry with an empty field: %+v", code, entry)
		}
		// Every one of these means the data stopped moving. If one is filed as
		// info it drops below the delivery threshold for email and Slack.
		if entry.Severity != severityCritical {
			t.Errorf("%s severity = %q, want %q — a sentinel alert that is not "+
				"critical will not be delivered off the bell", code, entry.Severity, severityCritical)
		}
		if _, ok := codeCategory[code]; !ok {
			t.Errorf("%s has no category, so a user cannot mute it on its own", code)
		}
	}
	t.Logf("checked %d sentinel codes against the catalog", len(codes))
}
