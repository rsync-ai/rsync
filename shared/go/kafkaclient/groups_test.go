package kafkaclient

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The reason Group exists: a customer writing exact-match Group ACLs has to be
// able to enumerate what to grant, and "rsync.*" is only enumerable if every
// group id actually carries the prefix.
func TestGroupQualifiesEveryConsumerGroupShape(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	for _, name := range []string{
		"cdc-sink-abc12345",
		"pipeline-abc12345-consumer",
		"agent-planner",
		"sentinel-dlq-replay",
		"schema-history-abc12345",
	} {
		got := Group(name)
		want := "rsync." + name
		if got != want {
			t.Errorf("Group(%q) = %q, want %q", name, got, want)
		}
	}
}

// The whole point of delegating to Topic rather than copying its logic: a group
// prefix that drifts from the topic prefix means an operator grants rsync.* on
// topics and something else on groups, and the join fails with an authorization
// error naming neither variable.
func TestGroupAndTopicApplyTheSamePrefix(t *testing.T) {
	for _, prefix := range []string{"rsync.", "", "acme", "rs ync/co:rp", "///"} {
		t.Setenv(EnvTopicPrefix, prefix)
		for _, name := range []string{"cdc-sink-abc12345", "pipeline.abc12345.data", "", "   "} {
			if g, tp := Group(name), Topic(name); g != tp {
				t.Errorf("prefix=%q: Group(%q) = %q but Topic(%q) = %q", prefix, name, g, name, tp)
			}
		}
	}
}

// Group ids are persisted and read back on the next run (a consumer that
// rejoins under rsync.rsync.cdc-sink is a different group, so it re-reads the
// topic from its own configured offset instead of resuming).
func TestGroupQualificationIsIdempotent(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	once := Group("cdc-sink-abc12345")
	twice := Group(once)
	thrice := Group(twice)
	if once != twice || twice != thrice {
		t.Fatalf("not idempotent: %q -> %q -> %q", once, twice, thrice)
	}
	if strings.Count(once, "rsync.") != 1 {
		t.Fatalf("prefix appears %d times in %q, want 1", strings.Count(once, "rsync."), once)
	}
}

// The same migration lever topics have. A deployment with live groups and
// committed offsets under bare ids must be able to take this code without
// resetting every consumer's position.
func TestEmptyPrefixLeavesGroupIDsUntouched(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "")
	for _, name := range []string{"cdc-sink-abc12345", "agent-planner"} {
		if got := Group(name); got != name {
			t.Errorf("Group(%q) with empty prefix = %q, want it unchanged", name, got)
		}
	}
}

func TestGroupPrefixWithoutSeparatorGainsOne(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync")
	if got := Group("cdc-sink"); got != "rsync.cdc-sink" {
		t.Fatalf("Group(cdc-sink) = %q, want rsync.cdc-sink", got)
	}
}

// A group id is not a topic name, but Kafka's legal character set for the two
// overlaps, and a prefix carrying a space would make every derived group id
// illegal at once.
func TestIllegalPrefixCharactersAreDroppedFromGroupIDs(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rs ync/co:rp")
	got := Group("cdc-sink")
	if strings.ContainsAny(got, " /:") {
		t.Fatalf("Group() = %q, still carries characters Kafka rejects", got)
	}
	if got != "rsynccorp.cdc-sink" {
		t.Fatalf("Group() = %q, want rsynccorp.cdc-sink", got)
	}
}

func TestEmptyGroupNameStaysEmpty(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	if got := Group(""); got != "" {
		t.Fatalf("Group(\"\") = %q, want empty", got)
	}
	if got := Group("   "); got != "" {
		t.Fatalf("Group(whitespace) = %q, want empty", got)
	}
}

func TestGroupsQualifiesEveryElement(t *testing.T) {
	t.Setenv(EnvTopicPrefix, "rsync.")
	got := Groups("cdc-sink-a", "cdc-sink-b")
	want := []string{"rsync.cdc-sink-a", "rsync.cdc-sink-b"}
	if len(got) != len(want) {
		t.Fatalf("Groups returned %d ids, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Groups()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// Group inherits Topic's contract, so it must satisfy the same cross-language
// table -- including the unset-prefix case, which is what a deployment that
// configures nothing actually gets. If a later change gives groups their own
// computation, this fails before it reaches a customer's ACLs.
func TestGroupMatchesTheTopicNamingContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "contracts", "kafka-topic-naming.json"))
	if err != nil {
		t.Fatalf("reading the shared contract: %v", err)
	}
	var contract struct {
		Cases []struct {
			Prefix *string `json:"prefix"`
			Input  string  `json:"input"`
			Want   string  `json:"want"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("parsing the shared contract: %v", err)
	}
	if len(contract.Cases) == 0 {
		t.Fatal("the shared contract has no cases; it would pass vacuously")
	}
	for _, c := range contract.Cases {
		if c.Prefix == nil {
			os.Unsetenv(EnvTopicPrefix)
		} else {
			t.Setenv(EnvTopicPrefix, *c.Prefix)
		}
		if got := Group(c.Input); got != c.Want {
			p := "<unset>"
			if c.Prefix != nil {
				p = *c.Prefix
			}
			t.Errorf("prefix=%q Group(%q) = %q, want %q", p, c.Input, got, c.Want)
		}
	}
}
