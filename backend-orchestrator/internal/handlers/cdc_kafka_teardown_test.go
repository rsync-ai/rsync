package handlers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// The pipeline under test and a second pipeline whose id8 differs by one char.
const (
	testPipelineUUID  = "abd8a64d-1f2e-4c3b-9a7d-5e6f70819234"
	testPipelineID8   = "abd8a64d"
	otherPipelineUUID = "abd8a64c-0000-0000-0000-000000000000"
	otherPipelineID8  = "abd8a64c"
)

func testNames() pipelineKafkaNames {
	return pipelineKafkaNames{id8: testPipelineID8, uuid: testPipelineUUID}
}

func TestOwnsTopic(t *testing.T) {
	n := testNames()

	owned := []string{
		// Debezium topic.prefix == connector name; carries schema-change events.
		"cdc-" + testPipelineID8,
		// Per-table CDC topics and their DLQs.
		"cdc-" + testPipelineID8 + ".inventory.orders",
		"cdc-" + testPipelineID8 + ".inventory.orders.dlq",
		// Debezium schema history.
		"schemahistory.cdc-" + testPipelineID8,
		// Batch backfill.
		"pipeline." + testPipelineID8 + ".data",
		"pipeline." + testPipelineID8 + ".data.dlq",
	}
	for _, topic := range owned {
		if !n.ownsTopic(topic) {
			t.Errorf("ownsTopic(%q) = false, want true (topic would leak)", topic)
		}
	}

	notOwned := []string{
		// Another pipeline's topics — deleting these is the catastrophic case.
		"cdc-" + otherPipelineID8,
		"cdc-" + otherPipelineID8 + ".inventory.orders",
		"schemahistory.cdc-" + otherPipelineID8,
		"pipeline." + otherPipelineID8 + ".data",
		// A longer id that merely STARTS with ours: the anchoring test. A plain
		// HasPrefix on the id would match these and delete another pipeline's data.
		"cdc-" + testPipelineID8 + "e",
		"cdc-" + testPipelineID8 + "e.inventory.orders",
		"schemahistory.cdc-" + testPipelineID8 + "e",
		"pipeline." + testPipelineID8 + "e.data",
		// Missing the "." terminator entirely.
		"pipeline." + testPipelineID8,
		// Shared cluster infrastructure — never ours to delete.
		"_schemas",
		"connect-offsets",
		"connect-configs",
		"connect-status",
		"__consumer_offsets",
		"agent.control",
	}
	for _, topic := range notOwned {
		if n.ownsTopic(topic) {
			t.Errorf("ownsTopic(%q) = true, want false (would delete a topic we do not own)", topic)
		}
	}
}

func TestOwnsGroup(t *testing.T) {
	n := testNames()

	owned := []string{
		"sink-" + testPipelineID8,               // CDC streaming sink
		"sink-" + testPipelineID8 + "-batch",    // batch backfill sink
		"sink-" + testPipelineID8 + "-1a2b3c4d", // per-execution sink (streaming_only/never)
		"cdc-schema-changes-" + testPipelineUUID,
		"cdc-table-stats-" + testPipelineUUID,
	}
	for _, group := range owned {
		if !n.ownsGroup(group) {
			t.Errorf("ownsGroup(%q) = false, want true (group would leak)", group)
		}
	}

	notOwned := []string{
		"sink-" + otherPipelineID8,
		"sink-" + otherPipelineID8 + "-batch",
		// Longer id starting with ours — the "-" terminator is what rejects it.
		"sink-" + testPipelineID8 + "e",
		"cdc-schema-changes-" + otherPipelineUUID,
		"cdc-table-stats-" + otherPipelineUUID,
		// The cdcstats groups key on the FULL uuid, not the id8 prefix.
		"cdc-table-stats-" + testPipelineID8,
		"cdc-schema-changes-" + testPipelineID8,
	}
	for _, group := range notOwned {
		if n.ownsGroup(group) {
			t.Errorf("ownsGroup(%q) = true, want false (would stop/delete a group we do not own)", group)
		}
	}
}

// A nil TopologyManager (broker unreachable at startup) must still yield the two
// long-lived sink groups, so the streaming worker is stopped even when the
// broker cannot be listed.
func TestDiscoverSinkGroupsFallsBackWithoutTopologyManager(t *testing.T) {
	got := discoverSinkGroups(t.Context(), nil, testNames())

	want := map[string]bool{
		"sink-" + testPipelineID8:            false,
		"sink-" + testPipelineID8 + "-batch": false,
	}
	for _, g := range got {
		if _, ok := want[g]; !ok {
			t.Errorf("discoverSinkGroups returned unexpected group %q", g)
			continue
		}
		want[g] = true
	}
	for g, seen := range want {
		if !seen {
			t.Errorf("discoverSinkGroups did not return %q", g)
		}
	}
}

// The sweep has to match the namespace-qualified spelling as well as the bare
// one. Producers now mint qualified names, and a matcher that only knew the bare
// form would sweep NOTHING for a pipeline created after the rename: the delete
// call succeeds, the log says the teardown ran, and 1-2 consumer groups plus
// every CDC topic stay on the customer's broker forever. That is the exact leak
// this file was written to close, re-opened silently.
//
// The expected names are spelled as literals rather than built with
// kafkaclient.Topic/Group. A test that derives its expectations from the same
// function the code under test calls agrees with the code by construction and
// cannot fail.
func TestOwnsMatchesNamespaceQualifiedNames(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	n := testNames()

	ownedTopics := []string{
		"rsync.cdc-" + testPipelineID8,
		"rsync.cdc-" + testPipelineID8 + ".inventory.orders",
		"rsync.cdc-" + testPipelineID8 + ".inventory.orders.dlq",
		// The prefix goes on the front of the whole name, not between
		// "schemahistory." and the connector name — debezium connector.py:528
		// wraps the joined string.
		"rsync.schemahistory.cdc-" + testPipelineID8,
		"rsync.pipeline." + testPipelineID8 + ".data",
		"rsync.pipeline." + testPipelineID8 + ".data.dlq",
	}
	for _, topic := range ownedTopics {
		if !n.ownsTopic(topic) {
			t.Errorf("ownsTopic(%q) = false, want true (qualified topic would leak)", topic)
		}
	}

	ownedGroups := []string{
		"rsync.sink-" + testPipelineID8,
		"rsync.sink-" + testPipelineID8 + "-batch",
		"rsync.sink-" + testPipelineID8 + "-stream",
		"rsync.sink-" + testPipelineID8 + "-1a2b3c4d",
		"rsync.cdc-schema-changes-" + testPipelineUUID,
		"rsync.cdc-table-stats-" + testPipelineUUID,
	}
	for _, group := range ownedGroups {
		if !n.ownsGroup(group) {
			t.Errorf("ownsGroup(%q) = false, want true (qualified group would leak)", group)
		}
	}

	// Both spellings stay live at once: a deployment that adopts the prefix keeps
	// the resources it created before the rename, and the same sweep reclaims
	// both halves.
	for _, bare := range []string{"cdc-" + testPipelineID8, "schemahistory.cdc-" + testPipelineID8} {
		if !n.ownsTopic(bare) {
			t.Errorf("ownsTopic(%q) = false, want true (pre-rename topic orphaned)", bare)
		}
	}
	for _, bare := range []string{"sink-" + testPipelineID8, "cdc-table-stats-" + testPipelineUUID} {
		if !n.ownsGroup(bare) {
			t.Errorf("ownsGroup(%q) = false, want true (pre-rename group orphaned)", bare)
		}
	}
}

// Qualifying must not widen what the sweep claims. The anchoring that keeps
// abd8a64d from matching abd8a64de has to hold in the qualified spelling too —
// otherwise the rename turns a leak into a deletion of a NEIGHBOURING
// pipeline's data, which is unrecoverable rather than merely untidy.
func TestOwnsRejectsForeignQualifiedNames(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "rsync.")
	n := testNames()

	notOwnedTopics := []string{
		"rsync.cdc-" + otherPipelineID8,
		"rsync.cdc-" + otherPipelineID8 + ".inventory.orders",
		"rsync.schemahistory.cdc-" + otherPipelineID8,
		"rsync.pipeline." + otherPipelineID8 + ".data",
		// Longer id that merely starts with ours.
		"rsync.cdc-" + testPipelineID8 + "e",
		"rsync.cdc-" + testPipelineID8 + "e.inventory.orders",
		"rsync.schemahistory.cdc-" + testPipelineID8 + "e",
		"rsync.pipeline." + testPipelineID8 + "e.data",
		"rsync.pipeline." + testPipelineID8,
		// Another product's namespace on the same shared cluster. Ours ends in
		// ".", theirs does not — "rsync" is not "rsync.".
		"rsyncx.cdc-" + testPipelineID8,
		// Shared cluster infrastructure.
		"_schemas",
		"connect-offsets",
		"__consumer_offsets",
	}
	for _, topic := range notOwnedTopics {
		if n.ownsTopic(topic) {
			t.Errorf("ownsTopic(%q) = true, want false (would delete a topic we do not own)", topic)
		}
	}

	notOwnedGroups := []string{
		"rsync.sink-" + otherPipelineID8,
		"rsync.sink-" + otherPipelineID8 + "-batch",
		"rsync.sink-" + testPipelineID8 + "e",
		"rsync.cdc-schema-changes-" + otherPipelineUUID,
		"rsync.cdc-table-stats-" + otherPipelineUUID,
		// The cdcstats groups key on the FULL uuid, not the id8 prefix.
		"rsync.cdc-table-stats-" + testPipelineID8,
		// Exact match only: a suffix on a cdcstats group is somebody else's.
		"rsync.cdc-table-stats-" + testPipelineUUID + "-2",
		"rsyncx.sink-" + testPipelineID8,
	}
	for _, group := range notOwnedGroups {
		if n.ownsGroup(group) {
			t.Errorf("ownsGroup(%q) = true, want false (would stop/delete a group we do not own)", group)
		}
	}
}

// KAFKA_TOPIC_PREFIX="" is the migration lever for a deployment whose live
// topics and committed offsets predate the namespace. With qualification off,
// the qualified and bare forms collapse to the same string; the matcher must
// still work rather than degenerate into matching everything.
func TestOwnsWithQualificationDisabled(t *testing.T) {
	t.Setenv("KAFKA_TOPIC_PREFIX", "")
	n := testNames()

	if !n.ownsTopic("cdc-" + testPipelineID8 + ".inventory.orders") {
		t.Error("ownsTopic bare CDC topic = false, want true")
	}
	if !n.ownsGroup("sink-" + testPipelineID8 + "-stream") {
		t.Error("ownsGroup bare sink group = false, want true")
	}
	if n.ownsTopic("cdc-" + otherPipelineID8) {
		t.Error("ownsTopic another pipeline's topic = true, want false")
	}
	if n.ownsGroup("sink-" + otherPipelineID8) {
		t.Error("ownsGroup another pipeline's group = true, want false")
	}
	// With the prefix off, a qualified name is a foreign string like any other.
	if n.ownsTopic("rsync.cdc-" + testPipelineID8) {
		t.Error("ownsTopic qualified topic under empty prefix = true, want false")
	}
}

// The cross-package half of the cdcstats contract, checked from the side that
// can actually call ownsGroup.
//
// cdcstats mints "cdc-table-stats-<uuid>" and "cdc-schema-changes-<uuid>" and
// puts both through kafkaclient.Group. This sweep is the only thing that ever
// reaps them, and the two halves live in different packages, so the failure mode
// is a rename on one side alone: the delete still succeeds, the sweep matches
// nothing, and one consumer group per deleted pipeline accumulates on the
// customer's broker forever. Nothing raises.
//
// The prefixes are read out of cdcstats' own source rather than copied here.
// A copy would keep passing after the rename it exists to catch, which is what
// the cdcstats-side version of this test did: both sides of its comparison were
// built from the same package's constants, so it never touched this file.
func TestOwnsGroupCoversTheGroupsCDCStatsActuallyMints(t *testing.T) {
	prefixes := map[string]string{
		"tableStatsGroupPrefix":   constFromSource(t, "../agents/cdcstats/kafka_identity.go", "tableStatsGroupPrefix"),
		"schemaChangeGroupPrefix": constFromSource(t, "../agents/cdcstats/schema_changes.go", "schemaChangeGroupPrefix"),
	}

	n := testNames()
	for name, prefix := range prefixes {
		// cdcstats hands Group() the base with the FULL uuid; both spellings
		// have to be reaped, because a pipeline created before namespacing has
		// a live group under the bare one.
		base := prefix + testPipelineUUID
		for _, spelling := range []string{base, kafkaclient.Group(base)} {
			if !n.ownsGroup(spelling) {
				t.Errorf("ownsGroup(%q) = false: cdcstats mints this from %s, so the group "+
					"survives the pipeline delete and leaks onto the broker", spelling, name)
			}
		}
		// And it must not have widened into a prefix match that would reap
		// another pipeline's group.
		if n.ownsGroup(prefix + otherPipelineUUID) {
			t.Errorf("ownsGroup(%q) = true: this is a different pipeline's group", prefix+otherPipelineUUID)
		}
	}
}

// constFromSource reads a `const <name> = "..."` out of a Go file. Parsing the
// other package's source is the only way to reference an unexported constant
// across a package boundary, and the alternative -- restating the literal --
// is exactly the failure this test is here to catch.
func constFromSource(t *testing.T, path, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, id := range vs.Names {
				if id.Name != name || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				v, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquoting %s in %s: %v", name, path, err)
				}
				return v
			}
		}
	}
	// Anti-vacuity: a rename that this parse cannot find would otherwise make
	// the test above check the empty string against nothing and pass.
	t.Fatalf("const %s not found in %s -- it was renamed or moved, and the teardown "+
		"sweep must be updated to match before this test can mean anything", name, path)
	return ""
}
