package executor

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/rsync-ai/shared/kafkaclient"
)

// The Debezium schema-history topic is the one topic in the CDC data plane that
// nobody created. Debezium's own default is publication-by-produce: Connect writes
// to schema.history.internal.kafka.topic and lets the broker bring it into being.
// That works on the bundled broker, which auto-creates, and fails on a customer's,
// which usually does not — and it fails in the worst shape available, because the
// history is only READ on connector RESTART. A pipeline provisions clean, snapshots,
// streams for days, and then dies on a restart with an error that names a missing
// topic and nothing about why.
//
// Two independent pieces of code now have to agree on that topic's name: the
// orchestrator, which pre-creates it with the right geometry before start_sync, and
// the connector, which puts it in the Debezium config. They agree because the
// orchestrator SENDS the name (params["schema_history_topic"]) — but the connector
// still derives a fallback for callers that omit it, and the two derivations must
// produce the same string or the fix is worse than the bug: the orchestrator creates
// one topic with correct retention, Connect writes to a different auto-created one,
// and the failure moves back to first restart while looking fixed.
//
// So this file pins both halves:
//   - the name, against the connector's _safe_name/_qualify_topic contract
//     (mirrored case-for-case in the connector's own test_topic_naming.py), and
//   - the ordering, because a pre-create that runs after Connect has already
//     auto-created the topic changes nothing at all.

// TestDebeziumSafeNameMatchesTheConnectorsRule locks the Go copy of _safe_name to the
// Python original in
// shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py.
//
// The trap this table exists for: _safe_name's legal set is [a-z0-9_-] — it does NOT
// include the dot, even though the dot is legal in a Kafka topic name and every other
// naming helper in the repo keeps it. A connector name carrying a dot therefore
// becomes an underscore here and would stay a dot in a naive Go reimplementation, and
// the two topics differ by one character.
func TestDebeziumSafeNameMatchesTheConnectorsRule(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		maxLen int
		want   string
	}{
		{"plain connector name is unchanged", "cdc-3a7e63e5", 80, "cdc-3a7e63e5"},
		{"upper-cased is folded down", "CDC-3A7E63E5", 80, "cdc-3a7e63e5"},
		{"surrounding whitespace is trimmed", "  cdc-3a7e63e5  ", 80, "cdc-3a7e63e5"},
		// The dot case. Legal in Kafka, illegal in _safe_name.
		{"dots become underscores", "cdc.pipeline.7f2", 80, "cdc_pipeline_7f2"},
		{"illegal runs collapse to one underscore", "cdc//pipeline!!7f2", 80, "cdc_pipeline_7f2"},
		{"existing underscore runs also collapse", "cdc__pipeline___7f2", 80, "cdc_pipeline_7f2"},
		{"leading and trailing underscores are stripped", "__cdc-7f2__", 80, "cdc-7f2"},
		{"a name of only illegal characters falls back", "!!!", 80, "rsync"},
		{"an empty name falls back", "", 80, "rsync"},
		{"whitespace-only falls back", "   ", 80, "rsync"},
		{"truncation happens last", "abcdefghij", 4, "abcd"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := debeziumSafeName(tc.in, tc.maxLen); got != tc.want {
				t.Errorf("debeziumSafeName(%q, %d) = %q, want %q — this diverges from "+
					"_safe_name() in the Debezium connector, which means the orchestrator "+
					"pre-creates one topic and Connect writes to another",
					tc.in, tc.maxLen, got, tc.want)
			}
		})
	}
}

// TestSchemaHistoryTopicIsNamespaced pins the whole derived name, prefix included, at
// the default prefix, a custom one, and the empty (migration) one.
//
// The connector derives the same string as _qualify_topic(f"schemahistory.{_safe_name(
// connector_name, 80)}"), reading the same KAFKA_TOPIC_PREFIX out of its own
// environment. A prefix mismatch is the same silent two-topic failure as a name
// mismatch, and it is the likelier one, because the connector runs in a different
// container from the orchestrator.
func TestSchemaHistoryTopicIsNamespaced(t *testing.T) {
	cases := []struct {
		name      string
		prefixEnv *string // nil = variable unset
		connector string
		want      string
	}{
		{"default prefix when unset", nil, "cdc-3a7e63e5", "rsync.schemahistory.cdc-3a7e63e5"},
		{"explicit prefix", strptr("acme"), "cdc-3a7e63e5", "acme.schemahistory.cdc-3a7e63e5"},
		{"prefix already carrying a separator", strptr("acme."), "cdc-3a7e63e5", "acme.schemahistory.cdc-3a7e63e5"},
		{"empty prefix disables qualification", strptr(""), "cdc-3a7e63e5", "schemahistory.cdc-3a7e63e5"},
		{"dotted connector name is sanitized under the prefix", nil, "cdc.pipeline.7f2", "rsync.schemahistory.cdc_pipeline_7f2"},
		{"empty connector name still yields a legal topic", nil, "", "rsync.schemahistory.rsync"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTopicPrefix(t, tc.prefixEnv)
			if got := schemaHistoryTopicFor(tc.connector); got != tc.want {
				t.Errorf("schemaHistoryTopicFor(%q) = %q, want %q", tc.connector, got, tc.want)
			}
		})
	}
}

// TestSchemaHistoryTopicQualificationIsIdempotent — the name is passed to the
// connector, which qualifies whatever it is handed. Handing it an already-qualified
// name must not produce rsync.rsync.schemahistory.…
func TestSchemaHistoryTopicQualificationIsIdempotent(t *testing.T) {
	withTopicPrefix(t, nil)
	once := schemaHistoryTopicFor("cdc-3a7e63e5")
	if twice := kafkaclient.Topic(once); twice != once {
		t.Errorf("re-qualifying %q produced %q — the connector qualifies the name it is "+
			"given, so a name that is not already stable there becomes a second topic",
			once, twice)
	}
}

func strptr(s string) *string { return &s }

// withTopicPrefix sets or unsets KAFKA_TOPIC_PREFIX for the duration of a test.
// t.Setenv cannot express "unset", and unset is a distinct case here: it is the one
// that selects the "rsync." default rather than an operator-supplied value.
func withTopicPrefix(t *testing.T, v *string) {
	t.Helper()
	const key = "KAFKA_TOPIC_PREFIX"
	prev, had := os.LookupEnv(key)
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
		} else {
			_ = os.Unsetenv(key)
		}
	})
	if v == nil {
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := os.Setenv(key, *v); err != nil {
		t.Fatal(err)
	}
}

// TestSchemaHistoryTopicIsPreCreatedBeforeStartSync is the ordering half.
//
// After Connect has started, the connector writes its own history and the topic's
// geometry — partition count, cleanup.policy, retention — is already fixed at whatever
// the broker's defaults gave it. A pre-create that runs afterwards succeeds, logs
// nothing unusual, and leaves the exact defect it was written to close. The invariant
// is positional, so it is asserted positionally: parser positions cannot drift with
// re-indentation the way a source-text match can.
//
// The geometry is asserted here too, for the same reason it is a constant and not a
// preference:
//
//	partitions = 1     the history is replayed in order; more than one partition has
//	                   no total order and the connector refuses to start.
//	cleanup.policy     MUST be "delete". The records are not keyed per schema object,
//	                   so "compact" silently drops DDL that is still needed — and
//	                   "compact" is the plausible-looking wrong answer, since it is
//	                   what Kafka's other internal topics use.
//	retention.ms = -1  forever. Any finite retention moves the failure to the first
//	                   restart after expiry, which is days away from any change.
func TestSchemaHistoryTopicIsPreCreatedBeforeStartSync(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}

	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == "executeStreamingDataTransfer" {
			fn = f
			break
		}
	}
	if fn == nil {
		t.Fatal("executeStreamingDataTransfer not found in executor.go — if it moved or " +
			"was renamed, move this guard with it rather than deleting it")
	}

	// A call inside a closure runs wherever the closure is invoked, not where it is
	// written, so its written position proves nothing about ordering.
	var lits []*ast.FuncLit
	ast.Inspect(fn, func(n ast.Node) bool {
		if fl, ok := n.(*ast.FuncLit); ok {
			lits = append(lits, fl)
		}
		return true
	})
	inClosure := func(p token.Pos) bool {
		for _, fl := range lits {
			if p > fl.Body.Lbrace && p < fl.Body.Rbrace {
				return true
			}
		}
		return false
	}

	var ensurePos token.Pos
	var ensureCall *ast.CallExpr
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || inClosure(call.Pos()) {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "EnsureTopicExistsWithConfig" {
			return true
		}
		if !ensurePos.IsValid() {
			ensurePos = call.Pos()
			ensureCall = call
		}
		return true
	})

	// The start_sync request, located by the operation string rather than by the
	// variable it is assigned to.
	var startSyncPos token.Pos
	ast.Inspect(fn, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING || inClosure(lit.Pos()) {
			return true
		}
		if v, err := strconv.Unquote(lit.Value); err == nil && v == "start_sync" {
			if !startSyncPos.IsValid() {
				startSyncPos = lit.Pos()
			}
		}
		return true
	})

	// Non-vacuity. Either half missing makes the comparison below meaningless, and a
	// missing pre-create is the defect itself.
	if !startSyncPos.IsValid() {
		t.Fatal(`no "start_sync" operation found inside executeStreamingDataTransfer — ` +
			"this guard can no longer see the thing it orders against; re-point it rather " +
			"than deleting it")
	}
	if !ensurePos.IsValid() {
		t.Fatal("executeStreamingDataTransfer no longer pre-creates the Debezium " +
			"schema-history topic (no EnsureTopicExistsWithConfig call). Without it the " +
			"topic exists only because the broker auto-creates it, with the broker's " +
			"retention — and the pipeline fails on its first connector RESTART, days later, " +
			"with an error that names nothing about retention")
	}

	if ensurePos > startSyncPos {
		t.Errorf("the schema-history pre-create runs at %s, AFTER start_sync at %s.\n"+
			"Once Connect is up it writes the history topic itself, so the pre-create "+
			"finds it already there and returns success while the topic keeps the "+
			"broker's cleanup.policy and retention. The guard passes, the log is clean, "+
			"and the connector still dies on its first restart.",
			fset.Position(ensurePos), fset.Position(startSyncPos))
	}

	// The name must also be handed to the connector, or the connector derives its own
	// and the two implementations are free to drift.
	var handedOver bool
	ast.Inspect(fn, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || inClosure(assign.Pos()) {
			return true
		}
		for _, lhs := range assign.Lhs {
			idx, ok := lhs.(*ast.IndexExpr)
			if !ok {
				continue
			}
			key, ok := idx.Index.(*ast.BasicLit)
			if !ok || key.Kind != token.STRING {
				continue
			}
			if v, err := strconv.Unquote(key.Value); err == nil && v == "schema_history_topic" {
				if assign.Pos() < startSyncPos {
					handedOver = true
				}
			}
		}
		return true
	})
	if !handedOver {
		t.Error(`params["schema_history_topic"] is not set before start_sync — the ` +
			"connector then falls back to deriving the name itself, and the orchestrator's " +
			"pre-created topic and the one Connect writes to are only equal by coincidence")
	}

	// Geometry.
	if ensureCall != nil {
		if len(ensureCall.Args) != 3 {
			t.Fatalf("EnsureTopicExistsWithConfig called with %d args, want 3 (topic, "+
				"partitions, config)", len(ensureCall.Args))
		}
		if lit, ok := ensureCall.Args[1].(*ast.BasicLit); !ok || lit.Value != "1" {
			t.Errorf("the schema-history topic is created with partitions=%s, want 1 — "+
				"Debezium replays this topic in order and refuses to start against a "+
				"multi-partition history", exprText(ensureCall.Args[1]))
		}
		want := map[string]string{"cleanup.policy": "delete", "retention.ms": "-1"}
		got := map[string]string{}
		if cl, ok := ensureCall.Args[2].(*ast.CompositeLit); ok {
			for _, el := range cl.Elts {
				kv, ok := el.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				k, kerr := basicString(kv.Key)
				v, verr := basicString(kv.Value)
				if kerr == nil && verr == nil {
					got[k] = v
				}
			}
		}
		for k, w := range want {
			if got[k] != w {
				t.Errorf("schema-history topic config %q = %q, want %q.\n"+
					"cleanup.policy must be \"delete\": the records are not keyed per schema "+
					"object, so compaction drops DDL that is still needed. retention.ms must "+
					"be -1: any finite retention moves the failure to the first restart after "+
					"expiry.", k, got[k], w)
			}
		}
	}
}

// basicString unquotes an expression that must be a plain string literal.
func basicString(e ast.Expr) (string, error) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", fmt.Errorf("not a string literal")
	}
	return strconv.Unquote(lit.Value)
}

// exprText renders an expression back to source, for failure messages that need to
// name what was actually written rather than "something else".
func exprText(e ast.Expr) string {
	var b strings.Builder
	if err := printer.Fprint(&b, token.NewFileSet(), e); err != nil {
		return "<unprintable>"
	}
	return b.String()
}
