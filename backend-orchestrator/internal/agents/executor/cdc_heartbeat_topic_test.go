package executor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
	"github.com/rsync-ai/shared/kafkaclient"
)

// A MongoDB Debezium source now emits heartbeats (connector.py, _build_config
// mongodb branch). That is the fix for KI-CDC-MONGO-RESUME-TOKEN-SILENT-STALL: a
// heartbeat commits a FRESH resume token on a timer, so an idle pipeline's stored token
// never ages past the oplog window and dies on the next reconnect. It is also the
// liveness beacon the Sentinel's freshness watchdog measures against
// (sentinel/cdc_source_freshness.go).
//
// A heartbeat that cannot be published is worse than no heartbeat: the connector keeps
// the stale token AND the watchdog now has a frozen position it believes should be
// moving. Two things have to hold for it to be publishable, and neither is guaranteed
// by the broker:
//
//   - The topic must exist. Debezium publishes by produce and relies on auto-create,
//     which a customer-managed broker usually has off (KI-KAFKA-DATAPLANE-AUTOCREATE-ONLY).
//     So the orchestrator pre-creates it, exactly as it does the schema history.
//   - The name must fall inside the `rsync.*` grant a BYO-Kafka cluster gives us.
//     Debezium's own default prefix is `__debezium-heartbeat`, which an ACL scoped to
//     `rsync.*` refuses outright.
//
// Which makes this the same two-implementations-one-name problem the schema-history
// topic has, with two extra traps on top.
//
// ORDER. Debezium composes the heartbeat topic as
// <topic.heartbeat.prefix>.<topic.prefix> — PREFIX FIRST. The name reads as though the
// prefix were a suffix on the server name, and getting the order backwards produces a
// perfectly legal topic that the orchestrator creates and the connector never writes to.
//
// KEY. There are two heartbeat-prefix properties and only one of them names the topic.
// `heartbeat.topics.prefix` is a live, non-deprecated ConfigDef entry
// (io/debezium/heartbeat/Heartbeat.java), so setting only it validates cleanly and looks
// right; the name is actually built by the topic-naming strategy
// (io/debezium/schema/AbstractTopicNamingStrategy.java) from `topic.heartbeat.prefix`,
// default `__debezium-heartbeat`. #1098 set only the first, and the heartbeat landed on
// `__debezium-heartbeat.<topic.prefix>` while the topic below stayed at offset 0 for
// three days (KI-CDC-HEARTBEAT-TOPIC-PREFIX-KEY-IGNORED, observed live 2026-09-20).
//
// Both are pinned here, and the KEY half is pinned against connector.py itself — a Go
// helper cannot observe which property the connector writes.

// paramsWith builds the executor's CDC params map for the naming helpers.
func paramsWith(kv map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range kv {
		out[k] = v
	}
	return out
}

// TestDebeziumTopicPrefixMatchesTheConnectorsRule locks the Go prediction of Debezium's
// topic.prefix to connector.py:825:
//
//	topic_prefix = _qualify_topic(str(args.get("topic_prefix") or connector_name).strip() or connector_name)
//
// The trap here is the opposite of the schema-history one. That name runs through
// _safe_name, which turns a dot into an underscore; this one does NOT — a dotted
// connector name stays dotted in topic.prefix. Applying debeziumSafeName here "for
// consistency" would produce a heartbeat topic under a server name Debezium never uses.
func TestDebeziumTopicPrefixMatchesTheConnectorsRule(t *testing.T) {
	cases := []struct {
		name   string
		params map[string]interface{}
		want   string
	}{
		{
			"connector_name is the fallback",
			paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5"}),
			"rsync.cdc-3a7e63e5",
		},
		{
			"an explicit topic_prefix wins",
			paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5", "topic_prefix": "orders"}),
			"rsync.orders",
		},
		{
			"a blank topic_prefix falls back to connector_name",
			paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5", "topic_prefix": "   "}),
			"rsync.cdc-3a7e63e5",
		},
		// NOT debeziumSafeName: the dot survives, unlike in the schema-history name.
		{
			"dots survive — this name is not run through _safe_name",
			paramsWith(map[string]interface{}{"connector_name": "cdc.pipeline.7f2"}),
			"rsync.cdc.pipeline.7f2",
		},
		{
			"an already-qualified prefix is not qualified twice",
			paramsWith(map[string]interface{}{"connector_name": "rsync.cdc-3a7e63e5"}),
			"rsync.cdc-3a7e63e5",
		},
		{
			"no name at all yields no prefix, so no topic is created",
			paramsWith(nil),
			"",
		},
		{
			"a non-string connector_name is not coerced into a topic name",
			paramsWith(map[string]interface{}{"connector_name": 42}),
			"",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTopicPrefix(t, nil)
			if got := debeziumTopicPrefixFor(tc.params); got != tc.want {
				t.Errorf("debeziumTopicPrefixFor(%v) = %q, want %q — this diverges from "+
					"connector.py:825, so the orchestrator pre-creates a heartbeat topic "+
					"under one server name and Debezium heartbeats under another",
					tc.params, got, tc.want)
			}
		})
	}
}

// TestHeartbeatTopicPutsThePrefixFirst is the ordering assertion.
//
// Debezium: <topic.heartbeat.prefix>.<topic.prefix>. Reversing it yields a legal,
// plausible-looking topic name, so nothing downstream complains — the orchestrator
// creates rsync.cdc-3a7e63e5.rsync.heartbeat, Debezium produces to
// rsync.heartbeat.rsync.cdc-3a7e63e5, and on a broker with auto-create off the
// heartbeat is simply never published. The resume token then ages out exactly as it did
// before the fix, while every status surface reports the fix is in place.
func TestHeartbeatTopicPutsThePrefixFirst(t *testing.T) {
	withTopicPrefix(t, nil)
	got := heartbeatTopicFor(debeziumTopicPrefixFor(
		paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5"})))
	const want = "rsync.heartbeat.rsync.cdc-3a7e63e5"
	if got != want {
		t.Errorf("heartbeat topic = %q, want %q — Debezium composes it as "+
			"<topic.heartbeat.prefix>.<topic.prefix>, prefix FIRST", got, want)
	}
	if strings.HasPrefix(got, "rsync.cdc-3a7e63e5") {
		t.Errorf("heartbeat topic = %q — the server name comes first, which is the "+
			"reversed composition: Debezium will publish somewhere else entirely", got)
	}
}

// TestHeartbeatTopicIsNamespaced pins the whole name at the default prefix, a custom
// one, and the empty (migration) one.
//
// The prefix is what keeps this topic inside the `rsync.*` ACL a BYO-Kafka cluster
// grants. A connector running in a different container resolves KAFKA_TOPIC_PREFIX from
// its own environment, so both halves are asserted against the same variable here.
func TestHeartbeatTopicIsNamespaced(t *testing.T) {
	cases := []struct {
		name       string
		prefixEnv  *string // nil = variable unset
		params     map[string]interface{}
		wantPrefix string
		wantTopic  string
	}{
		{
			"default prefix when unset",
			nil,
			paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5"}),
			"rsync.heartbeat",
			"rsync.heartbeat.rsync.cdc-3a7e63e5",
		},
		{
			"explicit prefix",
			strptr("acme"),
			paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5"}),
			"acme.heartbeat",
			"acme.heartbeat.acme.cdc-3a7e63e5",
		},
		{
			"prefix already carrying a separator",
			strptr("acme."),
			paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5"}),
			"acme.heartbeat",
			"acme.heartbeat.acme.cdc-3a7e63e5",
		},
		{
			"empty prefix disables qualification",
			strptr(""),
			paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5"}),
			"heartbeat",
			"heartbeat.cdc-3a7e63e5",
		},
		{
			"no server name means no heartbeat topic",
			nil,
			paramsWith(nil),
			"rsync.heartbeat",
			"",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTopicPrefix(t, tc.prefixEnv)
			if got := heartbeatTopicsPrefix(); got != tc.wantPrefix {
				t.Errorf("heartbeatTopicsPrefix() = %q, want %q — this value is written "+
					"into the connector config as topic.heartbeat.prefix, and outside "+
					"the product namespace a BYO-Kafka ACL refuses it",
					got, tc.wantPrefix)
			}
			if got := heartbeatTopicFor(debeziumTopicPrefixFor(tc.params)); got != tc.wantTopic {
				t.Errorf("heartbeat topic = %q, want %q", got, tc.wantTopic)
			}
		})
	}
}

// TestHeartbeatTopicQualificationIsIdempotent — the composed name is handed to
// kafkaclient-aware call sites and to the admin client. Qualifying it again must not
// produce rsync.rsync.heartbeat.…
func TestHeartbeatTopicQualificationIsIdempotent(t *testing.T) {
	withTopicPrefix(t, nil)
	once := heartbeatTopicFor(debeziumTopicPrefixFor(
		paramsWith(map[string]interface{}{"connector_name": "cdc-3a7e63e5"})))
	if twice := kafkaclient.Topic(once); twice != once {
		t.Errorf("re-qualifying %q produced %q", once, twice)
	}
}

// TestHeartbeatTopicIsPreCreatedBeforeStartSync is the ordering-and-gating half,
// asserted positionally for the same reason the schema-history guard is: parser
// positions survive re-indentation, source-text matching does not.
//
// Three things are pinned:
//
//   - The pre-create runs before start_sync. Afterwards the connector is already up and
//     producing, so on an auto-creating broker the topic exists with the broker's
//     geometry and the pre-create silently succeeds against it.
//   - It is gated on MongoDB, because the CONNECTOR gates heartbeats on MongoDB. The
//     two gates have to name the same sources: widen one without the other and either a
//     source heartbeats into a topic nobody created, or a topic is created that nothing
//     ever writes to.
//   - heartbeat_topics_prefix is handed to the connector rather than re-derived there,
//     the same anti-drift rule as schema_history_topic.
func TestHeartbeatTopicIsPreCreatedBeforeStartSync(t *testing.T) {
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
	inClosure := closureFilter(fn)

	ensurePos, ensureCall := findEnsureTopicCall(fn, inClosure, "hbTopic")

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

	// Non-vacuity. Either half missing makes the comparison meaningless, and a missing
	// pre-create is the defect itself.
	if !startSyncPos.IsValid() {
		t.Fatal(`no "start_sync" operation found inside executeStreamingDataTransfer — ` +
			"this guard can no longer see the thing it orders against; re-point it rather " +
			"than deleting it")
	}
	if !ensurePos.IsValid() {
		t.Fatal("executeStreamingDataTransfer no longer pre-creates the Debezium " +
			"heartbeat topic (no EnsureTopicExistsWithConfig call taking hbTopic). " +
			"Without it the topic exists only if the broker auto-creates it — and where " +
			"it does not, the MongoDB connector cannot refresh its resume token while " +
			"the source is idle, which is the whole of " +
			"KI-CDC-MONGO-RESUME-TOKEN-SILENT-STALL")
	}

	if ensurePos > startSyncPos {
		t.Errorf("the heartbeat pre-create runs at %s, AFTER start_sync at %s. "+
			"Once Connect is up the connector is already heartbeating, so on a broker "+
			"that auto-creates, the pre-create finds the topic there and returns success; "+
			"on one that does not, the heartbeats were already lost.",
			fset.Position(ensurePos), fset.Position(startSyncPos))
	}

	// The prefix must reach the connector before start_sync, or the connector derives
	// its own and the two namings are equal only by coincidence.
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
			if v, err := strconv.Unquote(key.Value); err == nil && v == "heartbeat_topics_prefix" {
				if assign.Pos() < startSyncPos {
					handedOver = true
				}
			}
		}
		return true
	})
	if !handedOver {
		t.Error(`params["heartbeat_topics_prefix"] is not set before start_sync — the ` +
			"connector then falls back to its own default, and the topic the orchestrator " +
			"created is not the one Debezium publishes to")
	}

	// The MongoDB gate: some enclosing condition must name the source type the connector
	// enables heartbeats for.
	var gated bool
	ast.Inspect(fn, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Cond == nil || ifs.Body == nil {
			return true
		}
		if ensurePos <= ifs.Body.Lbrace || ensurePos >= ifs.Body.Rbrace {
			return true
		}
		if strings.Contains(strings.ToLower(exprText(ifs.Cond)), "mongodb") {
			gated = true
		}
		return true
	})
	if !gated {
		t.Error("the heartbeat pre-create is not inside a MongoDB-gated branch. The " +
			"connector enables heartbeats for MongoDB sources only (connector.py, " +
			"_build_config mongodb branch); the two gates must name the same " +
			"set of sources, or a source heartbeats into a topic nobody created — or a " +
			"topic is created that nothing ever writes to. If heartbeats were " +
			"deliberately widened to another source family, widen the connector and this " +
			"assertion together.")
	}

	// Geometry. Deliberately NOT the schema history's: nothing replays this topic.
	if ensureCall != nil {
		if len(ensureCall.Args) != 3 {
			t.Fatalf("EnsureTopicExistsWithConfig called with %d args, want 3 (topic, "+
				"partitions, config)", len(ensureCall.Args))
		}
		if lit, ok := ensureCall.Args[1].(*ast.BasicLit); !ok || lit.Value != "1" {
			t.Errorf("the heartbeat topic is created with partitions=%s, want 1 — one "+
				"connector produces one ordered stream of liveness ticks",
				exprText(ensureCall.Args[1]))
		}
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
		if got["cleanup.policy"] != "delete" {
			t.Errorf("heartbeat topic cleanup.policy = %q, want \"delete\" — the records "+
				"are not keyed, so compaction is meaningless here", got["cleanup.policy"])
		}
		if v, ok := got["retention.ms"]; ok && v == "-1" {
			t.Error("the heartbeat topic is set to retention.ms=-1. A heartbeat is a " +
				"liveness tick, worthless once read, and unlike the schema history " +
				"nothing ever replays it — retaining every tick forever grows without " +
				"bound for the life of the pipeline.")
		}
	}
}

// debeziumConnectorSource returns connector.py from the Debezium connector's CURRENT
// version directory, resolved through the shared resolver rather than by guessing a
// version — versions/<current_version>/ is the Docker build context, so it is the code
// that actually runs (CLAUDE.md, "MCP connector changes").
func debeziumConnectorSource(t *testing.T) (string, string) {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	toolsDir := ""
	for d := cwd; ; {
		c := filepath.Join(d, "shared", "mcp-connectors")
		if info, statErr := os.Stat(c); statErr == nil && info.IsDir() {
			toolsDir = c
			break
		}
		parent := filepath.Dir(d)
		if parent == d {
			break
		}
		d = parent
	}
	if toolsDir == "" {
		t.Fatalf("no shared/mcp-connectors above %s — this guard cannot silently pass "+
			"just because it could not find the file it guards", cwd)
	}
	root := filepath.Join(toolsDir, "internal", "debezium")
	cv, ok := connectorpaths.ResolveCurrentVersion(root)
	if !ok {
		t.Fatalf("cannot resolve current_version from %s/latest.json", root)
	}
	path := filepath.Join(root, "versions", cv, "connector.py")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return path, string(data)
}

// TestConnectorWritesTheKeyDebeziumNamesTheTopicFrom is the KEY half of this file's
// contract, and the assertion whose absence let KI-CDC-HEARTBEAT-TOPIC-PREFIX-KEY-IGNORED
// ship green.
//
// Every other test here asserts what the ORCHESTRATOR computes. None of them can see
// which Debezium property the connector writes that value to, and that is the half that
// was wrong: #1098 set `heartbeat.topics.prefix` (a real ConfigDef entry, but vestigial
// for naming) and never `topic.heartbeat.prefix` (what AbstractTopicNamingStrategy reads,
// default `__debezium-heartbeat`). The orchestrator dutifully created
// rsync.heartbeat.rsync.cdc-<id> and Debezium published to __debezium-heartbeat.rsync.cdc-<id>
// — the exact split-brain the rest of this file exists to prevent, one layer down.
//
// On a BYO-Kafka cluster granting only `rsync.*` the unqualified topic is outside the
// prefixed Write/Describe ACL, and with errors.tolerance=none the refused produce fails
// the task: the heartbeat fix kills CDC on precisely the deployment it was written for.
func TestConnectorWritesTheKeyDebeziumNamesTheTopicFrom(t *testing.T) {
	path, src := debeziumConnectorSource(t)

	if !strings.Contains(src, `"topic.heartbeat.prefix"`) {
		t.Errorf("%s never sets topic.heartbeat.prefix. Debezium will name the heartbeat "+
			"topic __debezium-heartbeat.<topic.prefix>, not the %q-prefixed topic this "+
			"package pre-creates and ACL-grants; setting only heartbeat.topics.prefix "+
			"validates cleanly and does nothing.", path, heartbeatTopicsPrefix())
	}

	// Both keys must carry the SAME value. Whichever one a given Debezium honours, a
	// disagreement names a topic nothing created — the failure this file is about.
	if strings.Contains(src, `"heartbeat.topics.prefix"`) &&
		!strings.Contains(src, `cfg.setdefault("topic.heartbeat.prefix", _hb_prefix)`) {
		t.Errorf("%s sets both heartbeat-prefix keys from different expressions; they "+
			"must be assigned one shared value so they cannot drift apart", path)
	}
}
