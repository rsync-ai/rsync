package executor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// The sink dependency row must record WHICH PHASE the sink start was for.
//
// A hybrid-CDC pipeline registers two kafka_sink_worker rows under one pipeline_id: the
// batch-backfill sink ("sink-<pid8>-batch") and the CDC streaming sink ("sink-<pid8>").
// Both carry sink_mode "cdc" and start_offset "earliest", so neither of the other two
// metadata keys separates them. handlers.sinkConsumerGroupQuery breaks that tie on
// metadata->>'backfill', wrapped in COALESCE(..., 'false') so that rows predating the key
// keep today's behaviour. That COALESCE is also what makes the omission SILENT: drop this
// key and every row reads as non-backfill, the tiebreak degrades to a permanent no-op,
// and a CDC sink restart landing in the backfill phase hijacks the running backfill
// worker again — with no error anywhere, because the resolver still returns a real group.
//
// The scan is over the SOURCE rather than over behaviour for the same reason as
// cmd/orchestrator/sink_group_derivation_test.go: registerSinkWorker's only effect is a
// swallowed-error INSERT (dependency_manifest.go), so a missing key and a present one are
// indistinguishable from outside this package. What separates them is the text, and
// specifically WHERE the value came from — a recomputed phase, or a "-batch" suffix test
// on the consumer group, would type-check and pass a value-only assertion while
// reintroducing the second copy of the naming rule that handlers/sink_consumer_group.go
// exists to eliminate. So the identifier itself is asserted, not merely the key.
func TestSinkWorkerManifestRecordsTheBackfillDiscriminator(t *testing.T) {
	const (
		file    = "executor.go"
		wantKey = "backfill"
		wantVal = "isBatchBackfillTopic"
	)

	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}

	// Positive denominator: a walk that silently matches nothing looks exactly like a
	// pass. Count what each stage found and fail loudly if any stage found zero.
	var (
		foundAssign bool
		maps        int
		keyValue    ast.Expr
		keyPresent  bool
	)

	ast.Inspect(parsed, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 {
			return true
		}
		ident, ok := assign.Lhs[0].(*ast.Ident)
		if !ok || ident.Name != "registerSinkWorker" {
			return true
		}
		foundAssign = true

		// Inside the closure, find the map[string]interface{} literal handed to
		// upsertDependency and read its keys.
		ast.Inspect(assign, func(inner ast.Node) bool {
			lit, ok := inner.(*ast.CompositeLit)
			if !ok {
				return true
			}
			mapType, ok := lit.Type.(*ast.MapType)
			if !ok {
				return true
			}
			if keyIdent, ok := mapType.Key.(*ast.Ident); !ok || keyIdent.Name != "string" {
				return true
			}
			maps++
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				k, ok := kv.Key.(*ast.BasicLit)
				if !ok || k.Kind != token.STRING {
					continue
				}
				name, err := strconv.Unquote(k.Value)
				if err != nil || name != wantKey {
					continue
				}
				keyPresent = true
				keyValue = kv.Value
			}
			return true
		})
		return true
	})

	if !foundAssign {
		t.Fatal("no `registerSinkWorker := func() {` assignment found in executor.go — the " +
			"walk is broken, so a green result here would mean nothing. Re-point it at " +
			"whatever the sink-registration closure is called now before trusting this test.")
	}
	if maps == 0 {
		t.Fatal("registerSinkWorker contains no map[string]... composite literal — the walk " +
			"is broken, so a green result here would mean nothing.")
	}

	if !keyPresent {
		t.Fatalf("the sink dependency metadata in %s registerSinkWorker has no %q key. Without "+
			"it the tiebreak in handlers/sink_consumer_group.go is a permanent no-op: "+
			"COALESCE(metadata->>'backfill', 'false') makes EVERY row look non-backfill, so a "+
			"CDC sink restart during the backfill phase resolves to the ...-batch group and "+
			"hijacks the running backfill worker. Add `%q: %s,` to the map.",
			file, wantKey, wantKey, wantVal)
	}

	got, ok := keyValue.(*ast.Ident)
	if !ok || got.Name != wantVal {
		t.Fatalf("metadata[%q] is not the bare identifier %s (got %T at %s). It must reuse the "+
			"value computed once near the consumer-group selection — not a recomputation from "+
			"the topic, not a string literal, and not a \"-batch\" suffix test on the consumer "+
			"group, each of which puts a second copy of the naming rule in play that "+
			"handlers/sink_consumer_group.go exists to eliminate.",
			wantKey, wantVal, keyValue, fset.Position(keyValue.Pos()))
	}
}
