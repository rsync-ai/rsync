package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A sink's consumer group id must be READ from the dependency manifest, never
// re-derived from the pipeline id.
//
// This package used to build it inline as fmt.Sprintf("sink-%s", first8(pipelineID)),
// which was wrong in two independent ways and silent in both:
//
//   - Unqualified. The executor mints the group through kafkaclient.Group
//     (internal/agents/executor/sink_consumer_group.go), so the group that exists on
//     the broker is "rsync.sink-<pid8>" even at the DEFAULT prefix. The re-derived id
//     therefore misses at every prefix, not only a custom one.
//   - Shape-guessing. Only CDC sinks are "sink-<pid8>". Batch and streaming_only
//     sinks are "-batch" / "-stream" / "-<eid8>", so the derived id named a group
//     that had never existed for any of them.
//
// Neither miss reports anything: Manager.GetConsumerGroupLag hands the id straight to
// sarama, and a group that never committed comes back as an empty map rather than an
// error, so the metrics simply stay nil and the UI renders the pipeline as idle.
//
// The scan is over the SOURCE rather than over behavior because the failure has no
// behavior to observe -- the wrong id and the right one both return without error.
// What distinguishes them is where the id came from, and that is a property of the
// text.
func TestSinkConsumerGroupIsNotRederivedHere(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}

	scanned := 0
	var offenders []string
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		scanned++

		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			// "sink-%s" and friends: a format or concatenation fragment that is
			// building the id. A comment mentioning the old shape is not a literal,
			// so the explanation above this test does not trip it.
			if strings.HasPrefix(s, "sink-") {
				pos := fset.Position(lit.Pos())
				offenders = append(offenders,
					filepath.Base(pos.Filename)+":"+strconv.Itoa(pos.Line)+"  "+strconv.Quote(s))
			}
			return true
		})
	}

	if scanned == 0 {
		t.Fatal("scanned no non-test source files in this package -- the walk is broken, " +
			"so a green result here would mean nothing")
	}

	if len(offenders) > 0 {
		t.Fatalf("%d sink consumer group id(s) are built from a literal in this package:\n  %s\n\n"+
			"Read the group from the manifest instead -- handlers.ResolveSinkConsumerGroup(ctx, db, "+
			"pipelineID) returns the id the sink actually registered in pipeline_dependencies "+
			"(kind='kafka_sink_worker'), which is authoritative by construction. A re-derived id is "+
			"unqualified AND assumes the CDC shape, and Kafka answers both mistakes with an empty "+
			"result rather than an error.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}
