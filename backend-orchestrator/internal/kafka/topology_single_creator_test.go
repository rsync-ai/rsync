package kafka

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// There must be exactly one function in this service that asks Kafka to create a
// topic, and this test is what keeps it that way.
//
// The reason is not tidiness. Topic creation carries a policy — a replication factor
// clamped to the live broker count, and an explicit min.insync.replicas at or below
// it — and the cost of that policy being absent is invisible: a topic created RF=1
// against a broker whose default min.insync.replicas is 2 (MSK's, and most managed
// clusters') is created successfully, appears in ListTopics, accepts a sink
// subscription, and then rejects every acks=all produce with NOT_ENOUGH_REPLICAS
// forever. The pipeline reports running and moves zero rows, and nothing in any log
// names the replication factor.
//
// This service had THREE hand-rolled sarama.TopicDetail builders. The policy was
// added to one of them. Its unit tests were green the whole time, because a test that
// calls the fixed path cannot see the two that were not fixed. Counting the creation
// sites in the source is the only assertion that can.
//
// It reads the source rather than the running code on purpose: the failure mode is a
// NEW call site added later, and no runtime assertion can observe a path nobody
// invoked.

// theOnlyCreator is the single function permitted to call ClusterAdmin.CreateTopic.
const theOnlyCreator = "ensureTopicLocked"

// partitionGrowers may call ClusterAdmin.CreatePartitions. Growing a topic re-hashes
// every key onto a different partition, so it is a data-ordering change, not a
// capacity knob — it stays where it can be reviewed.
var partitionGrowers = map[string]bool{
	theOnlyCreator: true,
	// The explicit operator-facing route, reached only from an admin endpoint.
	"UpdatePartitions": true,
}

type creationSite struct {
	pos     string
	fn      string
	call    string
	problem string
}

func TestOnlyOneFunctionCreatesKafkaTopics(t *testing.T) {
	root := serviceRoot(t)

	var violations []creationSite
	sawTheCreator := false

	walkServiceSource(t, root, func(path string, fset *token.FileSet, file *ast.File) {
		var fnStack []string

		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncDecl:
				fnStack = append(fnStack, node.Name.Name)
			case *ast.FuncLit:
				// Keep the enclosing named function as the attribution.
				if len(fnStack) > 0 {
					fnStack = append(fnStack, fnStack[len(fnStack)-1])
				} else {
					fnStack = append(fnStack, "<closure>")
				}
			case *ast.CallExpr:
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				enclosing := "<file scope>"
				if len(fnStack) > 0 {
					enclosing = fnStack[len(fnStack)-1]
				}
				where := creationSite{
					pos:  relPos(root, fset, node.Pos()),
					fn:   enclosing,
					call: sel.Sel.Name,
				}

				switch sel.Sel.Name {
				case "CreateTopic":
					// Two different CreateTopics exist. sarama's ClusterAdmin takes
					// (name, detail, validateOnly); TopologyManager's own method takes
					// (ctx, TopicConfig) and is a legitimate front door that funnels
					// here, so calling it from anywhere is fine.
					if len(node.Args) != 3 {
						return true
					}
					if enclosing == theOnlyCreator {
						sawTheCreator = true
						return true
					}
					where.problem = fmt.Sprintf(
						"calls ClusterAdmin.CreateTopic directly, so the replication-factor "+
							"clamp and the explicit %s in %s do not apply to it. Route it "+
							"through TopologyManager.EnsureTopic instead",
						minInsyncReplicasKey, theOnlyCreator)
					violations = append(violations, where)

				case "CreateTopics":
					where.problem = "uses the plural CreateTopics, which bypasses the single " +
						"creation path entirely and reports per-topic errors nobody here reads"
					violations = append(violations, where)

				case "CreatePartitions":
					if partitionGrowers[enclosing] {
						return true
					}
					where.problem = "grows an existing topic's partition count. Kafka maps a " +
						"key to a partition modulo that count, so this silently re-routes every " +
						"existing key and breaks per-key CDC ordering with no error anywhere"
					violations = append(violations, where)
				}
			}
			return true
		})

		// Popping is unnecessary for attribution accuracy at this granularity: the
		// stack only ever grows within a file walk, and the last entry is always the
		// nearest enclosing function, which is what the message needs.
		_ = fnStack
	})

	// Violations first: when the choke point does not exist yet, the useful output is
	// the list of paths that would have to move into it, not the absence itself.
	if len(violations) > 0 {
		sort.Slice(violations, func(i, j int) bool { return violations[i].pos < violations[j].pos })
		var b strings.Builder
		fmt.Fprintf(&b, "%d topic-creation path(s) bypass %s:\n", len(violations), theOnlyCreator)
		for _, v := range violations {
			fmt.Fprintf(&b, "  %s  (in %s) %s: %s\n", v.pos, v.fn, v.call, v.problem)
		}
		t.Fatal(b.String())
	}

	// A green result must mean "one creator", never "no creator". If the scan stops
	// finding the creation call at all, this test has quietly stopped asserting
	// anything — the same silence it exists to prevent.
	if !sawTheCreator {
		t.Fatalf("found no ClusterAdmin.CreateTopic call inside %s at all — either it was "+
			"renamed (update theOnlyCreator) or topic creation moved somewhere this test "+
			"is no longer watching, which is the exact drift this test exists to catch",
			theOnlyCreator)
	}
}

// A new creation path does not have to call CreateTopic to be a new creation path —
// building the request is the tell, and it is what a copy-paste of the old code would
// bring with it. sarama.TopicDetail is the request.
func TestOnlyOneFunctionBuildsATopicCreationRequest(t *testing.T) {
	root := serviceRoot(t)
	var violations []creationSite

	walkServiceSource(t, root, func(path string, fset *token.FileSet, file *ast.File) {
		var fnStack []string
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncDecl:
				fnStack = append(fnStack, node.Name.Name)
			case *ast.FuncLit:
				if len(fnStack) > 0 {
					fnStack = append(fnStack, fnStack[len(fnStack)-1])
				} else {
					fnStack = append(fnStack, "<closure>")
				}
			case *ast.CompositeLit:
				sel, ok := node.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "TopicDetail" {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "sarama" {
					return true
				}
				enclosing := "<file scope>"
				if len(fnStack) > 0 {
					enclosing = fnStack[len(fnStack)-1]
				}
				if enclosing == theOnlyCreator {
					return true
				}
				violations = append(violations, creationSite{
					pos: relPos(root, fset, node.Pos()),
					fn:  enclosing,
					problem: fmt.Sprintf("builds a sarama.TopicDetail outside %s. Every topic "+
						"this service creates must get its NumPartitions, ReplicationFactor and "+
						"%s from the one place that reconciles them with the live cluster",
						theOnlyCreator, minInsyncReplicasKey),
				})
			}
			return true
		})
	})

	if len(violations) > 0 {
		sort.Slice(violations, func(i, j int) bool { return violations[i].pos < violations[j].pos })
		var b strings.Builder
		fmt.Fprintf(&b, "%d topic-creation request(s) built outside %s:\n", len(violations), theOnlyCreator)
		for _, v := range violations {
			fmt.Fprintf(&b, "  %s  (in %s): %s\n", v.pos, v.fn, v.problem)
		}
		t.Fatal(b.String())
	}
}

// serviceRoot locates backend-orchestrator/ from this package's directory, so the
// scan covers the whole service rather than only the package that happens to hold
// the creator today — a bypass added in internal/agents is exactly as costly.
func serviceRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolving service root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("expected the service root at %s to hold go.mod: %v", root, err)
	}
	return root
}

func walkServiceSource(t *testing.T, root string, visit func(string, *token.FileSet, *ast.File)) {
	t.Helper()
	walkServiceSourceMin(t, root, 50, visit)
}

// walkServiceSourceMin is walkServiceSource with the non-vacuity floor supplied by the
// caller, for scanning a service smaller than this one. The floor is the whole point of
// the helper -- a walk that reached nothing would make its caller pass forever -- so it
// is a required argument rather than an option with a default.
func walkServiceSourceMin(t *testing.T, root string, minFiles int, visit func(string, *token.FileSet, *ast.File)) {
	t.Helper()
	fset := token.NewFileSet()
	scanned := 0

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "vendor", "node_modules", ".git", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("parsing %s: %w", path, err)
		}
		scanned++
		visit(path, fset, file)
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	// A walk that silently matched nothing would make this test pass forever.
	if scanned < minFiles {
		t.Fatalf("only %d non-test .go files scanned under %s (want >= %d) — the walk is not "+
			"reaching the service, so this test proves nothing", scanned, root, minFiles)
	}
}

func relPos(root string, fset *token.FileSet, p token.Pos) string {
	pos := fset.Position(p)
	if rel, err := filepath.Rel(root, pos.Filename); err == nil {
		return fmt.Sprintf("%s:%d", rel, pos.Line)
	}
	return fmt.Sprintf("%s:%d", pos.Filename, pos.Line)
}
