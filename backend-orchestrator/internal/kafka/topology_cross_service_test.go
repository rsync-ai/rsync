package kafka

import (
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// The control-plane topology is provisioned by THIS service, but it is not only this
// service that produces to it. api-gateway and backend-temporal-adapter both write to
// topics created here, and neither of them creates a topic of its own.
//
// That split is deliberate — one creator is the invariant topology_single_creator_test.go
// exists to hold — but it leaves a gap that the sibling repos cannot close themselves: a
// new produce target added over there is provisioned by nobody, and nothing in either
// module can notice, because neither module knows what this one creates. It is the same
// dependency-on-auto-create that TestEveryTopicWeProduceToIsProvisioned closes for this
// service, one module boundary away, and it fails the same silent way: on a broker with
// auto.create.topics.enable off the produce is rejected, and on one with it on the topic
// is born with the BROKER's min.insync.replicas — created, listed, subscribable and
// permanently unwritable.
//
// So the check runs from the side that holds the answer. This test reads the sibling
// services' produce sites out of their source and checks them against the set
// EnsureAgentControlTopics actually creates.
//
// CAPABILITIES.md recorded this gap in as many words — "Neither of those two services is
// covered by the orchestrator's source-scanning guard test" — which is what this closes.

// crossServiceProducers are the Go services that produce to topics provisioned here.
//
// minFiles is each module's own non-vacuity floor: a walk that stopped reaching a
// service would otherwise leave this test passing forever while proving nothing. The
// numbers are set below the current counts (115 and 24 non-test .go files) with room to
// delete, and far enough above zero to catch a walk that broke.
var crossServiceProducers = []struct {
	dir      string
	minFiles int
}{
	{dir: "api-gateway", minFiles: 80},
	{dir: "backend-temporal-adapter", minFiles: 15},
}

// knownUncoveredSiblingProduceTargets is the reviewed-exception list, empty by design.
// An entry here asserts that a topic a sibling service produces to may depend on a
// broker setting this platform does not own on a customer-managed cluster, which needs
// an argument in writing rather than a blank line.
var knownUncoveredSiblingProduceTargets = map[string]string{}

func TestSiblingServiceProduceTargetsAreProvisioned(t *testing.T) {
	provisioned := provisionedControlPlaneTopics(t)

	for _, svc := range crossServiceProducers {
		t.Run(svc.dir, func(t *testing.T) {
			root := siblingServiceRoot(t, svc.dir)
			targets := siblingProduceTargets(t, root, svc.minFiles)

			if len(targets) == 0 {
				t.Fatalf("found no produce targets in %s — the scan is broken, so a green "+
					"result here would mean nothing", svc.dir)
			}

			var missing []string
			for topicName, sites := range targets {
				qualified := kafkaclient.Topic(topicName)
				if provisioned[qualified] {
					continue
				}
				if _, known := knownUncoveredSiblingProduceTargets[topicName]; known {
					continue
				}
				missing = append(missing, fmt.Sprintf("%s  (produced at %s)",
					qualified, strings.Join(sites, ", ")))
			}

			if len(missing) > 0 {
				sort.Strings(missing)
				t.Fatalf("%d topic(s) are produced to by %s but created by no service, so they "+
					"exist only if the broker's auto.create.topics.enable is on — a setting this "+
					"platform does not own on a customer-managed cluster, and one that leaves the "+
					"topic carrying the broker's own min.insync.replicas when it is on:\n  %s\n\n"+
					"Add them to EnsureAgentControlTopics (topology.go), or record why not in "+
					"knownUncoveredSiblingProduceTargets.",
					len(missing), svc.dir, strings.Join(missing, "\n  "))
			}
		})
	}
}

// The scan must keep seeing the produce sites that exist today. A refactor that moved
// them to a shape the matcher does not recognise would empty the result set, and an
// empty result set is indistinguishable from a clean one.
func TestSiblingProduceScanSeesTheKnownSites(t *testing.T) {
	want := map[string][]string{
		"api-gateway": {
			"rsync.healer.approved-changes", // handlers/schema_evolution.go, SendPipelineRequest
			"pii.scan.request",              // handlers/pii.go, SendAgentMessage
		},
		"backend-temporal-adapter": {
			"pipeline.domain.events", // workflows/activities.go, sarama.ProducerMessage
			"pipeline.failed.dlq",
			"agent.failed.dlq",
		},
	}

	for _, svc := range crossServiceProducers {
		got := siblingProduceTargets(t, siblingServiceRoot(t, svc.dir), svc.minFiles)
		for _, topicName := range want[svc.dir] {
			if _, ok := got[topicName]; !ok {
				t.Errorf("%s: scan no longer sees a produce to %q — the matcher has gone "+
					"blind to a shape that is still in the source", svc.dir, topicName)
			}
		}
	}
}

// A consumer subscription is not a produce. event_projector.go reads
// pipeline.domain.events through a kafka.ReaderConfig whose field is also named Topic,
// and main.go subscribes to the planner/PII response topics. Counting either as a
// produce target would turn TestSiblingServiceProduceTargetsAreProvisioned into a
// standing false alarm, which is how a guard gets deleted.
//
// This is the produce-target scanner's only NEGATIVE control, so it has to be
// impossible to empty by accident. It used to name agent.resolver.progress,
// agent.orchestrator.progress and task.results — three literals the WebSocket bridge
// carried until the producer-less subscriptions were pruned, after which this test
// would have passed because the strings were GONE rather than because the scanner was
// right. Hence the non-vacuity half below: every fixture name must still be present in
// api-gateway's source as a kafkaclient.Topic/Topics argument before its absence from
// the produce set means anything.
func TestSiblingProduceScanIgnoresConsumerSubscriptions(t *testing.T) {
	root := siblingServiceRoot(t, "api-gateway")
	produced := siblingProduceTargets(t, root, 80)
	all := siblingTopicLiterals(t, root, 80)

	consumeOnly := []string{
		"pipeline.domain.events",   // projector/event_projector.go ReaderConfig, handlers/domain_events.go Consume, websocket/kafka_bridge.go
		"agent.planner.responses",  // cmd/server/main.go, consumer subscription
		"pii.scan.response",        // cmd/server/main.go, consumer subscription
		"pipeline.agent.telemetry", // websocket/kafka_bridge.go subscription
		"agent.executor.responses", // websocket/kafka_bridge.go subscription
	}

	for _, name := range consumeOnly {
		sites, present := all[name]
		if !present {
			t.Errorf("fixture %q no longer appears anywhere in api-gateway as a "+
				"kafkaclient.Topic/Topics argument — this case would now pass because the "+
				"literal is gone, not because the scanner ignores subscriptions. Replace it "+
				"with a consume-only literal that does exist.", name)
			continue
		}
		if got, ok := produced[name]; ok {
			t.Errorf("%q is consumed, not produced (it appears at %s), but the scan reported "+
				"it as a produce target at %s — the matcher is counting subscriptions",
				name, strings.Join(sites, ", "), strings.Join(got, ", "))
		}
	}

	// The whole scan must also still see SOMETHING, or "not in produced" is trivially
	// true for every name on earth.
	if len(produced) == 0 {
		t.Fatal("the produce scan found no targets at all in api-gateway — every assertion " +
			"above is vacuous")
	}
}

// siblingTopicLiterals collects every topic literal a service hands to
// kafkaclient.Topic/Topics, in ANY context — produce, consume or plain naming.
//
// It is siblingProduceTargets with the isProduceContext filter removed, and it exists
// only so the negative control above can prove its own fixtures are still real. Sharing
// the walk with the scanner is the point: a fixture that this helper cannot see is one
// the scanner cannot see either.
func siblingTopicLiterals(t *testing.T, root string, minFiles int) map[string][]string {
	t.Helper()
	found := map[string][]string{}
	seen := map[string]bool{}

	walkServiceSourceMin(t, root, minFiles, func(path string, fset *token.FileSet, file *ast.File) {
		local := kafkaclientLocalName(file)
		if local == "" {
			return
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isKafkaclientTopicCall(call, local) {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				name, err := strconv.Unquote(lit.Value)
				if err != nil || !plausibleTopicName(name) {
					continue
				}
				site := relPos(root, fset, lit.Pos())
				if !seen[name+"\x00"+site] {
					seen[name+"\x00"+site] = true
					found[name] = append(found[name], site)
				}
			}
			return true
		})
	})
	return found
}

func siblingServiceRoot(t *testing.T, dir string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", dir))
	if err != nil {
		t.Fatalf("resolving %s: %v", dir, err)
	}
	// The sibling has to actually be there. A missing directory would otherwise walk
	// nothing and pass.
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("expected the sibling service at %s to hold go.mod: %v", root, err)
	}
	return root
}

// siblingProduceTargets finds every topic literal these services hand to a producer.
//
// Both modules route every topic through kafkaclient.Topic/Topics — api-gateway's
// producer has no chokepoint of its own, so the call site is the only place the
// namespace can be applied, and that is enforced over there by
// api-gateway/internal/kafka/topic_namespacing_test.go. That makes the qualifier call
// the reliable place to read a topic name off, and it is why this scan can be literal-
// based without a table of every producer signature.
//
// The produce/consume split is decided by the shape the qualifier call sits in, listed
// in isProduceContext. Literals only: a topic held in a variable is computed per
// pipeline, and those have their own explicit pre-creation on the executor path.
func siblingProduceTargets(t *testing.T, root string, minFiles int) map[string][]string {
	t.Helper()
	found := map[string][]string{}
	seen := map[string]bool{}

	walkServiceSourceMin(t, root, minFiles, func(path string, fset *token.FileSet, file *ast.File) {
		local := kafkaclientLocalName(file)
		if local == "" {
			return
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if !isProduceContext(n) {
				return true
			}
			// Collect the qualifier calls anywhere inside this produce site.
			ast.Inspect(n, func(inner ast.Node) bool {
				call, ok := inner.(*ast.CallExpr)
				if !ok || !isKafkaclientTopicCall(call, local) {
					return true
				}
				for _, arg := range call.Args {
					lit, ok := arg.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						continue
					}
					name, err := strconv.Unquote(lit.Value)
					if err != nil || !plausibleTopicName(name) {
						continue
					}
					// Deduped: a nested produce shape (a ProducerMessage literal
					// inside a SendMessage call) matches isProduceContext twice,
					// and the same site listed twice reads like two defects.
					site := relPos(root, fset, lit.Pos())
					if !seen[name+"\x00"+site] {
						seen[name+"\x00"+site] = true
						found[name] = append(found[name], site)
					}
				}
				return true
			})
			return true
		})
	})
	return found
}

// isProduceContext reports whether a node is a site that WRITES to Kafka.
//
// These are the shapes both services actually use, rather than a general rule:
//
//	sarama.ProducerMessage{Topic: …}   backend-temporal-adapter/internal/workflows/activities.go
//	kafka.Message{Topic: …}            kafka-go writer messages
//	kafka.WriterConfig{Topic: …}       kafka-go writer construction
//	producer.Send*(…, topic, …)        api-gateway UnifiedProducer / AvroProducer
//	writer.WriteMessages(…)            kafka-go direct write
//	*.Produce*(…)                      this service's Manager, for symmetry
//
// A produce shape that is not on this list is invisible here rather than misreported,
// which is the safe direction for a guard: TestSiblingProduceScanSeesTheKnownSites is
// what stops the list rotting into one that matches nothing.
func isProduceContext(n ast.Node) bool {
	switch node := n.(type) {
	case *ast.CompositeLit:
		switch typeName(node.Type) {
		case "ProducerMessage", "Message", "WriterConfig":
			return true
		}
	case *ast.CallExpr:
		sel, ok := node.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		name := sel.Sel.Name
		return strings.HasPrefix(name, "Send") ||
			strings.HasPrefix(name, "Produce") ||
			name == "WriteMessages"
	}
	return false
}

// typeName reports the bare type name of a composite literal's type, with any package
// qualifier and pointer/slice decoration stripped.
func typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		return t.Sel.Name
	case *ast.StarExpr:
		return typeName(t.X)
	}
	return ""
}

// kafkaclientLocalName returns the name this file binds the kafkaclient package to,
// resolved through the import rather than assumed to be spelled "kafkaclient" — the
// import is aliased in some files and a matcher keyed on the spelling would quietly
// skip them.
func kafkaclientLocalName(file *ast.File) string {
	const path = "github.com/rsync-ai/shared/kafkaclient"
	for _, imp := range file.Imports {
		p, err := strconv.Unquote(imp.Path.Value)
		if err != nil || p != path {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		return "kafkaclient"
	}
	return ""
}

func isKafkaclientTopicCall(call *ast.CallExpr, local string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if sel.Sel.Name != "Topic" && sel.Sel.Name != "Topics" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == local
}
