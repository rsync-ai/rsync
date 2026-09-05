package kafka

import (
	"context"
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strconv"
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// Every topic this service produces to must be created by this service.
//
// The alternative is the broker's auto.create.topics.enable, and that is not a
// property of this platform — it is a property of somebody else's cluster. With it
// off, a produce to a missing topic is rejected; with it on, the topic is born
// carrying the BROKER's defaults, which on MSK and most managed clusters means
// min.insync.replicas=2 on an RF=1 topic: created, listed, subscribable, and
// permanently unwritable, with the pipeline reporting running and moving zero rows.
//
// Neither failure is discoverable from a green test suite, because a produce to a
// topic nobody provisioned looks exactly like a produce to one somebody did. So this
// reads the produce call sites out of the source and checks them against the set that
// EnsureAgentControlTopics actually creates. A new produce target added later fails
// here rather than in a customer's cluster.

// knownUncoveredProduceTargets are produce targets this test can see and this package
// does not provision. They are listed rather than ignored so the gap is a reviewed
// decision with an owner, not an absence.
//
// It is currently empty, and that is the intended end state.
//
// It used to hold rsync.notifications, on the stated grounds that the "rsync."-prefixed
// notification and healer families "spell the prefix as a string literal, so
// KAFKA_TOPIC_PREFIX does not apply to them" and land "outside" a custom namespace.
// That reasoning was wrong in both halves, and worth recording because it is an easy
// mistake to make again. The prefix DOES apply to them: every one is produced through
// kafka.Manager, which qualifies at Produce/ProduceWithHeaders/Consume (manager.go:321,
// :396, :920), so under KAFKA_TOPIC_PREFIX=acme. the wire name is
// acme.rsync.notifications — INSIDE the acme namespace, not outside it. The literal
// merely reads as though it were already qualified, and Topic() is idempotent by prefix
// match, so at the default prefix qualifying it is a no-op and the distinction is
// invisible.
//
// That invisibility was the actual defect, and it was on the consumer side: api-gateway's
// notifier subscribed to the bare literals while the healer produced the qualified names,
// so a customer prefix silently severed every Slack and email alert. Fixed by resolving
// the subscriptions through Topic() (notifier.go resolveNotifierTopics), and guarded by
// api-gateway/internal/kafka/topic_namespacing_test.go.
//
// The provisioning half is fixed in topology.go, which now creates the whole family.
//
// Growing this map requires justifying, in writing, why a new topic may depend on a
// broker setting this platform does not control.
var knownUncoveredProduceTargets = map[string]string{}

func TestEveryTopicWeProduceToIsProvisioned(t *testing.T) {
	provisioned := provisionedControlPlaneTopics(t)
	targets := producedTopicLiterals(t, serviceRoot(t))

	if len(targets) == 0 {
		t.Fatal("found no literal produce targets in the service — the scan is broken, " +
			"so a green result here would mean nothing")
	}

	var missing []string
	for topicName, sites := range targets {
		qualified := kafkaclient.Topic(topicName)
		if provisioned[qualified] {
			continue
		}
		if _, known := knownUncoveredProduceTargets[topicName]; known {
			continue
		}
		missing = append(missing, fmt.Sprintf("%s  (produced at %s)", qualified, strings.Join(sites, ", ")))
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("%d topic(s) are produced to but never created by this service, so they "+
			"exist only if the broker's auto.create.topics.enable is on — a setting this "+
			"platform does not own on a customer-managed cluster, and one that leaves the "+
			"topic carrying the broker's own min.insync.replicas when it is on:\n  %s\n\n"+
			"Add them to EnsureAgentControlTopics (topology.go), or record why not in "+
			"knownUncoveredProduceTargets.", len(missing), strings.Join(missing, "\n  "))
	}
}

// The allowlist must stay a list of real gaps. An entry that has since been
// provisioned is stale documentation claiming a problem that no longer exists.
func TestKnownUncoveredListHasNoStaleEntries(t *testing.T) {
	provisioned := provisionedControlPlaneTopics(t)
	for topicName := range knownUncoveredProduceTargets {
		if provisioned[kafkaclient.Topic(topicName)] {
			t.Errorf("%q is listed as an uncovered produce target but EnsureAgentControlTopics "+
				"now creates it — drop the entry", topicName)
		}
	}
}

// provisionedControlPlaneTopics runs the real provisioner against a fake broker and
// reports what it actually asked Kafka to create. Reading the slice literal instead
// would assert the test's copy of the list rather than the list that runs.
func provisionedControlPlaneTopics(t *testing.T) map[string]bool {
	t.Helper()
	tm, admin := newFakeManager(1)
	if err := tm.EnsureAgentControlTopics(context.Background(), 3); err != nil {
		t.Fatalf("EnsureAgentControlTopics: %v", err)
	}
	out := make(map[string]bool, len(admin.created))
	for name := range admin.created {
		out[name] = true
	}
	return out
}

// producedTopicLiterals finds every string literal handed to a Produce* call.
//
// Literals only: a topic held in a variable is computed per pipeline (the CDC and
// batch data topics), and those have their own explicit pre-creation on the executor
// path. What this catches is the steady-state platform topic someone names inline —
// which is how all three of agent.executor.responses, pipeline.domain.events and
// pipeline.agent.telemetry came to depend on auto-creation without anyone deciding to.
func producedTopicLiterals(t *testing.T, root string) map[string][]string {
	t.Helper()
	found := map[string][]string{}

	walkServiceSource(t, root, func(path string, fset *token.FileSet, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !strings.HasPrefix(sel.Sel.Name, "Produce") {
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
				found[name] = append(found[name], relPos(root, fset, lit.Pos()))
				break // the topic is the first string argument
			}
			return true
		})
	})
	return found
}

// plausibleTopicName keeps format strings, header keys and log text out of the set.
// Every topic this platform uses is a dotted lowercase path.
func plausibleTopicName(s string) bool {
	if !strings.Contains(s, ".") || strings.ContainsAny(s, " %/:") {
		return false
	}
	return s == strings.ToLower(s)
}
