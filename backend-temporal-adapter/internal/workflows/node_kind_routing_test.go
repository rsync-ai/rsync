package workflows

// Runtime conformance census for the workflow NodeKind enum.
//
// WHAT THIS GUARDS. types/workflow_graph.go declares ten node kinds and
// types/graph_converter.go gives all ten a stage type, colour, group, icon,
// description and duration estimate — so a planner can emit, and the UI can
// draw, a node the runtime has never heard of. The runtime knows three:
// localNodeOutput resolves `source` and `transform` without an activity, and
// exactly one dispatch rule (`destination`) forces the task field the executor
// reads. The census fixes that count in place so it cannot shrink silently, and
// so a kind cannot be added and left unimplemented without a named failure.
//
// HOW IT GUARDS — and the correction that matters most in this file's history.
// "Routed" here means the kind changes something a CONSUMER READS. There is
// exactly one such field on the dispatch side:
//
//	routed(kind) := localNodeOutput handles it
//	             || applyNodeKindRouting forces task["operation"] to a value
//	                other than the executor's own default
//
// `request_type` is deliberately NOT part of that predicate. An earlier version
// of this census derived routed-ness from request_type, which made it
// silenceable by one line — `NodeKindWait: {RequestType: "wait"}` in
// nodeDispatchRules — that changes no runtime behaviour whatsoever. Nothing
// reads request_type: the adapter's task map arrives on the orchestrator as
// PendingRequest.Payload, that struct's own RequestType field is filled from
// the envelope's agent_type (shared/go/correlation/polling.go:90-91), and no
// code in backend-orchestrator reads Payload["request_type"]. Keying a guard on
// it meant treating a provably inert string as evidence of routing. It is still
// pinned below as a contract, because drift in it is worth noticing — it is
// just not evidence.
//
// Every assertion is made by CALLING the routing functions in
// node_kind_routing.go, never by parsing their source:
//
//   - A source-text guard treats "the constant is mentioned inside the
//     function" as "the kind is routed". One `logger.Info("...", node.Kind ==
//     NodeKindMerge)` line satisfies it while merge stays as dead as before.
//     This census observes what localNodeOutput RETURNS.
//   - A source-text guard has to decide whether `case "wait":` and `case
//     workflowtypes.NodeKindWait:` count. Rejecting the constant form turns CI
//     red on a strictly-improving refactor. This census never sees spelling.
//   - A source-text guard cannot see that a new case arm produces the same
//     values as the default arm. This census can, because it compares values.
//
// WHAT IT STILL DOES NOT PROVE. That a rule forces `operation` proves the
// executor will dispatch on a different key; it does not prove the handler for
// that key does what the node kind means. The executor lives in another Go
// module and cannot be called from here, so the operation names below are a
// transcription (executorDispatchOperations) with a file:line citation, not a
// derivation. What that buys is still real: making a dead kind look routed now
// costs a change to the one field the executor reads, set to a value that names
// an existing handler — not a string nobody consumes.

import (
	"sort"
	"testing"

	workflowtypes "github.com/rsync-ai/backend-temporal-adapter/internal/workflows/types"
)

// executorDispatchOperations is the set of operations the executor actually
// handles, transcribed from the `switch task.Operation` at
// backend-orchestrator/internal/agents/executor/executor.go:1661-1680. Its
// default arm (:1681) returns `unknown operation: %s` as a task failure.
//
// It is a transcription because the executor is a different Go module; this
// package cannot import it. That makes the set the weakest link in the census,
// so it is deliberately small, cited, and used for one purpose only: rejecting
// a dispatch rule whose Operation names no handler. A rule pointing at an
// operation that is not here would make the node fail at runtime with
// `unknown operation`, which is not routing either.
var executorDispatchOperations = map[string]string{
	"export":            "executor.go:1662",
	"import":            "executor.go:1664",
	"query":             "executor.go:1666",
	"execute":           "executor.go:1668",
	"data_transfer":     "executor.go:1670",
	"start_streaming":   "executor.go:1673",
	"start_cdc":         "executor.go:1673",
	"stop_streaming":    "executor.go:1675",
	"stop_cdc":          "executor.go:1675",
	"streaming_status":  "executor.go:1677",
	"cdc_status":        "executor.go:1677",
	"restart_streaming": "executor.go:1679",
	"restart_cdc":       "executor.go:1679",
}

// nodeKindExpectation pins the complete observable routing outcome for one kind.
//
// It is a golden table on purpose: any change to how a kind is routed has to be
// written down here as well as in node_kind_routing.go, by someone who has read
// both. Editing this table to make a red build green is a deliberate act, and
// the reviewer of that diff sees the claim being made.
type nodeKindExpectation struct {
	// LocallyHandled: localNodeOutput resolves this kind with no activity.
	// This is real routing — the scheduler returns without dispatching.
	LocallyHandled bool
	// RequestType: the value applyNodeKindRouting writes to
	// task["request_type"]. Empty means "no dispatch rule" — the kind falls
	// through to genericNodeRequestType. Pinned as a contract; NOT evidence of
	// routing, because nothing downstream reads this key.
	RequestType string
	// Operation: the value forced onto task["operation"], the ONLY field the
	// downstream executor branches on
	// (backend-orchestrator/internal/workers/executor.go:557 ->
	// internal/agents/executor/executor.go:1661). A non-empty value here — other
	// than the executor's own default — is what makes a dispatched kind routed.
	Operation string
	// ConnectorTypeField: the task key node_config["connector_type"] is copied
	// into, if any. Also inert downstream: the executor reads source_type /
	// destination_type only when the task carries source_config /
	// destination_config (executor.go:582-593), which this adapter never sets.
	ConnectorTypeField string
	// Unrouted must be non-empty exactly when the kind changes nothing a
	// consumer reads — i.e. it is not locally handled and forces no operation.
	//
	// It is a record of a known defect, not permission for one. There is no way
	// to clear it from this table alone: routed-ness is recomputed from what the
	// functions return, so an entry cleared without a real change fails, and a
	// real change that is not recorded here fails too. Adding request_type
	// strings does not move a kind out of this bucket.
	// See KI-NODE-KINDS-DECLARED-BUT-UNROUTED in CAPABILITIES.md.
	Unrouted string
}

// declarationOnlyReason is the shared Unrouted text for the four kinds that get
// a distinct request_type and nothing else. They are worth distinguishing from
// wait/merge/split in prose, but not in behaviour: both groups reach the
// executor with no kind-derived operation.
const declarationOnlyReason = "declaration only: a distinct request_type, which no consumer reads, " +
	"and no forced operation. Nothing else in the adapter branches on this kind — outside " +
	"types/ (declarations and UI presentation) the only NodeKind references are in " +
	"node_kind_routing.go — and backend-orchestrator has zero references to node_kind. " +
	"Whatever such a node does comes from a planner-supplied node_config.operation, not " +
	"from its kind."

var nodeKindCensus = map[string]nodeKindExpectation{
	// --- routed: resolved by the scheduler with no activity at all ---
	workflowtypes.NodeKindSource: {
		LocallyHandled:     true,
		RequestType:        "extract",
		ConnectorTypeField: "source_type",
	},
	workflowtypes.NodeKindTransform: {
		LocallyHandled: true,
		RequestType:    "transform",
	},

	// --- routed: the one rule that forces the executor's dispatch key ---
	workflowtypes.NodeKindDestination: {
		RequestType:        "load",
		Operation:          "data_transfer",
		ConnectorTypeField: "destination_type",
	},

	// --- unrouted: a distinct declaration string, no runtime effect ---
	workflowtypes.NodeKindAPICall:      {RequestType: "api_call", Unrouted: declarationOnlyReason},
	workflowtypes.NodeKindCondition:    {RequestType: "evaluate_condition", Unrouted: declarationOnlyReason},
	workflowtypes.NodeKindLLM:          {RequestType: "llm_process", Unrouted: declarationOnlyReason},
	workflowtypes.NodeKindNotification: {RequestType: "notification", Unrouted: declarationOnlyReason},

	// --- unrouted: not even a distinct declaration ---
	workflowtypes.NodeKindWait: {
		Unrouted: "no timer/delay path anywhere; reaches the executor with the generic " +
			"request_type and no operation, i.e. the exact payload of a kind the adapter " +
			"has never heard of",
	},
	workflowtypes.NodeKindMerge: {
		Unrouted: "fan-in is only the scheduler's in-degree bookkeeping; no merge semantics exist",
	},
	workflowtypes.NodeKindSplit: {
		Unrouted: "no fan-out/partition path; reaches the executor as an unrecognised kind",
	},
}

// observedNodeRouting is what the two routing seams actually produce for one
// kind. Every field is a return value, never a source-text reading.
type observedNodeRouting struct {
	Local       bool
	LocalOutput map[string]interface{}
	Task        map[string]interface{}
	RequestType string
	Operation   string
}

// ForcesOperation reports whether the kind changed the one task field the
// executor dispatches on. Setting it to the executor's own default
// (backend-orchestrator/internal/workers/executor.go:561) is the same as not
// setting it, so that does not count.
func (o observedNodeRouting) ForcesOperation() bool {
	return o.Operation != "" && o.Operation != defaultExecutorOperation
}

// KindSpecificRequestType reports whether the kind produced a declaration
// distinct from the generic one. Recorded for the census arithmetic; explicitly
// NOT part of Routed.
func (o observedNodeRouting) KindSpecificRequestType() bool {
	return o.RequestType != "" && o.RequestType != genericNodeRequestType
}

// Routed is the census's definition of "this kind does something": the
// scheduler answers it locally, or the executor receives a different dispatch
// key because of it. Nothing else qualifies.
func (o observedNodeRouting) Routed() bool {
	return o.Local || o.ForcesOperation()
}

// observeNodeRouting calls both seams for one kind with a fixed input.
func observeNodeRouting(kind string) observedNodeRouting {
	node := &workflowtypes.GraphNode{
		ID:     "census_node",
		Kind:   kind,
		Config: map[string]interface{}{"connector_type": "census_connector"},
	}
	out, local := localNodeOutput("census_node", node, nil)

	task := map[string]interface{}{}
	applyNodeKindRouting(task, kind, map[string]interface{}{"connector_type": "census_connector"})

	requestType, _ := task["request_type"].(string)
	operation, _ := task["operation"].(string)
	return observedNodeRouting{
		Local:       local,
		LocalOutput: out,
		Task:        task,
		RequestType: requestType,
		Operation:   operation,
	}
}

func TestNodeKindRoutingCensus(t *testing.T) {
	declared := workflowtypes.AllNodeKinds

	// Positive denominators. An empty registry would make every loop below
	// vacuously true. (types.TestNodeKindConstantsAreRegistered proves the
	// registry matches the declarations; this only proves it is not empty.)
	if len(declared) == 0 {
		t.Fatalf("workflowtypes.AllNodeKinds is empty — this census asserts nothing")
	}
	if len(executorDispatchOperations) == 0 {
		t.Fatalf("executorDispatchOperations is empty — the operation check below would " +
			"accept any value")
	}

	// 0. The census covers exactly the declared kinds, both directions. A new
	//    constant lands here with no entry and fails; a deleted one leaves a
	//    stale entry and fails.
	for _, kind := range declared {
		if _, ok := nodeKindCensus[kind]; !ok {
			t.Errorf("node kind %q is declared (workflowtypes.AllNodeKinds) but this "+
				"census has no entry for it. Add one describing how it is routed — or, "+
				"if nothing routes it, add it with an Unrouted reason AND a Known-issues "+
				"entry in CAPABILITIES.md.", kind)
		}
	}
	declaredSet := map[string]bool{}
	for _, kind := range declared {
		declaredSet[kind] = true
	}
	for _, kind := range sortedCensusKinds(nodeKindCensus) {
		if !declaredSet[kind] {
			t.Errorf("this census has an entry for %q, which is not in "+
				"workflowtypes.AllNodeKinds — stale entry, delete it", kind)
		}
	}

	// 1. Rule sanity, checked over the table itself. These are the cheap
	//    non-fixes: values that look like routing and are byte-identical to its
	//    absence.
	for _, kind := range sortedRuleKinds(nodeDispatchRules) {
		rule := nodeDispatchRules[kind]
		if rule.RequestType == genericNodeRequestType {
			t.Errorf("nodeDispatchRules[%q].RequestType is %q — that is "+
				"genericNodeRequestType, the value an UNROUTED kind already gets. A rule "+
				"that writes it changes nothing; it only makes the table look fuller.",
				kind, rule.RequestType)
		}
		if rule.Operation == defaultExecutorOperation {
			t.Errorf("nodeDispatchRules[%q].Operation is %q, which is the operation the "+
				"executor falls back to when the payload names none "+
				"(backend-orchestrator/internal/workers/executor.go:561). Forcing it is "+
				"byte-for-byte identical to forcing nothing, so this rule routes nothing.",
				kind, rule.Operation)
		}
		if rule.Operation != "" && rule.Operation != defaultExecutorOperation {
			if _, handled := executorDispatchOperations[rule.Operation]; !handled {
				t.Errorf("nodeDispatchRules[%q].Operation is %q, which is not one of the "+
					"operations the executor dispatches (%v — transcribed from "+
					"backend-orchestrator/internal/agents/executor/executor.go:1661-1680). A "+
					"node forcing it fails at runtime with `unknown operation: %s`. Add the "+
					"handler on the orchestrator side first, then list it in "+
					"executorDispatchOperations with its file:line.",
					kind, rule.Operation, sortedOperationNames(executorDispatchOperations), rule.Operation)
			}
		}
		if !declaredSet[kind] {
			t.Errorf("nodeDispatchRules has a rule for %q, which is not a declared node "+
				"kind — dead rule, delete it", kind)
		}
	}

	// 2. Observe the real routing for every declared kind and compare against
	//    the pinned table.
	var (
		localCount              int // resolved by the scheduler, no activity
		operationCount          int // dispatched AND forcing the executor's dispatch key
		kindSpecificDispatched  int // dispatched AND carrying a distinct request_type
		genericDispatched       int // dispatched AND carrying the generic request_type
		routedCount             int
		unroutedCount           int
		unroutedDeclarationOnly int
		unroutedNothing         int
	)
	seenRequestTypes := map[string]string{}

	for _, kind := range declared {
		want, ok := nodeKindCensus[kind]
		if !ok {
			continue // already reported above
		}

		got := observeNodeRouting(kind)

		// 2a. Scheduler-local routing — observed, not parsed.
		if got.Local != want.LocallyHandled {
			if want.LocallyHandled {
				t.Errorf("localNodeOutput no longer resolves %q locally (returned "+
					"handled=false). The scheduler's deterministic path for this kind was "+
					"dropped: it will now be dispatched as an activity. Restore it, or "+
					"update this census with why that is intended.", kind)
			} else {
				t.Errorf("localNodeOutput now resolves %q locally, which this census did "+
					"not expect. If intended, set LocallyHandled: true and say why the "+
					"kind needs no activity.", kind)
			}
		}
		if got.Local && got.LocalOutput == nil {
			t.Errorf("localNodeOutput claims to handle %q but returned a nil output map", kind)
		}
		if !got.Local && got.LocalOutput != nil {
			t.Errorf("localNodeOutput returned handled=false for %q but a non-nil output; "+
				"executeNode discards it, so this output is silently dropped", kind)
		}

		// 2b. Activity dispatch — the real function the activity calls.
		wantRequestType := want.RequestType
		if wantRequestType == "" {
			wantRequestType = genericNodeRequestType
		}
		if got.RequestType != wantRequestType {
			t.Errorf("node kind %q: applyNodeKindRouting wrote request_type=%q, census "+
				"pins %q. Nothing downstream reads request_type, so this is contract drift "+
				"rather than a behaviour change — and it does NOT make the kind routed. "+
				"Update the census only after checking that no new consumer appeared.",
				kind, got.RequestType, wantRequestType)
		}
		if got.Operation != want.Operation {
			t.Errorf("node kind %q: applyNodeKindRouting wrote operation=%q, census pins "+
				"%q. `operation` is the ONLY field the executor dispatches on "+
				"(backend-orchestrator/internal/agents/executor/executor.go:1661), so this "+
				"IS a real behaviour change.", kind, got.Operation, want.Operation)
		}
		if want.ConnectorTypeField != "" {
			if v, _ := got.Task[want.ConnectorTypeField].(string); v != "census_connector" {
				t.Errorf("node kind %q: expected node_config connector_type to be copied "+
					"into task[%q]; got %q", kind, want.ConnectorTypeField, v)
			}
		} else {
			for _, field := range []string{"source_type", "destination_type"} {
				if _, present := got.Task[field]; present {
					t.Errorf("node kind %q: task[%q] was set, but the census pins no "+
						"ConnectorTypeField for this kind", kind, field)
				}
			}
		}

		if got.KindSpecificRequestType() {
			if prev, dup := seenRequestTypes[got.RequestType]; dup {
				t.Errorf("node kinds %q and %q both declare request_type=%q — the "+
					"declaration no longer distinguishes them", prev, kind, got.RequestType)
			}
			seenRequestTypes[got.RequestType] = kind
		}

		// 2c. Routed-or-recorded. `routed` is recomputed from what the functions
		//     returned, and only from the two things a consumer reads: the
		//     scheduler resolving the node, or a forced executor operation.
		switch {
		case got.Routed() && want.Unrouted != "":
			t.Errorf("node kind %q now routes for real (locally=%v, operation=%q) but the "+
				"census still records it as unrouted (%q). Confirm the executor handler for "+
				"that operation does what this kind means, then clear the Unrouted reason "+
				"and strike the row in CAPABILITIES.md — a stale record hides the next dead kind.",
				kind, got.Local, got.Operation, want.Unrouted)
		case !got.Routed() && want.Unrouted == "":
			t.Errorf("node kind %q is declared but nothing routes it: localNodeOutput "+
				"returns handled=false and applyNodeKindRouting forces no executor "+
				"operation (request_type=%q is read by nobody). Implement it — a case in "+
				"localNodeOutput, or a rule whose Operation names a real executor handler — "+
				"or record it here with an Unrouted reason AND a Known-issues entry in "+
				"CAPABILITIES.md. Adding a request_type will not clear this.",
				kind, got.RequestType)
		}

		// Counters. Every kind lands in exactly one of local / dispatched, and
		// exactly one of routed / unrouted; step 3 asserts that they close.
		switch {
		case got.Local:
			localCount++
		default:
			if got.ForcesOperation() {
				operationCount++
			}
			if got.KindSpecificRequestType() {
				kindSpecificDispatched++
			} else {
				genericDispatched++
			}
		}
		if got.Routed() {
			routedCount++
		} else {
			unroutedCount++
			if got.KindSpecificRequestType() {
				unroutedDeclarationOnly++
			} else {
				unroutedNothing++
			}
		}

		// 2d. Agent selection does not differentiate node kinds. nodeKindToAgentType
		//     is a switch with seven arms and a default that all return the same
		//     string — a shape that reads like routing and is not. Recorded as an
		//     assertion so that if it ever starts to differentiate, the census notices.
		if agent := nodeKindToAgentType(kind); agent != "executor" {
			t.Errorf("nodeKindToAgentType(%q) = %q; every declared kind has always "+
				"resolved to \"executor\". If that changed, the census needs an "+
				"AgentType column.", kind, agent)
		}
	}

	// 3. Positive denominators, and a decomposition that has to close exactly.
	//    A census whose numbers do not add up has stopped measuring something.
	if localCount == 0 {
		t.Errorf("no declared node kind is handled locally — localNodeOutput's switch " +
			"lost every case, or the census is calling the wrong function")
	}
	if operationCount == 0 {
		t.Errorf("no dispatched node kind forces an executor operation — every dispatch " +
			"rule is now declaration-only, which means no dispatched kind changes what runs")
	}
	dispatched := len(declared) - localCount
	if operationCount+kindSpecificDispatched+genericDispatched == 0 {
		t.Fatalf("no dispatched kind was measured at all (dispatched=%d)", dispatched)
	}
	if kindSpecificDispatched+genericDispatched != dispatched {
		t.Fatalf("kind-specific request_type (%d) + generic (%d) != dispatched (%d)",
			kindSpecificDispatched, genericDispatched, dispatched)
	}
	if routedCount+unroutedCount != len(declared) {
		t.Fatalf("routed(%d) + unrouted(%d) != declared(%d)", routedCount, unroutedCount, len(declared))
	}
	if routedCount != localCount+operationCount {
		t.Fatalf("routed(%d) != local(%d) + forces-operation(%d)", routedCount, localCount, operationCount)
	}
	if unroutedDeclarationOnly+unroutedNothing != unroutedCount {
		t.Fatalf("declaration-only(%d) + nothing(%d) != unrouted(%d)",
			unroutedDeclarationOnly, unroutedNothing, unroutedCount)
	}

	// Rules whose kind is answered locally can never run: dag_scheduler.go:289
	// returns before dispatching the activity, and that activity is the only
	// non-test caller of applyNodeKindRouting.
	reachableRules, unreachableRules := 0, 0
	for _, kind := range sortedRuleKinds(nodeDispatchRules) {
		if observeNodeRouting(kind).Local {
			unreachableRules++
		} else {
			reachableRules++
		}
	}
	if reachableRules+unreachableRules != len(nodeDispatchRules) {
		t.Fatalf("reachable(%d) + unreachable(%d) != rules(%d)",
			reachableRules, unreachableRules, len(nodeDispatchRules))
	}

	t.Logf("%d declared node kind(s) = %d resolved locally by localNodeOutput + %d dispatched "+
		"to the executor activity. Of those %d dispatched: %d force task[\"operation\"] (the "+
		"only field the executor reads) and %d force none; %d carry a kind-specific "+
		"request_type (a declaration no consumer reads) and %d carry the generic one. "+
		"ROUTED = %d (%d local + %d operation). UNROUTED = %d (%d declaration-only + %d "+
		"nothing at all). nodeDispatchRules holds %d rules: %d reachable, %d unreachable "+
		"(localNodeOutput answers those kinds first).",
		len(declared), localCount, dispatched,
		dispatched, operationCount, dispatched-operationCount,
		kindSpecificDispatched, genericDispatched,
		routedCount, localCount, operationCount,
		unroutedCount, unroutedDeclarationOnly, unroutedNothing,
		len(nodeDispatchRules), reachableRules, unreachableRules)
}

func sortedCensusKinds(m map[string]nodeKindExpectation) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedRuleKinds(m map[string]nodeDispatchRule) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedOperationNames(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------
// Behaviour pins for the two routing seams.
//
// The census proves every declared kind is classified. These prove the
// classification is not hollow: they assert the actual values the two functions
// produce, so extracting them out of executeNode / ExecuteGraphNodeActivityV2
// cannot have quietly changed what a source, transform or destination node does.
// ---------------------------------------------------------------------------

func TestLocalNodeOutputSourceIsDeclarative(t *testing.T) {
	node := &workflowtypes.GraphNode{
		ID:   "extract_pg",
		Kind: workflowtypes.NodeKindSource,
		Config: map[string]interface{}{
			"connector_type": "postgresql",
			"database":       "app",
		},
	}
	out, handled := localNodeOutput("extract_pg", node, nil)
	if !handled {
		t.Fatalf("source must be resolved locally; a dispatched source node is how the "+
			"executor used to fall back to a direct transfer (out=%v)", out)
	}
	if got := out["connector_type"]; got != "postgresql" {
		t.Errorf("out[connector_type] = %v, want postgresql", got)
	}
	cfg, ok := out["config"].(map[string]interface{})
	if !ok || cfg["database"] != "app" {
		t.Errorf("out[config] = %v, want the node config verbatim", out["config"])
	}

	// A source with no config still resolves locally, with an empty (not nil) map.
	bare := &workflowtypes.GraphNode{ID: "extract_bare", Kind: workflowtypes.NodeKindSource}
	out, handled = localNodeOutput("extract_bare", bare, nil)
	if !handled || out == nil || len(out) != 0 {
		t.Errorf("bare source: handled=%v out=%#v, want handled=true and an empty map", handled, out)
	}
}

func TestLocalNodeOutputTransformAccumulatesUpstream(t *testing.T) {
	// Two upstream inputs, one in each supported shape, plus this node's own
	// transform. Input keys are visited in sorted order, so the result must be
	// deterministic — Temporal replays this function.
	inputs := map[string]interface{}{
		"b_node": map[string]interface{}{
			"transforms": []interface{}{map[string]interface{}{"type": "b"}},
		},
		"a_node": map[string]interface{}{
			"metadata": map[string]interface{}{
				"transforms": []interface{}{map[string]interface{}{"type": "a"}},
			},
		},
	}
	node := &workflowtypes.GraphNode{
		ID:   "transform_c",
		Kind: workflowtypes.NodeKindTransform,
		Config: map[string]interface{}{
			"transforms": []interface{}{map[string]interface{}{"type": "c"}},
		},
	}

	out, handled := localNodeOutput("transform_c", node, inputs)
	if !handled {
		t.Fatalf("transform must be resolved locally")
	}
	got := transformTypes(t, out)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("accumulated transforms = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("accumulated transforms = %v, want %v (upstream first, sorted by "+
				"input key, then this node's own)", got, want)
		}
	}
}

func TestLocalNodeOutputTransformShorthandInfersTypeFromNodeID(t *testing.T) {
	// No "transforms" key: the whole config is one transform spec, and the type
	// is inferred from the node ID's "transform_" prefix.
	node := &workflowtypes.GraphNode{
		ID:     "transform_uppercase",
		Kind:   workflowtypes.NodeKindTransform,
		Config: map[string]interface{}{"column": "name"},
	}
	out, handled := localNodeOutput("transform_uppercase", node, nil)
	if !handled {
		t.Fatalf("transform must be resolved locally")
	}
	got := transformTypes(t, out)
	if len(got) != 1 || got[0] != "uppercase" {
		t.Errorf("inferred transform types = %v, want [uppercase]", got)
	}
}

func TestLocalNodeOutputLeavesOtherKindsToTheActivity(t *testing.T) {
	// Derived from the census, not from a second hand-written list: a kind the
	// census does not mark LocallyHandled must be dispatched, and there is no
	// separate place to edit that could disagree with it.
	checked := 0
	for _, kind := range sortedCensusKinds(nodeKindCensus) {
		if nodeKindCensus[kind].LocallyHandled {
			continue
		}
		checked++
		node := &workflowtypes.GraphNode{ID: "n", Kind: kind}
		if out, handled := localNodeOutput("n", node, nil); handled || out != nil {
			t.Errorf("localNodeOutput(%q): handled=%v out=%#v, want false/nil so "+
				"executeNode dispatches the activity", kind, handled, out)
		}
	}
	if checked == 0 {
		t.Fatalf("no census entry is dispatched — this test asserted nothing")
	}
}

func TestApplyNodeKindRoutingDestinationOverridesPlannerOperation(t *testing.T) {
	// A planner may set node_config.operation, which ExecuteGraphNodeActivityV2
	// copies onto the task BEFORE routing. The destination rule must still win —
	// that ordering was implicit in the old inline switch and is load-bearing:
	// `operation` is the only field the executor dispatches on.
	task := map[string]interface{}{"operation": "planner_supplied"}
	applyNodeKindRouting(task, workflowtypes.NodeKindDestination, map[string]interface{}{
		"connector_type": "snowflake",
	})
	if task["operation"] != "data_transfer" {
		t.Errorf("task[operation] = %v, want data_transfer (destination must override "+
			"the planner's value)", task["operation"])
	}
	if task["request_type"] != "load" || task["destination_type"] != "snowflake" {
		t.Errorf("task = %#v, want request_type=load destination_type=snowflake", task)
	}

	// A kind whose rule sets no Operation must leave the planner's value alone.
	task = map[string]interface{}{"operation": "planner_supplied"}
	applyNodeKindRouting(task, workflowtypes.NodeKindLLM, nil)
	if task["operation"] != "planner_supplied" {
		t.Errorf("task[operation] = %v, want the planner's value untouched", task["operation"])
	}
	if task["request_type"] != "llm_process" {
		t.Errorf("task[request_type] = %v, want llm_process", task["request_type"])
	}
}

func TestUnroutedKindsReachTheExecutorLikeAnUnknownKind(t *testing.T) {
	// The documented meaning of genericNodeRequestType: what a kind the adapter
	// has never heard of receives. It is therefore not evidence of routing.
	unknown := map[string]interface{}{}
	applyNodeKindRouting(unknown, "kind_that_does_not_exist", nil)
	if unknown["request_type"] != genericNodeRequestType {
		t.Errorf("task[request_type] = %v, want %q", unknown["request_type"], genericNodeRequestType)
	}
	if op, present := unknown["operation"]; present {
		t.Errorf("an unknown kind set task[operation]=%v; it must set none", op)
	}

	// Every kind the census records as unrouted is indistinguishable from that
	// unknown kind in the field the executor reads. The list is DERIVED from the
	// census, so there is no second place to edit: dropping a kind from this
	// check means clearing its Unrouted reason, which the census then rejects
	// unless the routing really changed.
	checked := 0
	for _, kind := range sortedCensusKinds(nodeKindCensus) {
		if nodeKindCensus[kind].Unrouted == "" {
			continue
		}
		checked++
		task := map[string]interface{}{}
		applyNodeKindRouting(task, kind, nil)
		if op, present := task["operation"]; present {
			t.Errorf("the census records node kind %q as unrouted, but "+
				"applyNodeKindRouting now forces task[operation]=%v. That IS a runtime "+
				"change: the executor dispatches on this field "+
				"(backend-orchestrator/internal/agents/executor/executor.go:1661). Verify a "+
				"handler for that operation exists AND does what this kind means, then "+
				"update the census entry. Do not try to reach this state by adding a "+
				"request_type: that key is read by nobody.", kind, op)
		}
	}
	if checked == 0 {
		t.Fatalf("the census records no unrouted kind — either every kind is now routed " +
			"(in which case update CAPABILITIES.md) or the Unrouted reasons were deleted " +
			"without a behaviour change")
	}
}

// transformTypes pulls the accumulated transform "type" values out of a
// localNodeOutput result, failing on any shape it does not recognise.
func transformTypes(t *testing.T, out map[string]interface{}) []string {
	t.Helper()
	md, ok := out["metadata"].(map[string]interface{})
	if !ok {
		t.Fatalf("out[metadata] = %#v, want a map", out["metadata"])
	}
	list, ok := md["transforms"].([]interface{})
	if !ok {
		t.Fatalf("metadata[transforms] = %#v, want a slice", md["transforms"])
	}
	types := make([]string, 0, len(list))
	for _, item := range list {
		spec, ok := item.(map[string]interface{})
		if !ok {
			t.Fatalf("transform entry %#v is not a map", item)
		}
		value, _ := spec["type"].(string)
		types = append(types, value)
	}
	return types
}
