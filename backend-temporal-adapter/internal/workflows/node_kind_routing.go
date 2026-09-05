package workflows

import (
	"fmt"
	"sort"
	"strings"

	workflowtypes "github.com/rsync-ai/backend-temporal-adapter/internal/workflows/types"
)

// ==============================================================================
// NODE KIND ROUTING
// ==============================================================================
//
// This file is the whole surface on which a workflow NodeKind changes runtime
// behaviour. There are exactly two seams, and both live here as ordinary
// functions so a test can CALL them instead of reading their source text:
//
//  1. localNodeOutput      — the kinds the DAG scheduler resolves itself, with
//     no activity at all (dag_scheduler.go executeNode calls it).
//  2. applyNodeKindRouting — the kind-specific fields written onto the executor
//     task for every other kind (nl_pipeline_v2_activities.go
//     ExecuteGraphNodeActivityV2 calls it).
//
// node_kind_routing_test.go is the conformance census over those two functions.
// It invokes them for every constant in workflowtypes.AllNodeKinds and compares
// the OUTCOME against a pinned table, so "this kind looks handled" is not
// something the census can be told — it only sees what the code returns.
//
// HONEST SCOPE — which of the fields below a downstream consumer actually reads.
// Only ONE of them does anything, and the census is keyed on that one:
//
//   - `operation` is read. internal/workers/executor.go:557 copies
//     task.Payload["operation"] into ExecutorTask.Operation (and defaults it to
//     "execute" at :561 when absent), and internal/agents/executor/executor.go:1661
//     switches on it. Forcing it is the only way a node kind changes what runs.
//   - `request_type` is read by NOTHING. The adapter's task map arrives on the
//     orchestrator as PendingRequest.Payload, and that struct's own RequestType
//     field is filled from the envelope's `agent_type`, not from this key
//     (shared/go/correlation/polling.go:90-91) — so the "request_type" log line
//     at internal/workers/executor_redis_polling.go:82 prints "executor", never
//     the value written here. Nothing in backend-orchestrator reads
//     Payload["request_type"] at all. It is a declaration that reaches Redis and
//     stops.
//   - `source_type` / `destination_type` are read only inside
//     `if task.Context["source_config"]` / `["destination_config"]`
//     (internal/workers/executor.go:582-593). No code path in this adapter ever
//     writes either config key, so for a node_execution task those two are inert
//     as well.
//
// So a kind having a rule here means the adapter NAMES it. Only a rule that
// forces `operation` — or a case in localNodeOutput — means anything executes
// differently because of the kind. See KI-NODE-KINDS-DECLARED-BUT-UNROUTED in
// CAPABILITIES.md.

// genericNodeRequestType is the request_type written for a node kind that has
// no dispatch rule. It is the "nobody routed this" value, which is why
// nodeDispatchRules must never map a kind to it: a rule whose RequestType is
// this string is byte-for-byte the unrouted behaviour wearing a case label, and
// the census rejects it.
const genericNodeRequestType = "execute"

// defaultExecutorOperation is what the executor uses when the task payload
// names no operation (internal/workers/executor.go:557-561). Setting
// task["operation"] to this value is therefore byte-for-byte identical to not
// setting it, so a rule that does so has forced nothing — the census rejects it
// for the same reason it rejects a RequestType of genericNodeRequestType.
//
// It is the same string as genericNodeRequestType by coincidence, not by
// derivation: one is the adapter's fallback declaration, the other is the
// orchestrator's fallback dispatch key. They are separate constants so that a
// change to either side stays readable.
const defaultExecutorOperation = "execute"

// nodeDispatchRule is the kind-specific part of an executor task.
type nodeDispatchRule struct {
	// RequestType becomes task["request_type"] — a declaration nothing reads
	// (see HONEST SCOPE above). Must not be genericNodeRequestType. Adding a
	// rule that only sets this field changes no runtime behaviour, and the
	// census will still count the kind as unrouted.
	RequestType string
	// Operation, when non-empty, becomes task["operation"] and therefore
	// overrides any planner-supplied node_config.operation, exactly as the
	// previous inline switch did. This is the ONLY field here that the executor
	// reads, so it is the only field that can make a kind "routed". It must name
	// an operation the executor's switch actually handles and must not be
	// defaultExecutorOperation; both are checked by the census.
	Operation string
	// ConnectorTypeField, when non-empty, is the task key that node_config's
	// "connector_type" is copied into (e.g. "source_type").
	ConnectorTypeField string
}

// nodeDispatchRules is the pinned routing table for the kinds that reach
// ExecuteGraphNodeActivityV2. A kind absent from this map falls through to
// genericNodeRequestType.
//
// ADDING A ROW HERE DOES NOT ROUTE A KIND. Every field except Operation is
// inert downstream, so a row like `NodeKindWait: {RequestType: "wait"}` leaves
// the kind exactly as dead as it was; the census derives routed-ness from
// Operation and from localNodeOutput, never from this map's membership.
//
// Two of the rows below are also unreachable: localNodeOutput answers `source`
// and `transform` before dag_scheduler.go:289 ever dispatches the activity, so
// their rules describe a call that no non-test caller makes. They are kept
// because the activity is exported and a future caller could reach it.
var nodeDispatchRules = map[string]nodeDispatchRule{
	workflowtypes.NodeKindSource: {
		RequestType:        "extract",
		ConnectorTypeField: "source_type",
	},
	workflowtypes.NodeKindDestination: {
		// In DAG mode the destination node represents the full transfer step
		// (source -> destination) via the executor's data plane
		// (Kafka/MinIO/kafka-mcp-sink).
		RequestType:        "load",
		Operation:          "data_transfer",
		ConnectorTypeField: "destination_type",
	},
	workflowtypes.NodeKindTransform:    {RequestType: "transform"},
	workflowtypes.NodeKindAPICall:      {RequestType: "api_call"},
	workflowtypes.NodeKindNotification: {RequestType: "notification"},
	workflowtypes.NodeKindCondition:    {RequestType: "evaluate_condition"},
	workflowtypes.NodeKindLLM:          {RequestType: "llm_process"},
}

// applyNodeKindRouting writes the kind-specific fields of an executor task.
// It is the only thing in ExecuteGraphNodeActivityV2 that branches on the node
// kind, so calling it from a test observes exactly what the activity does.
func applyNodeKindRouting(task map[string]interface{}, nodeKind string, nodeConfig map[string]interface{}) {
	rule, ok := nodeDispatchRules[nodeKind]
	if !ok {
		task["request_type"] = genericNodeRequestType
		return
	}
	if rule.Operation != "" {
		task["operation"] = rule.Operation
	}
	task["request_type"] = rule.RequestType
	if rule.ConnectorTypeField != "" {
		// Reading a nil map is legal in Go, so a node with no config is fine.
		if connType, ok := nodeConfig["connector_type"].(string); ok {
			task[rule.ConnectorTypeField] = connType
		}
	}
}

// localNodeOutput resolves the node kinds the DAG scheduler answers itself,
// deterministically and without dispatching an activity. This prevents
// accidental full transfers for "source" nodes (which previously could fall
// back to direct transfer in the executor) and allows "transform" nodes to act
// as config producers (e.g. metadata.transforms) without needing a separate
// runtime.
//
// The returned bool IS the routing decision. There is deliberately no separate
// `if node.Kind == A || node.Kind == B` gate in front of this switch: a gate
// can be narrowed while an unreachable `case` is left behind, which reads like
// coverage and executes like nothing. Here, dropping a kind means deleting its
// case, and the census sees it immediately.
func localNodeOutput(nodeID string, node *workflowtypes.GraphNode, inputs map[string]interface{}) (map[string]interface{}, bool) {
	out := map[string]interface{}{}
	switch node.Kind {
	case workflowtypes.NodeKindSource:
		// Treat as a declarative node. Runtime transfer happens in the destination node
		// (operation=data_transfer) using connection IDs from workflow state.
		if node.Config != nil {
			if v, ok := node.Config["connector_type"]; ok && v != nil {
				out["connector_type"] = v
			}
			out["config"] = node.Config
		}
	case workflowtypes.NodeKindTransform:
		// Deterministically produce an *accumulated* transform list.
		//
		// The planner typically emits transform nodes in a chain:
		//   extract -> transform_a -> transform_b -> ... -> load
		//
		// The destination node only receives the immediate parent's output as a node_input,
		// so each transform node must carry forward upstream metadata.transforms.
		coerceList := func(raw interface{}) []interface{} {
			if raw == nil {
				return nil
			}
			switch vv := raw.(type) {
			case []interface{}:
				out := make([]interface{}, 0, len(vv))
				for _, it := range vv {
					if it != nil {
						out = append(out, it)
					}
				}
				return out
			case []map[string]interface{}:
				out := make([]interface{}, 0, len(vv))
				for _, it := range vv {
					if it != nil {
						out = append(out, it)
					}
				}
				return out
			case map[string]interface{}:
				return []interface{}{vv}
			default:
				// Unsupported shape; ignore.
				return nil
			}
		}

		// 1) Upstream transforms, if any.
		agg := make([]interface{}, 0, 8)
		if inputs != nil && len(inputs) > 0 {
			keys := make([]string, 0, len(inputs))
			for k := range inputs {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				v := inputs[k]
				m, ok := v.(map[string]interface{})
				if !ok || m == nil {
					continue
				}
				// Shape A: { metadata: { transforms: [...] } }
				if mdRaw, ok := m["metadata"]; ok && mdRaw != nil {
					if md, ok := mdRaw.(map[string]interface{}); ok && md != nil {
						if tRaw, ok := md["transforms"]; ok && tRaw != nil {
							agg = append(agg, coerceList(tRaw)...)
							continue
						}
					}
				}
				// Shape B: { transforms: [...] }
				if tRaw, ok := m["transforms"]; ok && tRaw != nil {
					agg = append(agg, coerceList(tRaw)...)
				}
			}
		}

		// 2) This node's transform(s).
		if node.Config != nil {
			if v, ok := node.Config["transforms"]; ok && v != nil {
				agg = append(agg, coerceList(v)...)
			} else {
				// Shorthand: treat node.Config as a single transform spec.
				spec := make(map[string]interface{}, len(node.Config)+1)
				for k, v := range node.Config {
					spec[k] = v
				}
				if _, ok := spec["type"]; !ok || strings.TrimSpace(fmt.Sprintf("%v", spec["type"])) == "" {
					if inferred := strings.TrimSpace(strings.TrimPrefix(nodeID, "transform_")); inferred != "" {
						spec["type"] = inferred
					}
				}
				agg = append(agg, spec)
			}
		}

		out["metadata"] = map[string]interface{}{"transforms": agg}
	default:
		return nil, false
	}
	return out, true
}
