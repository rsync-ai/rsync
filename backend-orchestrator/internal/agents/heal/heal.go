// Package heal — Phase 4 of the strategic plan.
//
// Healer takes a Diagnosis (from Phase 3) and decides:
//
//	confidence >= AutoBand  → auto-execute the action
//	confidence >= HITLBand  → ask the operator first
//	confidence <  HITLBand  → escalate to a human
//
// The action set is closed and small — every concrete action is
// represented by an Executor that the Healer dispatches into. v1 ships
// the dispatch logic + the decision band; the concrete Executor
// implementations (regenerate, refresh_auth, backoff, request_config)
// are wired in by callers in subsequent commits.
//
// Why split from diagnose? So the Healer can be wired into a worker
// loop that consumes diagnoses asynchronously, while Diagnose itself
// stays a pure inspection step that's easy to call from multiple
// places (executor on failure, periodic background sweep, etc.).
package heal

import (
	"context"
	"errors"
	"fmt"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// Decision bands. Calibrated to bias toward HITL on first iteration —
// the cost of an unprompted wrong heal is high; the cost of asking the
// user one extra question is low. As DiagnoseAgent's confidence
// calibrates (Phase 3 eval), these can move.
//
// AutoBand is the single source of truth diagnose.AutoExecuteBand — the LLM
// diagnoser derives its confidence cap from the same constant so an LLM verdict
// can never reach this band. Keep it referencing diagnose, not a bare literal.
const (
	AutoBand = diagnose.AutoExecuteBand
	HITLBand = 0.50
)

// Outcome of a heal attempt.
type Outcome string

const (
	OutcomeAutoExecuted    Outcome = "auto_executed"
	OutcomeHITLRequested   Outcome = "hitl_requested"
	OutcomeEscalated       Outcome = "escalated"
	OutcomeActionFailed    Outcome = "action_failed"
	OutcomeNoActionDefined Outcome = "no_action_defined"
)

// HealResult is what the Healer returns. ActionExecuted == true only
// in the auto-executed and the post-HITL-approval paths.
type HealResult struct {
	Outcome        Outcome
	Action         diagnose.Action
	ActionExecuted bool
	HITLPrompt     string // populated when Outcome == OutcomeHITLRequested
	Rationale      string // copied from diagnosis for downstream display
	Error          error  // populated when Outcome == OutcomeActionFailed
}

// Executor performs one action. Implementations call out to the real
// systems (tool-generator, MCP, etc.); the Healer just dispatches.
//
// HITLSafe distinguishes "transition UI to waiting_for_user" executors
// (regenerate_connector, request_user_config — safe to run on medium
// confidence because they ASK the user) from "actually do the thing"
// executors (backoff_retry — only run on high confidence).
type Executor interface {
	Action() diagnose.Action
	Run(ctx context.Context, signal diagnose.Signal) error
	HITLSafe() bool
}

// Healer dispatches diagnoses to actions.
type Healer struct {
	// Registry of action -> executor. Built via Register.
	executors map[diagnose.Action]Executor

	// AutoBand / HITLBand can be overridden for tests or per-pipeline
	// risk tuning. Default to the package constants.
	AutoBand, HITLBand float64
}

func New() *Healer {
	return &Healer{
		executors: map[diagnose.Action]Executor{},
		AutoBand:  AutoBand,
		HITLBand:  HITLBand,
	}
}

func (h *Healer) Register(exec Executor) {
	h.executors[exec.Action()] = exec
}

// Heal evaluates a diagnosis and either executes, asks, or escalates.
func (h *Healer) Heal(ctx context.Context, sig diagnose.Signal, dx diagnose.Diagnosis) HealResult {
	res := HealResult{
		Action:    dx.SuggestedAction,
		Rationale: dx.Rationale,
	}

	// Escalation cases come first — they don't depend on the executor
	// registry.
	if dx.SuggestedAction == diagnose.ActionEscalate {
		res.Outcome = OutcomeEscalated
		return res
	}
	if dx.SuggestedAction == diagnose.ActionNoOp {
		res.Outcome = OutcomeNoActionDefined
		return res
	}

	exec, ok := h.executors[dx.SuggestedAction]
	if !ok {
		// Action wasn't wired up — escalate so it doesn't silently
		// no-op. Operators should see this in the run detail page.
		res.Outcome = OutcomeNoActionDefined
		res.Error = fmt.Errorf("no executor registered for action %q", dx.SuggestedAction)
		return res
	}

	switch {
	case dx.Confidence >= h.AutoBand:
		// High confidence — fire and report.
		if err := exec.Run(ctx, sig); err != nil {
			res.Outcome = OutcomeActionFailed
			res.Error = err
			return res
		}
		res.Outcome = OutcomeAutoExecuted
		res.ActionExecuted = true
		return res
	case dx.Confidence >= h.HITLBand:
		// Medium confidence — call the executor, but classify as HITL.
		// For our current action set, the "executor" for HITL-friendly
		// actions (regenerate_connector, request_user_config, refresh_auth)
		// transitions the pipeline to `waiting_for_user` with a structured
		// blocking_reason — that IS the HITL prompt the UI surfaces.
		//
		// BackoffRetry is the one non-HITLSafe executor, and it DOES reach
		// this branch: the orchestration rules (workflow-gone 0.8,
		// swept-zombie 0.75 — pkg/diagnose/diagnose.go) sit in the band on
		// purpose, because starting a pipeline run is a real side effect that
		// nothing has authorised the healer to perform unattended. The result
		// is a recorded recommendation the operator can act on, surfaced by
		// the self-healing panel — not a silent no-op. Do not "fix" this by
		// raising those confidences without deciding that autonomous runs are
		// wanted.
		if !exec.HITLSafe() {
			res.Outcome = OutcomeHITLRequested
			res.HITLPrompt = fmt.Sprintf(
				"Suggested: %s — %s (confidence %.0f%%). Approve?",
				dx.SuggestedAction, dx.Rationale, dx.Confidence*100,
			)
			return res
		}
		if err := exec.Run(ctx, sig); err != nil {
			res.Outcome = OutcomeActionFailed
			res.Error = err
			return res
		}
		res.Outcome = OutcomeHITLRequested
		res.ActionExecuted = true
		res.HITLPrompt = fmt.Sprintf(
			"Healer set pipeline to waiting_for_user (%s, confidence %.0f%%)",
			dx.SuggestedAction, dx.Confidence*100,
		)
		return res
	default:
		// Low confidence — surface to a human without taking action.
		res.Outcome = OutcomeEscalated
		return res
	}
}

// ApproveHITL runs the action that a Heal call previously deferred via
// OutcomeHITLRequested. Returns the post-action outcome.
func (h *Healer) ApproveHITL(ctx context.Context, sig diagnose.Signal, dx diagnose.Diagnosis) HealResult {
	exec, ok := h.executors[dx.SuggestedAction]
	if !ok {
		return HealResult{
			Outcome:   OutcomeNoActionDefined,
			Action:    dx.SuggestedAction,
			Rationale: dx.Rationale,
			Error:     errors.New("no executor registered for action"),
		}
	}
	if err := exec.Run(ctx, sig); err != nil {
		return HealResult{
			Outcome:   OutcomeActionFailed,
			Action:    dx.SuggestedAction,
			Rationale: dx.Rationale,
			Error:     err,
		}
	}
	return HealResult{
		Outcome:        OutcomeAutoExecuted,
		Action:         dx.SuggestedAction,
		ActionExecuted: true,
		Rationale:      dx.Rationale,
	}
}
