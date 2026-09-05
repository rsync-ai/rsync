package heal

import (
	"context"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// CleanupCDC tests use the CleanupFn hook so we don't need a real PG manager.

func TestCleanupCDC_RequiresPipelineID(t *testing.T) {
	e := &CleanupCDCResourcesExecutor{DB: nil, CleanupFn: nil}
	err := e.Run(context.Background(), diagnose.Signal{})
	if err == nil {
		t.Fatal("expected error when PipelineID is empty")
	}
}

// The nil-hook and failing-hook cases are asserted through Heal in
// auto_executors_outcome_test.go, because what matters is the outcome that
// reaches the ledger, not Run's return value in isolation.
//
// Two tests used to live here asserting the opposite — that a nil hook and a
// failed hook both return nil — on the grounds that erroring would "crash the
// healer worker". It does not: heal.go captures Run's error into the
// HealResult. They were pinning the bug KI-HEAL-NIL-HOOK-REPORTS-SUCCESS
// describes, and passed green the whole time it was live.

func TestCleanupCDC_CallsCleanupFn(t *testing.T) {
	called := false
	e := &CleanupCDCResourcesExecutor{
		DB: nil,
		CleanupFn: func(_ context.Context, pid string) error {
			called = true
			if pid != "p1" {
				t.Errorf("wrong pipeline id passed: %s", pid)
			}
			return nil
		},
	}
	err := e.Run(context.Background(), diagnose.Signal{PipelineID: "p1"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("CleanupFn was never invoked")
	}
}

func TestRepairOwnership_RequiresPipelineID(t *testing.T) {
	e := &RepairOwnershipRowExecutor{}
	err := e.Run(context.Background(), diagnose.Signal{})
	if err == nil {
		t.Fatal("expected error when PipelineID is empty")
	}
}

func TestRepairOwnership_CallsRepairFn(t *testing.T) {
	called := false
	e := &RepairOwnershipRowExecutor{
		RepairFn: func(_ context.Context, pid string) error {
			called = true
			if pid != "p1" {
				t.Errorf("wrong pipeline id: %s", pid)
			}
			return nil
		},
	}
	_ = e.Run(context.Background(), diagnose.Signal{PipelineID: "p1"})
	if !called {
		t.Error("RepairFn was never invoked")
	}
}

func TestActions_AreUniqueAndStable(t *testing.T) {
	// The 3 new actions must be distinct from each other and from the
	// existing 4 actions. Catches accidental enum collisions.
	all := []diagnose.Action{
		diagnose.ActionRefreshAuth,
		diagnose.ActionBackoffRetry,
		diagnose.ActionRegenerateConnector,
		diagnose.ActionRequestUserConfig,
		diagnose.ActionEscalate,
		diagnose.ActionNoOp,
		ActionCleanupCDCResources,
		ActionRepairOwnershipRow,
		ActionSweepZombie,
	}
	seen := make(map[diagnose.Action]bool)
	for _, a := range all {
		if seen[a] {
			t.Errorf("duplicate action value: %q", a)
		}
		seen[a] = true
	}
}

func TestZombieSweeper_ActionMatches(t *testing.T) {
	s := &ZombieExecutionSweeper{}
	if s.Action() != ActionSweepZombie {
		t.Errorf("got %q, want %q", s.Action(), ActionSweepZombie)
	}
	if !s.HITLSafe() {
		t.Error("sweeper should be HITL-safe (idempotent + reversible)")
	}
}
