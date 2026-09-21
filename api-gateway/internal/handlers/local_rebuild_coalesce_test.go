package handlers

import (
	"database/sql"
	"sync"
	"testing"
	"time"
)

// stubLocalRebuild swaps the rebuild for a recorder, and returns what it saw.
type localRun struct {
	upstream  string
	coalesced int
}

func stubLocalRebuild(t *testing.T, during func(n int)) (*[]localRun, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	runs := []localRun{}
	orig, origPolicy := runLocalRebuildFn, waitingOnUpstreamPolicyFn
	waitingOnUpstreamPolicyFn = func(*sql.DB, modelRefreshTarget, modelRefreshSource, int) bool { return false }
	runLocalRebuildFn = func(_ *sql.DB, _ modelRefreshTarget, src modelRefreshSource, coalesced int) {
		mu.Lock()
		runs = append(runs, localRun{src.ID, coalesced})
		n := len(runs)
		mu.Unlock()
		if during != nil {
			during(n)
		}
	}
	t.Cleanup(func() {
		runLocalRebuildFn, waitingOnUpstreamPolicyFn = orig, origPolicy
		localRebuilds.Lock()
		localRebuilds.m = map[string]*localRebuild{}
		localRebuilds.Unlock()
	})
	return &runs, &mu
}

// The live failure (B10b): a fan-in model woken by its sibling upstream while a rebuild
// woken by the root is still running. Before, the second trigger lost the run lock and
// was dropped; now the running rebuild absorbs it and rebuilds once more.
func TestRebuildLocally_TriggerDuringRunRebuildsAgainWithLatestSource(t *testing.T) {
	target := modelRefreshTarget{ModelID: "d_fanin"}
	started := make(chan struct{})
	release := make(chan struct{})
	runs, mu := stubLocalRebuild(t, func(n int) {
		if n == 1 {
			close(started)
			<-release
		}
	})

	done := make(chan struct{})
	go func() {
		rebuildLocally(nil, target, modelRefreshSource{ID: "stg_orders"})
		close(done)
	}()
	<-started

	// Two more completions land while the first rebuild is in flight: neither may run
	// concurrently, and both collapse into one further rebuild naming the latest.
	rebuildLocally(nil, target, modelRefreshSource{ID: "orders_daily"})
	rebuildLocally(nil, target, modelRefreshSource{ID: "orders_daily_again"})
	mu.Lock()
	if len(*runs) != 1 {
		t.Fatalf("a trigger during a run started a concurrent rebuild: %v", *runs)
	}
	mu.Unlock()

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("rebuildLocally did not finish")
	}

	want := []localRun{{"stg_orders", 1}, {"orders_daily_again", 2}}
	if len(*runs) != len(want) {
		t.Fatalf("runs = %v, want %v", *runs, want)
	}
	for i := range want {
		if (*runs)[i] != want[i] {
			t.Fatalf("runs = %v, want %v", *runs, want)
		}
	}
	if !claimLocalRebuild(target.ModelID, modelRefreshSource{}) {
		t.Fatal("claim was not released after the burst drained")
	}
}

// Control: no overlap, no extra rebuild.
func TestRebuildLocally_SequentialTriggersEachRunOnce(t *testing.T) {
	target := modelRefreshTarget{ModelID: "m"}
	runs, _ := stubLocalRebuild(t, nil)
	rebuildLocally(nil, target, modelRefreshSource{ID: "a"})
	rebuildLocally(nil, target, modelRefreshSource{ID: "b"})
	if len(*runs) != 2 || (*runs)[0] != (localRun{"a", 1}) || (*runs)[1] != (localRun{"b", 1}) {
		t.Fatalf("runs = %v", *runs)
	}
}

// Other models are not held up by a run of this one.
func TestRebuildLocally_ClaimIsPerModel(t *testing.T) {
	stubLocalRebuild(t, nil)
	if !claimLocalRebuild("x", modelRefreshSource{}) {
		t.Fatal("first claim refused")
	}
	if !claimLocalRebuild("y", modelRefreshSource{}) {
		t.Fatal("a run of x blocked y")
	}
}

// A panicking rebuild runs on a background goroutine, where an escaped panic ends the
// gateway. It must be recovered there, and must not leave the model claimed, or every
// later completion of it would be absorbed by a run that no longer exists.
func TestRebuildLocally_PanicIsRecoveredAndReleasesClaim(t *testing.T) {
	stubLocalRebuild(t, func(int) { panic("boom") })
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Fatalf("the panic escaped rebuildLocally: %v", p)
			}
		}()
		rebuildLocally(nil, modelRefreshTarget{ModelID: "p"}, modelRefreshSource{ID: "a"})
	}()
	if !claimLocalRebuild("p", modelRefreshSource{}) {
		t.Fatal("claim leaked after a panic")
	}
}

// A rerun is a new trigger: under 'all' the policy is asked again, and a rerun it refuses
// does not rebuild. The first run is not re-asked; its caller already did.
func TestRebuildLocally_RerunMeetsThePolicyAgain(t *testing.T) {
	target := modelRefreshTarget{ModelID: "fanin"}
	asked := []string{}
	runs, _ := stubLocalRebuild(t, func(n int) {
		if n == 1 {
			rebuildLocally(nil, target, modelRefreshSource{ID: "sibling"})
		}
	})
	waitingOnUpstreamPolicyFn = func(_ *sql.DB, _ modelRefreshTarget, src modelRefreshSource, coalesced int) bool {
		asked = append(asked, src.ID)
		return true
	}
	rebuildLocally(nil, target, modelRefreshSource{ID: "root"})
	if len(*runs) != 1 || (*runs)[0].upstream != "root" {
		t.Fatalf("runs = %v, want only the first", *runs)
	}
	if len(asked) != 1 || asked[0] != "sibling" {
		t.Fatalf("policy asked for %v, want only the rerun's source", asked)
	}
	if !claimLocalRebuild(target.ModelID, modelRefreshSource{}) {
		t.Fatal("claim not released after a refused rerun")
	}
}

// An upstream that keeps completing must not hold one fire slot forever: past the bound
// the next rebuild is handed to a goroutine that queues for a slot, and still runs.
func TestRebuildLocally_EndlessCompletionsAreRequeuedNotDropped(t *testing.T) {
	target := modelRefreshTarget{ModelID: "hot"}
	handedOff := make(chan struct{})
	var once sync.Once
	runs, mu := stubLocalRebuild(t, func(n int) {
		switch {
		case n < maxLocalRebuildRuns:
			rebuildLocally(nil, target, modelRefreshSource{ID: "again"})
		case n == maxLocalRebuildRuns:
			rebuildLocally(nil, target, modelRefreshSource{ID: "after-bound"})
		default:
			once.Do(func() { close(handedOff) })
		}
	})

	rebuildLocally(nil, target, modelRefreshSource{ID: "first"})
	mu.Lock()
	if len(*runs) != maxLocalRebuildRuns {
		t.Fatalf("ran %d times in one call, want the bound %d", len(*runs), maxLocalRebuildRuns)
	}
	mu.Unlock()

	select {
	case <-handedOff:
	case <-time.After(5 * time.Second):
		t.Fatal("the completion past the bound was dropped instead of requeued")
	}
	mu.Lock()
	last := (*runs)[len(*runs)-1]
	mu.Unlock()
	if last.upstream != "after-bound" {
		t.Fatalf("requeued rebuild = %v, want after-bound", last)
	}
}
