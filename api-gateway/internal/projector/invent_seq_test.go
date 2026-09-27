package projector

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sync"
	"testing"
	"time"

	"github.com/rsync-ai/shared/kafkaclient"
)

// Readers order the timeline by (occurred_at, seq, event_id), and occurred_at is often
// whole-second, so seq decides most ties. A seq the projector invents has to sort on
// the same scale as the seq every producer stamps, or the invented rows all draw
// first within their second no matter when they arrived.

func newSeqProjector() *EventProjector {
	return &EventProjector{lastSeq: map[string]int64{}, gapSeen: map[string]bool{}}
}

// The prod shape (2026-09-26): an executor STAGE_PROGRESS with no seq arrives after an
// adapter event stamped earlier within the same whole second. It must sort after it.
func TestAnInventedSeqSortsOnTheProducersScale(t *testing.T) {
	p := newSeqProjector()
	second := time.Date(2026, 9, 26, 7, 30, 26, 0, time.UTC)
	stamped := kafkaclient.DomainEventSeq(second.Add(100 * time.Millisecond))

	invented := p.inventSeq("exec-1", second.Add(400*time.Millisecond))

	// Control: the counter this replaced would have handed the same event 1.
	if legacy := int64(1); legacy >= stamped {
		t.Fatal("control is wrong: a counter value should sort below a nanosecond stamp")
	}
	if invented <= stamped {
		t.Fatalf("invented seq %d sorts before a producer seq %d stamped 300ms earlier "+
			"in the same second; the event that arrived last is drawn first", invented, stamped)
	}
	if want := kafkaclient.DomainEventSeq(second.Add(400 * time.Millisecond)); invented != want {
		t.Fatalf("invented seq %d, want the producers' derivation of the arrival time %d", invented, want)
	}
}

func TestAnInventedSeqStillIncreasesWithinAnExecution(t *testing.T) {
	p := newSeqProjector()
	at := time.Date(2026, 9, 26, 7, 30, 26, 0, time.UTC)

	a := p.inventSeq("exec-1", at)
	b := p.inventSeq("exec-1", at)                        // same nanosecond
	c := p.inventSeq("exec-1", at.Add(-time.Millisecond)) // clock stepped back
	if !(a < b && b < c) {
		t.Fatalf("seqs %d, %d, %d are not strictly increasing in arrival order", a, b, c)
	}

	// Another execution is not pushed forward by this one's history.
	if other := p.inventSeq("exec-2", at); other != kafkaclient.DomainEventSeq(at) {
		t.Fatalf("exec-2 got %d, want its own arrival time %d", other, kafkaclient.DomainEventSeq(at))
	}
}

func TestInventSeqIsSafeUnderConcurrentProjection(t *testing.T) {
	p := newSeqProjector()
	at := time.Date(2026, 9, 26, 7, 30, 26, 0, time.UTC)

	const n = 64
	got := make(chan int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got <- p.inventSeq("exec-1", at)
		}()
	}
	wg.Wait()
	close(got)

	seen := map[int64]bool{}
	for v := range got {
		if seen[v] {
			t.Fatalf("seq %d handed out twice", v)
		}
		seen[v] = true
	}
}

// The tests above call inventSeq directly; this pins that storeRunEvent uses it, so
// the fallback cannot quietly go back to a counter of its own.
func TestStoreRunEventInventsSeqThroughInventSeq(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "event_projector.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse event_projector.go: %v", err)
	}
	var storeRunEvent *ast.FuncDecl
	ast.Inspect(file, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok && fn.Name.Name == "storeRunEvent" {
			storeRunEvent = fn
		}
		return storeRunEvent == nil
	})
	if storeRunEvent == nil {
		t.Fatal("storeRunEvent not found — renamed or moved, and this guard now proves nothing")
	}

	calls, directWrites := 0, 0
	ast.Inspect(storeRunEvent, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "inventSeq" {
				calls++
			}
		case *ast.SelectorExpr:
			if x.Sel.Name == "lastSeq" {
				directWrites++
			}
		}
		return true
	})
	if calls != 1 {
		t.Fatalf("storeRunEvent calls inventSeq %d time(s), want 1", calls)
	}
	if directWrites != 0 {
		t.Fatal("storeRunEvent touches lastSeq directly — a second seq derivation beside inventSeq")
	}
}
