package projector

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
)

// The projector invents an event_id and a seq when a producer omits them, and the
// invented values are individually plausible — a unique content hash, an increasing
// per-execution counter. That plausibility is what let the temporal adapter emit
// events with no seq for as long as it did: the rows looked identical to the
// orchestrator's, while sorting three orders of magnitude below them on every tie.
//
// Substituting is still the right behaviour (dropping telemetry would be worse), so
// the fix is to make the substitution audible. These tests pin that it says which
// producer and which field, and that it says so once rather than per event.

func TestEnvelopeGapNamesTheProducerAndTheField(t *testing.T) {
	p, hook := projectorWithLogHook(t)

	p.noteEnvelopeGap(map[string]interface{}{
		"schema_version": float64(2), // JSON numbers arrive as float64
		"event_type":     "STAGE_COMPLETED",
	}, "seq")

	entries := hook.AllEntries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 log line, got %d", len(entries))
	}
	msg := entries[0].Message
	for _, want := range []string{"seq", "STAGE_COMPLETED", "2"} {
		if !strings.Contains(msg, want) {
			t.Errorf("log line does not mention %q — an operator cannot tell which "+
				"producer is missing what:\n%s", want, msg)
		}
	}
}

func TestEnvelopeGapLogsOncePerProducerFieldAndEventType(t *testing.T) {
	p, hook := projectorWithLogHook(t)

	// The same gap, over and over — this is the steady state on a live topic, and
	// it is why an unconditional log here would be worse than no log at all.
	for i := 0; i < 50; i++ {
		p.noteEnvelopeGap(map[string]interface{}{
			"schema_version": float64(2),
			"event_type":     "STAGE_COMPLETED",
		}, "seq")
	}
	if got := len(hook.AllEntries()); got != 1 {
		t.Fatalf("50 identical gaps produced %d log lines, want 1 — a line per event on "+
			"this topic is a line per stage transition of every run", got)
	}

	// A different field, event type, or producer is a different fact and must be said.
	p.noteEnvelopeGap(map[string]interface{}{"schema_version": float64(2), "event_type": "STAGE_COMPLETED"}, "event_id")
	p.noteEnvelopeGap(map[string]interface{}{"schema_version": float64(2), "event_type": "STAGE_STARTED"}, "seq")
	p.noteEnvelopeGap(map[string]interface{}{"schema_version": float64(1), "event_type": "STAGE_COMPLETED"}, "seq")

	if got := len(hook.AllEntries()); got != 4 {
		t.Fatalf("got %d log lines, want 4 — distinct (producer, event_type, field) gaps "+
			"are distinct facts and collapsing them hides one behind another", got)
	}
}

func TestEnvelopeGapIsSafeUnderConcurrentProjection(t *testing.T) {
	// storeRunEvent runs on the consumer goroutine today, but lastSeq already carries
	// its own mutex for the same reason; an unsynchronised map here would be a data
	// race waiting for the first concurrent projector.
	p, _ := projectorWithLogHook(t)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.noteEnvelopeGap(map[string]interface{}{
				"schema_version": float64(2),
				"event_type":     "STAGE_COMPLETED",
			}, "seq")
		}()
	}
	wg.Wait()
}

func TestEnvelopeGapSurvivesAMalformedEvent(t *testing.T) {
	// Anything can land on a Kafka topic. A panic here would kill the projector loop
	// and stop the timeline for every pipeline, to report a missing field.
	p, hook := projectorWithLogHook(t)

	p.noteEnvelopeGap(map[string]interface{}{}, "seq")
	p.noteEnvelopeGap(map[string]interface{}{"schema_version": nil, "event_type": nil}, "seq")
	p.noteEnvelopeGap(nil, "seq")

	if len(hook.AllEntries()) == 0 {
		t.Fatal("a malformed event with a missing envelope logged nothing at all")
	}
}

// The tests above call noteEnvelopeGap directly, which proves it behaves but not
// that anything invokes it. Wiring it up is the entire point: an unreachable
// audit line is indistinguishable from no audit line, and the fallbacks it guards
// are exactly the code that has already been silent once.
func TestBothEnvelopeFallbacksReportTheirGap(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "event_projector.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse event_projector.go: %v", err)
	}

	var storeRunEvent *ast.FuncDecl
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if ok && fn.Name.Name == "storeRunEvent" {
			storeRunEvent = fn
		}
		return storeRunEvent == nil
	})
	if storeRunEvent == nil {
		t.Fatal("storeRunEvent not found — it was renamed or moved, and this guard now " +
			"proves nothing")
	}

	calls := 0
	ast.Inspect(storeRunEvent, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "noteEnvelopeGap" {
			calls++
		}
		return true
	})

	if calls < 2 {
		t.Fatalf("storeRunEvent calls noteEnvelopeGap %d time(s); expected one per invented "+
			"envelope field (event_id and seq). A fallback that fires without reporting is "+
			"the original defect.", calls)
	}
}

func projectorWithLogHook(t *testing.T) (*EventProjector, *test.Hook) {
	t.Helper()

	logger := log.StandardLogger()
	prevOut, prevLevel := logger.Out, logger.Level
	hook := test.NewLocal(logger)
	logger.SetLevel(log.InfoLevel)
	logger.SetOutput(newDiscard())
	t.Cleanup(func() {
		logger.SetOutput(prevOut)
		logger.SetLevel(prevLevel)
	})

	return &EventProjector{}, hook
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

func newDiscard() discard { return discard{} }
