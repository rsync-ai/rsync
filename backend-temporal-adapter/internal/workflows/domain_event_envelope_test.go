package workflows

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// The defect this guards against is not a wrong value — it is an absent one.
//
// This workflow emitted its events to pipeline.domain.events with no seq at all.
// The api-gateway projector, which folds these together with the orchestrator's
// events into one pipeline_run_events timeline, filled the hole with a
// per-execution counter: 1, 2, 3. The orchestrator supplied UnixNano. Both rows
// then had a populated seq column, so nothing anywhere looked broken, and every
// ORDER BY using seq as a tiebreaker sorted this workflow's entire stream
// underneath the other producer's.
//
// A single missed call site reintroduces exactly that, silently, so the rule is
// structural rather than behavioural: the activity is invoked in one place.

const emitHelperFile = "domain_event_envelope.go"

func TestEmitDomainEventActivityIsOnlyInvokedThroughTheHelper(t *testing.T) {
	sites, totalExecuteActivity := emitDomainEventCallSites(t)

	if totalExecuteActivity < 10 {
		t.Fatalf("the scan found only %d workflow.ExecuteActivity calls in this package — "+
			"this workflow has far more than that, so the scan is broken and a green "+
			"result here would mean nothing", totalExecuteActivity)
	}
	if len(sites) == 0 {
		t.Fatal("the scan found no EmitDomainEventActivity call at all — either the activity " +
			"was renamed or the scan is broken; a green result would mean nothing either way")
	}

	var offenders []string
	for _, site := range sites {
		if !strings.HasPrefix(site, emitHelperFile+":") {
			offenders = append(offenders, site)
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("%d call(s) reach EmitDomainEventActivity without going through "+
			"emitDomainEvent:\n  %s\n\nEvents emitted this way carry no seq, and the "+
			"api-gateway projector will invent one from its own namespace rather than "+
			"reject them — so this fails here or nowhere. Route the call through "+
			"emitDomainEvent(ctx, event) in %s.",
			len(offenders), strings.Join(offenders, "\n  "), emitHelperFile)
	}
}

// The helper is only worth having if the workflow actually uses it. If a refactor
// were to inline it back out, the test above would still pass on a package with
// one remaining call site inside the helper file and no callers.
func TestWorkflowRoutesItsEventsThroughTheHelper(t *testing.T) {
	const wantAtLeast = 5

	callers := 0
	walkPackageSource(t, func(path string, fset *token.FileSet, file *ast.File) {
		if filepath.Base(path) == emitHelperFile {
			return
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "emitDomainEvent" {
				callers++
			}
			return true
		})
	})

	if callers < wantAtLeast {
		t.Fatalf("only %d call(s) to emitDomainEvent outside %s; expected at least %d "+
			"(PIPELINE_CREATED, the two CONTROL_PLANE events, repair events, "+
			"PIPELINE_WAITING, stage events, PIPELINE_COMPLETED). Either events were "+
			"removed, or they were re-routed around the envelope.",
			callers, emitHelperFile, wantAtLeast)
	}
}

func TestStampDomainEventEnvelopeFillsSeqAndOccurredAt(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)

	t.Run("stamps seq in the same namespace as the other producer", func(t *testing.T) {
		event := map[string]interface{}{"event_type": "STAGE_STARTED"}
		stampDomainEventEnvelope(event, now)

		if got, want := event["seq"], kafkaclient.DomainEventSeq(now); got != want {
			t.Fatalf("seq = %v, want %v — the orchestrator derives seq this way, and the "+
				"two must be comparable", got, want)
		}
	})

	t.Run("prefers the producer's own timestamp for occurred_at", func(t *testing.T) {
		event := map[string]interface{}{"timestamp": "2026-08-17T11:59:00Z"}
		stampDomainEventEnvelope(event, now)

		if got := event["occurred_at"]; got != "2026-08-17T11:59:00Z" {
			t.Fatalf("occurred_at = %v, want the event's own timestamp", got)
		}
	})

	t.Run("falls back to the workflow clock, never to the projector's", func(t *testing.T) {
		event := map[string]interface{}{}
		stampDomainEventEnvelope(event, now)

		if got := event["occurred_at"]; got != "2026-08-17T12:00:00Z" {
			t.Fatalf("occurred_at = %v, want the workflow clock — otherwise the projector "+
				"orders the event by when it happened to poll", got)
		}
	})

	t.Run("never overwrites what the caller set", func(t *testing.T) {
		event := map[string]interface{}{
			"seq":         int64(7),
			"occurred_at": "2020-01-01T00:00:00Z",
		}
		stampDomainEventEnvelope(event, now)

		if event["seq"] != int64(7) || event["occurred_at"] != "2020-01-01T00:00:00Z" {
			t.Fatalf("stamping clobbered caller-supplied values: %v", event)
		}
	})

	t.Run("leaves event_id alone", func(t *testing.T) {
		// Deriving an id from this clock would be unsafe: workflow.Now does not
		// advance within a workflow task, so two events emitted without an
		// intervening yield would collide and the projector's
		// ON CONFLICT DO NOTHING would drop the second without a trace.
		event := map[string]interface{}{}
		stampDomainEventEnvelope(event, now)

		if _, present := event["event_id"]; present {
			t.Fatal("an event_id derived from workflow.Now() collides within a workflow " +
				"task and the projector silently drops the duplicate — see the rationale " +
				"in " + emitHelperFile)
		}
	})

	t.Run("tolerates a nil event", func(t *testing.T) {
		stampDomainEventEnvelope(nil, now) // must not panic
	})
}

// emitDomainEventCallSites returns every position where EmitDomainEventActivity is
// handed to workflow.ExecuteActivity, plus the total number of ExecuteActivity calls
// seen — the latter is the non-vacuity floor for the scan itself.
func emitDomainEventCallSites(t *testing.T) (sites []string, totalExecuteActivity int) {
	t.Helper()

	walkPackageSource(t, func(path string, fset *token.FileSet, file *ast.File) {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "ExecuteActivity" {
				return true
			}
			totalExecuteActivity++
			for _, arg := range call.Args {
				if id, ok := arg.(*ast.Ident); ok && id.Name == "EmitDomainEventActivity" {
					pos := fset.Position(call.Pos())
					sites = append(sites, fmt.Sprintf("%s:%d", filepath.Base(path), pos.Line))
					break
				}
			}
			return true
		})
	})
	return sites, totalExecuteActivity
}

func walkPackageSource(t *testing.T, fn func(path string, fset *token.FileSet, file *ast.File)) {
	t.Helper()

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		fn(name, fset, file)
	}
}
