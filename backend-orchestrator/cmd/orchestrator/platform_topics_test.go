package main

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"
)

// TestEnsurePlatformTopicsWithRetryRetriesThenSucceeds: a broker that refuses the
// first two CreateTopics rounds (controller still electing) must not leave the
// platform topics to consumer-group auto-creation. The helper keeps trying, with a
// doubling backoff, and reports success once a round succeeds.
func TestEnsurePlatformTopicsWithRetryRetriesThenSucceeds(t *testing.T) {
	calls := 0
	var slept []time.Duration
	err := ensurePlatformTopicsWithRetry(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("controller not available")
		}
		return nil
	}, 5, time.Second, func(d time.Duration) { slept = append(slept, d) })

	if err != nil {
		t.Fatalf("err = %v, want nil after a successful third attempt", err)
	}
	if calls != 3 {
		t.Fatalf("ensure called %d times, want 3 (two failures then success)", calls)
	}
	want := []time.Duration{time.Second, 2 * time.Second}
	if len(slept) != len(want) || slept[0] != want[0] || slept[1] != want[1] {
		t.Fatalf("backoff = %v, want %v (doubling, and no sleep after the success)", slept, want)
	}
}

// TestEnsurePlatformTopicsWithRetryIsBounded: a broker that never accepts must not
// hold startup forever. The helper stops after the configured attempts, does not
// sleep after the last one, and returns the last error so the caller can log it.
func TestEnsurePlatformTopicsWithRetryIsBounded(t *testing.T) {
	calls := 0
	sleeps := 0
	last := errors.New("platform topic rsync.notifications: denied")
	err := ensurePlatformTopicsWithRetry(context.Background(), func(context.Context) error {
		calls++
		return last
	}, 4, time.Millisecond, func(time.Duration) { sleeps++ })

	if calls != 4 {
		t.Fatalf("ensure called %d times, want exactly 4", calls)
	}
	if sleeps != 3 {
		t.Fatalf("slept %d times, want 3 (none after the final attempt)", sleeps)
	}
	if !errors.Is(err, last) {
		t.Fatalf("err = %v, want it to wrap the last ensure error", err)
	}
}

// TestEnsurePlatformTopicsWithRetryStopsOnCancel: a cancelled startup context ends the
// retry loop at once instead of sleeping through the remaining attempts.
func TestEnsurePlatformTopicsWithRetryStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := ensurePlatformTopicsWithRetry(ctx, func(context.Context) error {
		calls++
		return errors.New("context canceled")
	}, 5, time.Second, func(time.Duration) { t.Fatal("slept after the context was cancelled") })
	if err == nil || calls != 1 {
		t.Fatalf("calls = %d, err = %v; want one call and an error", calls, err)
	}
}

// TestMainEnsuresPlatformTopicsBeforeAnyConsumerStarts pins the startup ORDER in
// main(): the platform-topic ensure must run before every Start call (the workers,
// the sentinel agents, the cdcstats agent, the probes). A consumer-group subscription
// auto-creates the topic it joins at the broker's defaults, and EnsurePlatformTopics
// never re-sizes a topic that already exists, so a consumer started first would fix
// the topic's geometry for good.
func TestMainEnsuresPlatformTopicsBeforeAnyConsumerStarts(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	var mainFn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.Name == "main" {
			mainFn = fd
		}
	}
	if mainFn == nil {
		t.Fatal("no func main in main.go")
	}

	ensurePos := token.NoPos
	var starts []token.Pos
	var startNames []string
	ast.Inspect(mainFn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			if fn.Name == "ensurePlatformTopicsWithRetry" && ensurePos == token.NoPos {
				ensurePos = call.Pos()
			}
		case *ast.SelectorExpr:
			if fn.Sel.Name == "Start" {
				starts = append(starts, call.Pos())
				name := "?"
				if x, ok := fn.X.(*ast.Ident); ok {
					name = x.Name
				}
				startNames = append(startNames, name)
			}
			if fn.Sel.Name == "EnsureAgentControlTopics" {
				t.Errorf("main.go still calls EnsureAgentControlTopics at %s", fset.Position(call.Pos()))
			}
		}
		return true
	})

	if ensurePos == token.NoPos {
		t.Fatal("main() never calls ensurePlatformTopicsWithRetry: the platform topics " +
			"would be created by whichever consumer subscribes first, at broker defaults")
	}
	if len(starts) < 5 {
		t.Fatalf("found %d .Start( calls in main(), want at least 5; the order check "+
			"would be vacuous", len(starts))
	}
	var early []string
	for i, p := range starts {
		if p < ensurePos {
			early = append(early, startNames[i]+" at "+fset.Position(p).String())
		}
	}
	if len(early) > 0 {
		t.Errorf("these Start calls run before ensurePlatformTopicsWithRetry: %s",
			strings.Join(early, ", "))
	}
}
