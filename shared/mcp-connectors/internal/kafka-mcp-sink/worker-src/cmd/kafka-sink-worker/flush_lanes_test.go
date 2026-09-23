package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// settle is how long a test waits before concluding that something it expects NOT
// to happen has not happened. Long enough that a machine under load does not make
// the test lie, short enough that the suite stays quick.
const settle = 250 * time.Millisecond

// arrive is how long a test waits for something it expects TO happen. Generous:
// exceeding it means broken, not slow.
const arrive = 5 * time.Second

func TestOffsetSpaceOf(t *testing.T) {
	// The first two fields of a batcher key are topic and partition, and a Kafka
	// topic name cannot contain '|', so the prefix is unambiguous. The last two
	// cases are the fallback: too few separators to name a space, so the key is
	// its own space — conservative (over-serialised), never wrong.
	cases := []struct {
		key  string
		want string
	}{
		{"cdc.public.orders|0|orders", "cdc.public.orders|0"},
		{"cdc.public.orders|11|orders", "cdc.public.orders|11"},
		{"cdc.public.orders|0|orders|v2|2026-09-22", "cdc.public.orders|0"},
		{"cdc.public.orders|0|", "cdc.public.orders|0"},
		{"cdc.public.orders|0", "cdc.public.orders|0"},
		{"cdc.public.orders", "cdc.public.orders"},
		{"", ""},
	}
	for _, c := range cases {
		if got := offsetSpaceOf(c.key); got != c.want {
			t.Errorf("offsetSpaceOf(%q) = %q, want %q", c.key, got, c.want)
		}
	}

	// The property the lanes actually rely on: two keys differ in their offset
	// space iff they come from different (topic, partition) pairs. Two tables on
	// one partition MUST agree, or their offsets interleave across lanes.
	if offsetSpaceOf("t|0|orders") != offsetSpaceOf("t|0|customers") {
		t.Error("two tables on one partition got different offset spaces; their offsets would interleave across lanes")
	}
	if offsetSpaceOf("t|0|orders") == offsetSpaceOf("t|1|orders") {
		t.Error("two partitions of one topic got the same offset space; they would never overlap")
	}
}

func TestNewFlushLanes_BelowTwoMeansInlineFlushing(t *testing.T) {
	for _, n := range []int{-1, 0, 1} {
		if f := newFlushLanes(n); f != nil {
			f.close()
			t.Fatalf("newFlushLanes(%d) = %v, want nil so flushes run inline as before", n, f)
		}
	}

	// A nil *flushLanes must be usable without a nil check, and submit must run the
	// job on the CALLING goroutine — that is what "inline, exactly as before" means.
	// A plain int, not an atomic: if submit ever handed this off, -race would say so.
	var f *flushLanes
	ran := 0
	f.submit("t|0|x", func() { ran++ })
	if ran != 1 {
		t.Fatalf("nil lanes ran the job %d times on the caller, want 1 (inline)", ran)
	}
	f.waitKey("t|0|x")
	f.waitAll()
	f.close()
}

func TestNewFlushLanes_ClampsToMax(t *testing.T) {
	f := newFlushLanes(maxFlushLanes + 100)
	defer f.close()
	if got := len(f.lanes); got != maxFlushLanes {
		t.Fatalf("newFlushLanes(%d) built %d lanes, want the %d cap", maxFlushLanes+100, got, maxFlushLanes)
	}
}

func TestFlushLanes_OneOffsetSpaceAlwaysGetsOneLane(t *testing.T) {
	f := newFlushLanes(8)
	defer f.close()
	want := f.laneFor("cdc.public.orders|3|orders")
	for _, key := range []string{
		"cdc.public.orders|3|orders",
		"cdc.public.orders|3|customers",
		"cdc.public.orders|3|orders|v2|x",
	} {
		if got := f.laneFor(key); got != want {
			t.Fatalf("laneFor(%q) picked a different lane from its offset space's; offsets would complete out of order", key)
		}
	}
}

// twoKeysOnDistinctLanes returns two batcher keys that hash to different lanes.
func twoKeysOnDistinctLanes(t *testing.T, f *flushLanes) (string, string) {
	t.Helper()
	first := "cdc.t|0|x"
	for i := 1; i < 1000; i++ {
		k := fmt.Sprintf("cdc.t|%d|x", i)
		if f.laneFor(k) != f.laneFor(first) {
			return first, k
		}
	}
	t.Fatal("could not find two keys on distinct lanes")
	return "", ""
}

func TestFlushLanes_DifferentOffsetSpacesRunAtTheSameTime(t *testing.T) {
	f := newFlushLanes(4)
	defer f.close()
	a, b := twoKeysOnDistinctLanes(t, f)

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	block := func() { entered <- struct{}{}; <-release }
	f.submit(a, block)
	f.submit(b, block)

	// Both must be inside their job at once. If lanes did not overlap, the second
	// would still be queued behind the first, which is blocked forever.
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(arrive):
			close(release)
			t.Fatal("only one flush was running; lanes for different offset spaces did not overlap, which is the whole point")
		}
	}
	close(release)
	f.waitAll()
}

// TestFlushLanes_OneOffsetSpaceIsSerial is the control for the test above: the
// same probe, with both keys in ONE offset space, must NOT see the overlap. Without
// it, a "lanes" pool that ignored the key entirely would pass the overlap test.
func TestFlushLanes_OneOffsetSpaceIsSerial(t *testing.T) {
	f := newFlushLanes(4)
	defer f.close()

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	block := func() { entered <- struct{}{}; <-release }
	f.submit("cdc.t|0|orders", block)
	f.submit("cdc.t|0|customers", block)

	select {
	case <-entered:
	case <-time.After(arrive):
		close(release)
		t.Fatal("no job started at all")
	}
	select {
	case <-entered:
		close(release)
		t.Fatal("two flushes from one offset space ran at once; their offsets can then complete out of order, and both offset records keep only max(offset)")
	case <-time.After(settle):
	}
	close(release)
	f.waitAll()
}

func TestFlushLanes_OneOffsetSpaceCompletesInSubmitOrder(t *testing.T) {
	f := newFlushLanes(4)
	defer f.close()

	const n = 50
	// A plain slice with no synchronisation on purpose: if a lane ever ran two of
	// its own jobs concurrently, -race reports it here rather than flaking.
	var order []int
	for i := 0; i < n; i++ {
		i := i
		f.submit(fmt.Sprintf("cdc.t|0|table-%d", i), func() { order = append(order, i) })
	}
	f.waitAll()

	if len(order) != n {
		t.Fatalf("ran %d jobs, want %d", len(order), n)
	}
	for i, got := range order {
		if got != i {
			t.Fatalf("job %d completed at position %d; offsets in one partition must complete in ascending order", got, i)
		}
	}
}

func TestFlushLanes_WaitKeyDrainsOnlyItsOwnLane(t *testing.T) {
	f := newFlushLanes(4)
	defer f.close()
	other, mine := twoKeysOnDistinctLanes(t, f)

	otherStarted := make(chan struct{})
	release := make(chan struct{})
	var otherDone, mineDone int32
	f.submit(other, func() {
		close(otherStarted)
		<-release
		atomic.StoreInt32(&otherDone, 1)
	})
	<-otherStarted
	f.submit(mine, func() { atomic.StoreInt32(&mineDone, 1) })

	returned := make(chan struct{})
	go func() { f.waitKey(mine); close(returned) }()
	select {
	case <-returned:
	case <-time.After(arrive):
		close(release)
		t.Fatal("waitKey blocked on a lane that does not own its key; it must drain one offset space, not the world")
	}
	if atomic.LoadInt32(&mineDone) != 1 {
		t.Fatal("waitKey returned before its own lane's job finished")
	}
	if atomic.LoadInt32(&otherDone) != 0 {
		t.Fatal("the other lane's job finished; the probe never proved waitKey was selective")
	}

	close(release)
	f.waitAll()
	if atomic.LoadInt32(&otherDone) != 1 {
		t.Fatal("waitAll returned with a lane still running")
	}
}

func TestResolveFlushLaneCount(t *testing.T) {
	// The literal name, not the constant: this is what an operator types and what a
	// compose file sets, so a rename must fail here.
	const envName = "RSYNC_SINK_FLUSH_LANES"
	if EnvSinkFlushLanes != envName {
		t.Fatalf("EnvSinkFlushLanes = %q, want %q", EnvSinkFlushLanes, envName)
	}

	cases := []struct {
		name string
		set  bool
		val  string
		want int
	}{
		{name: "unset", want: defaultFlushLanes},
		{name: "empty", set: true, val: "", want: defaultFlushLanes},
		{name: "blank", set: true, val: "   ", want: defaultFlushLanes},
		{name: "explicit", set: true, val: "6", want: 6},
		{name: "padded", set: true, val: " 6 ", want: 6},
		{name: "disabled", set: true, val: "0", want: 0},
		{name: "one", set: true, val: "1", want: 1},
		{name: "negative", set: true, val: "-2", want: defaultFlushLanes},
		{name: "malformed", set: true, val: "four", want: defaultFlushLanes},
		{name: "over max", set: true, val: "1000", want: maxFlushLanes},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// t.Setenv restores the previous value at the end of the subtest,
			// including the "was not set at all" case.
			t.Setenv(envName, c.val)
			if !c.set {
				if err := os.Unsetenv(envName); err != nil {
					t.Fatalf("Unsetenv: %v", err)
				}
			}
			if got := resolveFlushLaneCount(); got != c.want {
				t.Fatalf("resolveFlushLaneCount() with %s=%q = %d, want %d", envName, c.val, got, c.want)
			}
		})
	}
}

// TestCDCDBBatcher_FlushTableWaitsForAnEarlierFlushOnTheSameKey is the ordering
// barrier that makes deletes safe.
//
// flushTable runs before a CDC delete. If it returned while an earlier threshold or
// interval flush for the same key was still in flight, the delete would overtake the
// upsert and resurrect the row. Returning early is exactly what the pre-lane code
// did, so this test can fail for the right reason.
func TestCDCDBBatcher_FlushTableWaitsForAnEarlierFlushOnTheSameKey(t *testing.T) {
	f := newFlushLanes(4)
	defer f.close()
	b := &cdcDBBatcher{batches: map[string]*cdcDBBatch{}, lanes: f}

	const (
		topic     = "cdc.public.orders"
		partition = 0
		table     = "orders"
	)
	key := fmt.Sprintf("%s|%d|%s", topic, partition, table)

	started := make(chan struct{})
	release := make(chan struct{})
	var flushDone int32
	// Nothing is buffered for this key: the batch this waits on is one an earlier
	// threshold flush already handed to the lane. That is the case the barrier
	// exists for, and the case a "wait only if we flushed something" version misses.
	f.submit(key, func() {
		close(started)
		<-release
		atomic.StoreInt32(&flushDone, 1)
	})
	<-started

	returned := make(chan int, 1)
	go func() { returned <- b.flushTable(context.Background(), topic, partition, table) }()
	select {
	case <-returned:
		close(release)
		t.Fatal("flushTable returned while an earlier flush for the same key was still in flight; a delete would overtake it and resurrect the row")
	case <-time.After(settle):
	}

	close(release)
	select {
	case n := <-returned:
		if n != 0 {
			t.Fatalf("flushTable reported %d buffered rows, want 0", n)
		}
	case <-time.After(arrive):
		t.Fatal("flushTable never returned after the in-flight flush completed")
	}
	if atomic.LoadInt32(&flushDone) != 1 {
		t.Fatal("flushTable returned without the earlier flush having finished")
	}
}

// TestCDCDBBatcher_FlushTableWithoutLanesDoesNotBlock pins the inline path: a
// batcher built with no lanes (every existing test fixture, and any deployment with
// RSYNC_SINK_FLUSH_LANES=0) must behave exactly as it did before.
func TestCDCDBBatcher_FlushTableWithoutLanesDoesNotBlock(t *testing.T) {
	b := &cdcDBBatcher{batches: map[string]*cdcDBBatch{}}
	done := make(chan int, 1)
	go func() { done <- b.flushTable(context.Background(), "t", 0, "orders") }()
	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("flushTable = %d, want 0", n)
		}
	case <-time.After(arrive):
		t.Fatal("flushTable blocked with no lanes configured")
	}
}
