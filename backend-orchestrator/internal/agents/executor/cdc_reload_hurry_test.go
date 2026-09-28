package executor

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
)

// The start check's RUNNING streak is what a Reload hands the snapshot
// dispatcher in place of a second ReadyStable wait, so it must be a streak the
// check actually watched, and zero on every path that is not one.
func TestCheckCDCConnectorStarted_ProofIsTheWatchedStreak(t *testing.T) {
	shrinkCDCStartCheck(t, 2*time.Second, 30*time.Millisecond, 5*time.Millisecond)
	c := &connectStub{bodies: []string{statusBody("RUNNING", [2]string{"RUNNING", ""})}}
	before := time.Now()
	reason, proof := checkCDCConnectorStarted(context.Background(), startStub(t, c), false)
	if reason != "" {
		t.Fatalf("stable RUNNING must pass, got %q", reason)
	}
	if proof.Since.Before(before) || proof.At.Sub(proof.Since) < 30*time.Millisecond || proof.At.After(time.Now()) {
		t.Fatalf("proof %+v is not the watched streak of at least the settle time", proof)
	}

	shrinkCDCStartCheck(t, 60*time.Millisecond, 20*time.Millisecond, 5*time.Millisecond)
	if _, proof := checkCDCConnectorStarted(context.Background(), startStub(t, &connectStub{statusCode: http.StatusNotFound}), false); proof != (cdcsnapshot.RunningProof{}) {
		t.Fatalf("a check that timed out without a verdict must carry no proof, got %+v", proof)
	}
	failed := &connectStub{bodies: []string{statusBody("RUNNING", [2]string{"FAILED", "boom"})}}
	if reason, proof := checkCDCConnectorStarted(context.Background(), startStub(t, failed), false); reason == "" || proof != (cdcsnapshot.RunningProof{}) {
		t.Fatalf("a FAILED task must fail with no proof, got %q %+v", reason, proof)
	}
}

type recordingHurrier struct {
	id    string
	proof cdcsnapshot.RunningProof
	calls int
}

func (h *recordingHurrier) Hurry(id string, p cdcsnapshot.RunningProof) {
	h.id, h.proof, h.calls = id, p, h.calls+1
}

func TestHurryCDCReload_HandsTheDispatcherTheRequestAndProof(t *testing.T) {
	h := &recordingHurrier{}
	a := &Agent{}
	a.SetSnapshotHurrier(h)
	proof := cdcsnapshot.RunningProof{Since: time.Unix(100, 0), At: time.Unix(111, 0)}
	a.hurryCDCReload(cdcsnapshot.Request{ID: "req-1"}, proof)
	if h.calls != 1 || h.id != "req-1" || h.proof != proof {
		t.Fatalf("hurrier got %d calls, id %q, proof %+v", h.calls, h.id, h.proof)
	}
	a.hurryCDCReload(cdcsnapshot.Request{}, proof)
	if h.calls != 1 {
		t.Fatal("a request without an id must not be hurried")
	}
	(&Agent{}).hurryCDCReload(cdcsnapshot.Request{ID: "x"}, proof) // no dispatcher wired: no panic
}

// The real dispatcher satisfies the executor's interface (main.go wires it).
var _ snapshotHurrier = (*cdcsnapshot.Dispatcher)(nil)
