package executor

import (
	"strings"
	"testing"
)

// TestClassifyLandedReconcile locks the total-DLQ fix: a kafka-dispatched run
// with zero landed rows AND zero acks within the reconcile deadline must fail
// CLOSED (silent_drop_detected with a real reason) instead of the empty-error
// unverified_completion that read as a stuck/ambiguous "Data transfer failed:".
// It also guards that the proven happy path, partial shortfall, and the
// object/blob fail-soft lane are unchanged.
func TestClassifyLandedReconcile(t *testing.T) {
	const exec = "exec-1"
	cases := []struct {
		name                           string
		totalRows, landed, received    int64
		ackRows, kafkaMsgs, minioFiles int
		outboxBatches                  int
		sinkErr                        string
		wantDrop                       bool
		wantUnverified                 bool
		wantLanded                     int64
		wantReasonContains             string
		wantStatus                     string
	}{
		{
			name: "happy full load (30k/30k) stays success", totalRows: 30000, landed: 30000, received: 30000,
			ackRows: 3, kafkaMsgs: 5, wantDrop: false, wantUnverified: false, wantLanded: 30000,
		},
		{
			// Benign upsert/dedup merge: the sink RECEIVED all 30k rows but the
			// destination merged them to 15k writes. received==dispatched, NO sink
			// error → must stay success and NOT feed the smaller count downstream
			// (wantLanded stays totalRows), so upsert pipelines are never false-failed.
			name: "benign upsert undercount (received==dispatched) stays success", totalRows: 30000, landed: 15000, received: 30000,
			ackRows: 2, kafkaMsgs: 5, wantDrop: false, wantUnverified: false, wantLanded: 30000,
		},
		{
			// NEW (C5): a RECEIPT shortfall with no sink error — the sink only pulled
			// 15k of 30k dispatched (rows never reached the destination lane, e.g. an
			// upstream transform DLQ, or the sink hasn't drained). Landing is
			// unconfirmed → fail-SOFT (unverified_completion), not a silent success.
			// wantLanded stays totalRows so the downstream 5% probe doesn't also fire.
			name: "receipt shortfall with NO sink error is unverified, not success", totalRows: 30000, landed: 15000, received: 15000,
			ackRows: 2, kafkaMsgs: 5, wantDrop: false, wantUnverified: true, wantLanded: 30000,
			wantReasonContains: "landing unconfirmed",
		},
		{
			// Version-skew guard: a sink build that doesn't populate rows_read
			// (received==0) must NOT be flagged — fall back to the old warn-only
			// benign behavior rather than false-failing every run.
			name: "undercount with received==0 (ledger skew) stays success", totalRows: 30000, landed: 15000, received: 0,
			ackRows: 2, kafkaMsgs: 5, wantDrop: false, wantUnverified: false, wantLanded: 30000,
		},
		{
			// THE FAST-FOLLOW FIX: a partial shortfall WITH a negative-ack sink error
			// is permanent batch loss (some batches DLQ'd) and must fail closed as a
			// partial drop — previously reported `completed`/`success`. sinkErr wins
			// over the receipt-shortfall tier.
			name: "partial shortfall WITH sink error fails closed as partial drop", totalRows: 30000, landed: 15000, received: 15000,
			ackRows: 4, kafkaMsgs: 5, sinkErr: "postgresql: type mismatch on batch 7",
			wantDrop: true, wantUnverified: false, wantLanded: 15000,
			wantStatus:         "silent_partial_drop_detected",
			wantReasonContains: "partially dropped rows",
		},
		{
			name: "ack-evidenced total drop surfaces sink error", totalRows: 30000, landed: 0, received: 30000,
			ackRows: 3, kafkaMsgs: 5, sinkErr: "databricks: table not found",
			wantDrop: true, wantUnverified: false, wantLanded: 0,
			wantStatus:         "silent_drop_detected",
			wantReasonContains: "sink error: databricks: table not found",
		},
		{
			// THE FIX: zero acks on a kafka run no longer emits unverified_completion.
			name: "zero-ack kafka run fails closed as total drop", totalRows: 30000, landed: 0, received: 0,
			ackRows: 0, kafkaMsgs: 5, wantDrop: true, wantUnverified: false, wantLanded: 0,
			wantReasonContains: "no acks were recorded within the reconcile deadline",
		},
		{
			name: "zero-ack kafka run surfaces the sink error when present", totalRows: 30000, landed: 0, received: 0,
			ackRows: 0, kafkaMsgs: 5, sinkErr: "connect timeout",
			wantDrop: true, wantUnverified: false, wantLanded: 0,
			wantReasonContains: "sink error: connect timeout",
		},
		{
			// KI-NLCHAT-TYPECONVERT-FALSE-SUCCESS regression: a batch dispatched via
			// the MinIO claim-check sink lane (minioFiles>0, kafkaMsgs==0) whose
			// consumer transform DLQ'd — 0 landed, 0 acks — must ALSO fail closed.
			// Before the fix this hit the fail-soft branch and reported `completed`.
			name: "zero-ack MinIO claim-check lane fails closed", totalRows: 100, landed: 0, received: 0,
			ackRows: 0, kafkaMsgs: 0, minioFiles: 1, wantDrop: true, wantUnverified: false, wantLanded: 0,
			wantReasonContains: "no acks were recorded within the reconcile deadline",
		},
		{
			// Neither sink lane touched (no kafka messages AND no MinIO files AND no
			// outbox rows) keeps the fail-soft path — in practice
			// classifyLandedReconcile is only reached when dispatchedViaSink, so this
			// is the defensive all-zero guard.
			name: "zero-ack no-sink-lane stays fail-soft", totalRows: 30000, landed: 0, received: 0,
			ackRows: 0, kafkaMsgs: 0, minioFiles: 0, outboxBatches: 0, wantDrop: false, wantUnverified: true, wantLanded: 30000,
		},
		{
			// KI-SILENTDROP-ACK-SUM-SPANS-CHUNKS, half 2. The final dispatch of a
			// chunked run whose table ended exactly on a chunk boundary resumes, reads
			// 0 rows and exports nothing — so BOTH per-dispatch sink counters are 0
			// even though earlier chunks pushed 30k rows through the sink. The outbox
			// row count is the only surviving evidence that this execution dispatched
			// anything; without it the run reported success having verified nothing.
			name:      "chunked run whose final dispatch exported nothing still fails closed on zero acks",
			totalRows: 30000, landed: 0, received: 0,
			ackRows: 0, kafkaMsgs: 0, minioFiles: 0, outboxBatches: 12,
			wantDrop: true, wantUnverified: false, wantLanded: 0,
			wantReasonContains: "no acks were recorded within the reconcile deadline",
		},
		{
			// KI-SILENTDROP-ACK-SUM-SPANS-CHUNKS, half 1 — the shape the fix exists
			// for. Five chunks dispatched 150k rows under ONE execution id; chunks 1-4
			// landed 100k, chunk 5's 50k were never received. Fed the whole-execution
			// denominator (150k) this is a receipt shortfall → fail-soft. Fed the final
			// chunk's own count (50k) instead — the pre-fix behaviour, covered by the
			// paired case below — `landed` (100k) exceeds it and the drop is invisible.
			name:      "chunked run: whole-execution denominator exposes the final chunk's loss",
			totalRows: 150000, landed: 100000, received: 100000,
			ackRows: 10, kafkaMsgs: 5, minioFiles: 0, outboxBatches: 12,
			wantDrop: false, wantUnverified: true, wantLanded: 150000,
			wantReasonContains: "landing unconfirmed for 50000 of 150000 dispatched rows",
		},
		{
			// The paired PRE-FIX denominator, pinned so the regression is visible as a
			// behavioural difference rather than an argument: same ledger sums, but
			// `dispatched` carries only the final chunk's 50k. landed=100k >= 50k, so
			// no branch matches and the run resolves as a clean success — 50k rows
			// silently lost. This case documents the bug; it must NOT be "fixed".
			name:      "PRE-FIX denominator (final chunk only) reports the same lossy run as success",
			totalRows: 50000, landed: 100000, received: 100000,
			ackRows: 10, kafkaMsgs: 5, minioFiles: 0, outboxBatches: 12,
			wantDrop: false, wantUnverified: false, wantLanded: 50000,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := classifyLandedReconcile(c.totalRows, c.landed, c.received, c.ackRows, c.kafkaMsgs, c.minioFiles, c.outboxBatches, c.sinkErr, exec)
			if d.AckEvidencedDrop != c.wantDrop {
				t.Errorf("AckEvidencedDrop = %v, want %v", d.AckEvidencedDrop, c.wantDrop)
			}
			if d.UnverifiedCompletion != c.wantUnverified {
				t.Errorf("UnverifiedCompletion = %v, want %v", d.UnverifiedCompletion, c.wantUnverified)
			}
			if d.LandedRows != c.wantLanded {
				t.Errorf("LandedRows = %d, want %d", d.LandedRows, c.wantLanded)
			}
			if c.wantReasonContains != "" && !strings.Contains(d.Reason, c.wantReasonContains) {
				t.Errorf("Reason = %q, want it to contain %q", d.Reason, c.wantReasonContains)
			}
			if c.wantStatus != "" && d.Status != c.wantStatus {
				t.Errorf("Status = %q, want %q", d.Status, c.wantStatus)
			}
			// A drop must always carry a non-empty terminal reason (the whole point
			// of the fix — no more empty-error terminal states).
			if d.AckEvidencedDrop && strings.TrimSpace(d.Reason) == "" {
				t.Errorf("AckEvidencedDrop with empty Reason — defeats the fix")
			}
			// A drop and a fail-soft are mutually exclusive.
			if d.AckEvidencedDrop && d.UnverifiedCompletion {
				t.Errorf("both AckEvidencedDrop and UnverifiedCompletion set")
			}
		})
	}
}
