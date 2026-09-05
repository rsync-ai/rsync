package executor

import "testing"

// TestReconcileInputs covers the decision the executor makes BEFORE the
// classification arithmetic runs: which denominator the landing reconcile is
// measured against, and whether it runs at all. That decision is where
// KI-SILENTDROP-ACK-SUM-SPANS-CHUNKS lived — the classification was always
// correct about the numbers it was handed, it was handed the wrong ones.
//
// The two rows that matter most are the last two: they are the same lossy run
// under the fixed and the pre-fix denominator, and they must disagree.
func TestReconcileInputs(t *testing.T) {
	cases := []struct {
		name                  string
		thisDispatchRows      int64
		kafkaMsgs, minioFiles int
		outboxRows            int64
		outboxBatches         int
		wantDispatched        int64
		wantViaSink           bool
	}{
		{
			// Single-dispatch run: the outbox covers exactly the one dispatch, so
			// the fix is a no-op and today's healthy runs are untouched.
			name:             "unchunked run — outbox equals this dispatch",
			thisDispatchRows: 30000, kafkaMsgs: 5,
			outboxRows: 30000, outboxBatches: 3,
			wantDispatched: 30000, wantViaSink: true,
		},
		{
			// THE FIX. Five chunks under one execution id dispatched 150k; this
			// (final) dispatch only counted its own 50k. The ack ledger already
			// sums 150k's worth of window, so only the outbox total can be weighed
			// against it.
			name:             "chunked run — whole-execution outbox total wins over this chunk",
			thisDispatchRows: 50000, kafkaMsgs: 5,
			outboxRows: 150000, outboxBatches: 12,
			wantDispatched: 150000, wantViaSink: true,
		},
		{
			// THE SECOND HOLE. A table whose row count lands exactly on a chunk
			// boundary resumes, reads 0 rows and exports nothing: every per-dispatch
			// counter is 0. Pre-fix, dispatchedViaSink was false and the ENTIRE run
			// skipped landing verification while reporting success. The outbox is the
			// only surviving evidence that this execution used the sink at all.
			name:             "chunked run whose final dispatch exported nothing still verifies",
			thisDispatchRows: 0, kafkaMsgs: 0, minioFiles: 0,
			outboxRows: 150000, outboxBatches: 12,
			wantDispatched: 150000, wantViaSink: true,
		},
		{
			// Degradation guard: outbox unavailable (table empty, query failed,
			// pre-043 schema) → sumDispatchedRows returns (0,0) and the behaviour is
			// EXACTLY the pre-fix behaviour, never weaker.
			name:             "outbox unavailable — falls back to this dispatch's own count",
			thisDispatchRows: 30000, kafkaMsgs: 5,
			outboxRows: 0, outboxBatches: 0,
			wantDispatched: 30000, wantViaSink: true,
		},
		{
			// Same guard, partial: an outbox lagging behind the producer must not
			// SHRINK the denominator, or the fix could mask a drop today's code
			// catches. The floor is what makes this monotone.
			name:             "outbox lagging behind the producer — floored at this dispatch",
			thisDispatchRows: 50000, kafkaMsgs: 5,
			outboxRows: 40000, outboxBatches: 2,
			wantDispatched: 50000, wantViaSink: true,
		},
		{
			// A transfer that never touched a sink lane must still not be reconciled
			// against an ack ledger it does not write to — otherwise every direct-write
			// destination would report a total drop.
			name:             "no sink lane touched — reconcile stays off",
			thisDispatchRows: 500, kafkaMsgs: 0, minioFiles: 0,
			outboxRows: 0, outboxBatches: 0,
			wantDispatched: 500, wantViaSink: false,
		},
		{
			// The MinIO claim-check lane is a sink lane too (KI-NLCHAT-TYPECONVERT-
			// FALSE-SUCCESS): it must arm the reconcile on its own, with no outbox.
			name:             "MinIO claim-check lane alone arms the reconcile",
			thisDispatchRows: 100, kafkaMsgs: 0, minioFiles: 1,
			outboxRows: 0, outboxBatches: 0,
			wantDispatched: 100, wantViaSink: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dispatched, viaSink := reconcileInputs(c.thisDispatchRows, c.kafkaMsgs, c.minioFiles, c.outboxRows, c.outboxBatches)
			if dispatched != c.wantDispatched {
				t.Errorf("dispatched = %d, want %d", dispatched, c.wantDispatched)
			}
			if viaSink != c.wantViaSink {
				t.Errorf("viaSink = %v, want %v", viaSink, c.wantViaSink)
			}
			// Monotone-safety invariant, asserted on every row rather than just the
			// two that target it: the denominator can never be smaller than what the
			// pre-fix code used, so this change cannot weaken the check for ANY input.
			if dispatched < c.thisDispatchRows {
				t.Errorf("dispatched %d is BELOW this dispatch's own count %d — the fix would be weaker than the code it replaces", dispatched, c.thisDispatchRows)
			}
		})
	}
}
