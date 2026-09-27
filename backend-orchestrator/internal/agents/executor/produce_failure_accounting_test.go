package executor

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// rowsFor builds n small rows, comfortably under one 750 KB chunk so the chunker
// emits exactly one produce call per test unless the test says otherwise.
func rowsFor(n int) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, map[string]interface{}{"id": i, "email": "a@b.c"})
	}
	return out
}

// agentWithProduce returns an Agent whose Kafka produce is the given function and
// whose db is nil (every outbox helper returns early on a nil db).
func agentWithProduce(fn func(topic string, key, value []byte, headers map[string]string) error) *Agent {
	return &Agent{produceBatchStub: fn}
}

func sendChunked(t *testing.T, a *Agent, rows []map[string]interface{}) (int64, int64, int) {
	t.Helper()
	return a.sendChunkedToKafka(context.Background(), rows, "customers", "customers", "customers",
		"public", []string{"id"}, nil, "rsync.batch.customers", "trace", "pipe-1", "exec-1", 0, 0, "append", nil)
}

func TestSendChunkedToKafka_AFailedProduceIsReportedNotSwallowed(t *testing.T) {
	a := agentWithProduce(func(string, []byte, []byte, map[string]string) error {
		return errors.New("kafka: broker unreachable")
	})

	delivered, undelivered, failedChunks := sendChunked(t, a, rowsFor(40))

	if delivered != 0 {
		t.Errorf("delivered = %d, want 0 when every produce failed", delivered)
	}
	if undelivered != 40 {
		t.Errorf("undelivered = %d, want 40 — a produce failure used to be logged at Warn and nothing else, so the rows were counted as dispatched", undelivered)
	}
	if failedChunks < 1 {
		t.Errorf("failedChunks = %d, want >= 1", failedChunks)
	}
}

// The control: the same call with a produce that succeeds must report the rows as
// delivered and nothing as lost. Without it, "undelivered == 40" above could just
// mean the function counts every row as undelivered always.
func TestSendChunkedToKafka_ASuccessfulProduceLosesNothing(t *testing.T) {
	calls := 0
	a := agentWithProduce(func(string, []byte, []byte, map[string]string) error {
		calls++
		return nil
	})

	delivered, undelivered, failedChunks := sendChunked(t, a, rowsFor(40))

	if calls == 0 {
		t.Fatal("produce was never called — the test proves nothing")
	}
	if delivered != 40 {
		t.Errorf("delivered = %d, want 40", delivered)
	}
	if undelivered != 0 || failedChunks != 0 {
		t.Errorf("undelivered/failedChunks = %d/%d, want 0/0 on a clean produce", undelivered, failedChunks)
	}
}

// A run where only SOME chunks fail must report exactly the rows that were lost,
// not all of them and not none.
func TestSendChunkedToKafka_PartialFailureCountsOnlyTheLostRows(t *testing.T) {
	// One row per chunk: a payload this large exceeds the 750 KB target on its own,
	// so the chunker closes a chunk after every row.
	big := strings.Repeat("x", 800*1024)
	rows := []map[string]interface{}{
		{"id": 1, "blob": big},
		{"id": 2, "blob": big},
		{"id": 3, "blob": big},
	}
	call := 0
	a := agentWithProduce(func(string, []byte, []byte, map[string]string) error {
		call++
		if call == 2 {
			return errors.New("kafka: message too large")
		}
		return nil
	})

	delivered, undelivered, failedChunks := sendChunked(t, a, rows)

	if call != 3 {
		t.Fatalf("produce called %d times, want 3 (one chunk per oversized row) — the chunking assumption behind this test no longer holds", call)
	}
	if delivered != 2 {
		t.Errorf("delivered = %d, want 2", delivered)
	}
	if undelivered != 1 {
		t.Errorf("undelivered = %d, want 1", undelivered)
	}
	if failedChunks != 1 {
		t.Errorf("failedChunks = %d, want 1", failedChunks)
	}
}

func TestClassifyUndelivered_NothingLostIsNotAFailure(t *testing.T) {
	if v := classifyUndelivered(500, 0, 0); v.Failed {
		t.Errorf("a run that lost nothing must not fail: %+v", v)
	}
}

func TestClassifyUndelivered_EverythingLostIsATotalDrop(t *testing.T) {
	v := classifyUndelivered(500, 500, 4)
	if !v.Failed {
		t.Fatal("a run whose every row failed to produce must not report success")
	}
	if v.Status != "silent_drop_detected" {
		t.Errorf("Status = %q, want silent_drop_detected", v.Status)
	}
	if !strings.Contains(v.Reason, "500") || !strings.Contains(v.Reason, "not retried") {
		t.Errorf("Reason must name the row count and say the rows are not retried: %q", v.Reason)
	}
}

func TestClassifyUndelivered_PartialLossIsAPartialDrop(t *testing.T) {
	v := classifyUndelivered(500, 25, 1)
	if !v.Failed {
		t.Fatal("25 permanently lost rows must not report success")
	}
	if v.Status != "silent_partial_drop_detected" {
		t.Errorf("Status = %q, want silent_partial_drop_detected", v.Status)
	}
	if !strings.Contains(v.Reason, "25 of 500") {
		t.Errorf("Reason must name both counts: %q", v.Reason)
	}
	// A single failed batch must read as "1 batch", not "1 batchs" / "1 batches".
	if !strings.Contains(v.Reason, "1 batch ") {
		t.Errorf("Reason should say \"1 batch\" for a single failure: %q", v.Reason)
	}
}

// 5% is CheckForSilentDrop's tolerance for timing jitter between a dispatch count
// and an ack ledger that is still draining. A produce failure has no jitter: the
// rows are already known lost, so even one of them fails the run.
func TestClassifyUndelivered_ASingleLostRowStillFailsTheRun(t *testing.T) {
	v := classifyUndelivered(1_000_000, 1, 1)
	if !v.Failed {
		t.Error("one permanently lost row out of a million is still data loss; the 5% silent-drop tolerance is for ledger lag, not for rows we KNOW never left the process")
	}
}

// TestProduceFailureWiring pins the call sites. classifyUndelivered is pure and
// tested above, but it only protects a run if executeBatchDataTransfer actually
// feeds it — and if it is consulted BEFORE reconcileInputs, which cannot see this
// failure class at all (every produce failing leaves directKafkaMessages,
// minioFilesCreated and outboxBatches at 0, so it reports viaSink=false and the
// landing check is skipped entirely).
func TestProduceFailureWiring(t *testing.T) {
	fset := token.NewFileSet()
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	f, err := parser.ParseFile(fset, "executor.go", src, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	body := func(name string) string {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name {
				return string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
			}
		}
		t.Fatalf("%s not found in executor.go; if it moved or was renamed, update this test", name)
		return ""
	}

	batch := body("executeBatchDataTransfer")

	// Three produce sites can lose rows: the two sendChunkedToKafka calls (inline
	// payloads and the MinIO-staging fallback) and the MinIO claim-check produce.
	if n := strings.Count(batch, "undeliveredRows +="); n != 3 {
		t.Errorf("executeBatchDataTransfer accumulates undeliveredRows at %d sites, want 3 (inline chunks, MinIO-failure chunk fallback, MinIO claim-check produce)", n)
	}
	if n := strings.Count(batch, "undeliveredBatches +=") + strings.Count(batch, "undeliveredBatches++"); n != 3 {
		t.Errorf("executeBatchDataTransfer accumulates undeliveredBatches at %d sites, want 3", n)
	}

	// directKafkaMessages must never count a dispatch that failed.
	if n := strings.Count(batch, "directKafkaMessages++"); n != 2 {
		t.Fatalf("directKafkaMessages is incremented at %d sites, want 2", n)
	}
	if n := strings.Count(batch, "if chunkDelivered > 0 {\n\t\t\t\t\t\tdirectKafkaMessages++"); n == 0 {
		if !strings.Contains(batch, "chunkDelivered > 0") {
			t.Error("directKafkaMessages must be guarded on chunkDelivered > 0 — counting a message that never reached the broker is what let a total produce failure look like a sink dispatch")
		}
	}

	gate := strings.Index(batch, "classifyUndelivered(")
	reconcile := strings.Index(batch, "reconcileInputs(")
	if gate < 0 {
		t.Fatal("executeBatchDataTransfer must consult classifyUndelivered before reporting success")
	}
	if reconcile < 0 {
		t.Fatal("reconcileInputs call not found; if it moved, update this test")
	}
	if gate > reconcile {
		t.Error("the produce-failure gate must run BEFORE reconcileInputs: reconcileInputs reports viaSink=false when every produce failed, which skips the landing check altogether")
	}
}

// A lost batch must not be checkpointed past, and a chunk continuation must not
// hide it. Local e2e 2026-09-26 (run 44472b21): batch 4 logged "Failed to send
// batch to Kafka", the checkpoint still advanced to its last key, and the run's
// undelivered count died with the dispatch because needs_continuation returned
// before classifyUndelivered. A resumed run would have skipped those 1,000 rows.
func TestProduceFailureStopsTheTableBeforeItsCheckpoint(t *testing.T) {
	fset := token.NewFileSet()
	src, err := os.ReadFile("executor.go")
	if err != nil {
		t.Fatalf("read executor.go: %v", err)
	}
	f, err := parser.ParseFile(fset, "executor.go", src, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	var batch string
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "executeBatchDataTransfer" {
			batch = string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
		}
	}
	if batch == "" {
		t.Fatal("executeBatchDataTransfer not found in executor.go; if it moved or was renamed, update this test")
	}

	// Every produce site that feeds undeliveredRows also feeds the batch's own count.
	if n := strings.Count(batch, "batchUndelivered +="); n != 3 {
		t.Errorf("batchUndelivered is accumulated at %d sites, want 3 (one per undeliveredRows site)", n)
	}
	stop := strings.Index(batch, "if batchUndelivered > 0 {")
	// LastIndex: the earlier advancePage is the all-rows-filtered path, which sends nothing.
	advance := strings.LastIndex(batch, "advancePage(offset, keyOrdinal")
	save := strings.Index(batch, "\"table_complete\": sourceRowCount < exportBatchSize")
	if stop < 0 || advance < 0 || save < 0 {
		t.Fatalf("stop=%d advance=%d checkpoint=%d: a marker is missing; if the loop was restructured, update this test", stop, advance, save)
	}
	if stop > advance || stop > save {
		t.Error("a batch with undelivered rows must stop the table BEFORE the page advances and the checkpoint is saved, or the next run resumes past the lost rows")
	}

	gate := strings.Index(batch, "classifyUndelivered(")
	cont := strings.Index(batch, "Status:     \"needs_continuation\"")
	if cont < 0 {
		t.Fatal("needs_continuation return not found; if it moved, update this test")
	}
	if gate > cont {
		t.Error("the produce-failure gate must run BEFORE the needs_continuation return: the undelivered count lives for one dispatch only")
	}
}
