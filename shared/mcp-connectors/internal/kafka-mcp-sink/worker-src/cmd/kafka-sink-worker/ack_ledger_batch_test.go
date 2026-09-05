package main

import (
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// makeAckBatch builds n parallel SinkMessage/kafka.Message pairs for the
// batched ack-ledger tests.
func makeAckBatch(n int) ([]*SinkMessage, []kafka.Message) {
	sms := make([]*SinkMessage, n)
	msgs := make([]kafka.Message, n)
	for i := 0; i < n; i++ {
		sms[i] = &SinkMessage{
			PipelineID:  "pipe",
			ExecutionID: "exec",
			Table:       "t",
			RowCount:    1,
			CDCOp:       "c",
			TxID:        "tx",
			LSN:         int64(i),
			SourceTS:    int64(i * 10),
		}
		msgs[i] = kafka.Message{Topic: "topic", Partition: i % 3, Offset: int64(i)}
	}
	return sms, msgs
}

func TestBuildCDCAckInserts_SingleChunk(t *testing.T) {
	sms, msgs := makeAckBatch(3)
	ins := buildCDCAckInserts(sms, msgs, "dest", time.Unix(0, 0).UTC())
	if len(ins) != 1 {
		t.Fatalf("want 1 insert, got %d", len(ins))
	}
	if got, want := len(ins[0].args), 3*pgAckLedgerCols; got != want {
		t.Fatalf("want %d args, got %d", want, got)
	}
	// Every bind arg must have exactly one placeholder — else the driver errors.
	if got := strings.Count(ins[0].query, "$"); got != len(ins[0].args) {
		t.Fatalf("placeholders %d != args %d", got, len(ins[0].args))
	}
	if !strings.Contains(ins[0].query, "$48)") {
		t.Fatalf("expected sequential placeholder $48, query=%s", ins[0].query)
	}
	if !strings.Contains(ins[0].query, "ON CONFLICT") {
		t.Fatalf("missing ON CONFLICT idempotency clause")
	}
}

func TestBuildCDCAckInserts_Chunking(t *testing.T) {
	n := 2*pgAckLedgerChunk + 1 // spans three chunks: 4000, 4000, 1
	sms, msgs := makeAckBatch(n)
	ins := buildCDCAckInserts(sms, msgs, "dest", time.Unix(0, 0).UTC())
	if len(ins) != 3 {
		t.Fatalf("want 3 chunks for %d rows, got %d", n, len(ins))
	}
	total := 0
	for i, c := range ins {
		rows := len(c.args) / pgAckLedgerCols
		total += rows
		if len(c.args) > 65535 {
			t.Fatalf("chunk %d has %d bind args, exceeds PostgreSQL 65535 limit", i, len(c.args))
		}
		if rows > pgAckLedgerChunk {
			t.Fatalf("chunk %d has %d rows, exceeds chunk cap %d", i, rows, pgAckLedgerChunk)
		}
		if strings.Count(c.query, "$") != len(c.args) {
			t.Fatalf("chunk %d: placeholders != args", i)
		}
	}
	if total != n {
		t.Fatalf("chunks cover %d rows, want %d", total, n)
	}
}

func TestBuildCDCAckInserts_Empty(t *testing.T) {
	if got := buildCDCAckInserts(nil, nil, "d", time.Unix(0, 0).UTC()); got != nil {
		t.Fatalf("empty input want nil, got %v", got)
	}
	// Defensive: mismatched message slice must not panic and must skip.
	sms, _ := makeAckBatch(3)
	if got := buildCDCAckInserts(sms, []kafka.Message{{}}, "d", time.Unix(0, 0).UTC()); got != nil {
		t.Fatalf("mismatched lengths want nil, got %v", got)
	}
}
