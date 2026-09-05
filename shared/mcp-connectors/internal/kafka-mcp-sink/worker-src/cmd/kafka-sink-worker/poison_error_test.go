package main

// Regression coverage for the P1-C keyless-delete crash-loop fix: a
// deterministically un-processable CDC event (keyless delete to an upsert dest,
// or a change event with no row payload) must be classified `poisonError`
// (DLQ + commit + advance), NOT `fatalError` (os.Exit → the supervisor respawns
// into the same message forever, delivering nothing for the whole pipeline).

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestErrorClassification_PoisonVsFatal(t *testing.T) {
	poison := poisonError{err: errors.New("keyless delete")}
	fatal := fatalError{err: errors.New("dlq down")}
	plain := errors.New("transient")

	if !isPoison(poison) {
		t.Fatal("poisonError must be isPoison")
	}
	if isFatal(poison) {
		t.Fatal("poisonError must NOT be isFatal (fatal → os.Exit crash-loop)")
	}
	if !isFatal(fatal) {
		t.Fatal("fatalError must be isFatal")
	}
	if isPoison(fatal) {
		t.Fatal("fatalError must NOT be isPoison")
	}
	for _, e := range []error{nil, plain} {
		if isPoison(e) || isFatal(e) {
			t.Fatalf("%v must be neither poison nor fatal", e)
		}
	}
	// Wrapped poison is still detected (errors.As unwraps), and stays non-fatal —
	// so it is never mistaken for a crash-worthy condition anywhere up the stack.
	wrapped := fmt.Errorf("processing failed: %w", poison)
	if !isPoison(wrapped) {
		t.Fatal("wrapped poisonError must still be isPoison via errors.As")
	}
	if isFatal(wrapped) {
		t.Fatal("wrapped poisonError must NOT be isFatal")
	}
}

func TestKeylessCDCDeleteIsPoisonNotFatal(t *testing.T) {
	// A CDC delete with no PK in the message key, to an upsert destination
	// (default, non-append mode): can never be applied → must be poison.
	hw := newHighWaterTracker()
	cfg := &WorkerConfig{} // no cdc_write_mode → upsert (not append) mode
	metrics := &Metrics{}
	sm := &SinkMessage{IsCDC: true, CDCOp: "d", Table: "orders", PK: nil, KeyFields: nil}
	msg := kafka.Message{Topic: "cdc-p.public.orders", Partition: 0, Offset: 42}
	var im, um, dm, bm sync.Map

	// pgDB=nil and httpClient=nil: the poison return fires before any DB/network
	// call, so no infrastructure is needed to exercise it.
	commit, err := processCDCEvent(
		context.Background(), hw, nil, nil, cfg, nil, nil, nil, msg, sm, metrics,
		&im, &um, &dm, &bm, time.Hour,
	)

	if commit {
		t.Fatal("a keyless delete must NOT commit as applied")
	}
	if err == nil {
		t.Fatal("a keyless delete must return an error")
	}
	if !isPoison(err) {
		t.Fatalf("keyless delete must be a poison message (DLQ + advance), got: %v", err)
	}
	if isFatal(err) {
		t.Fatalf("keyless delete must NOT be fatal (fatal → os.Exit crash-loop), got: %v", err)
	}
}
