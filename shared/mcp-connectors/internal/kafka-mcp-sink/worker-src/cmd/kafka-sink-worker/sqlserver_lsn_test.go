package main

import "testing"

func TestParseSQLServerLSN(t *testing.T) {
	// Empty / unparseable -> 0.
	if got := parseSQLServerLSN(""); got != 0 {
		t.Errorf("parseSQLServerLSN(\"\") = %d, want 0", got)
	}
	if got := parseSQLServerLSN("not-hex-zzzz"); got != 0 {
		t.Errorf("parseSQLServerLSN(garbage) = %d, want 0", got)
	}

	// A real SQL Server LSN must parse to a positive int64 (never 0), otherwise
	// the sink's ordering/idempotency key is lost.
	lsn := parseSQLServerLSN("0000002a:00000948:0003")
	if lsn <= 0 {
		t.Fatalf("parseSQLServerLSN(real LSN) = %d, want > 0", lsn)
	}

	// Deterministic.
	if again := parseSQLServerLSN("0000002a:00000948:0003"); again != lsn {
		t.Errorf("parseSQLServerLSN not deterministic: %d vs %d", lsn, again)
	}

	// Monotonic: a later commit LSN (higher VLF / block) must compare greater.
	earlier := parseSQLServerLSN("0000002a:00000948:0003")
	laterBlock := parseSQLServerLSN("0000002a:00000950:0001")
	laterVLF := parseSQLServerLSN("0000002b:00000100:0001")
	if !(laterBlock > earlier) {
		t.Errorf("later log-block LSN %d not > earlier %d", laterBlock, earlier)
	}
	if !(laterVLF > laterBlock) {
		t.Errorf("later VLF LSN %d not > earlier VLF %d", laterVLF, laterBlock)
	}
}
