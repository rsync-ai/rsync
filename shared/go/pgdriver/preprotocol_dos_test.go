package pgdriver

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestMaliciousServerCannotDriveUnboundedAllocation is the regression test for
// the advisory this package exists to close: GO-2026-6173 / CVE-2026-56874,
// "Denial of Service in github.com/lib/pq PostgreSQL Driver".
//
// lib/pq's conn.go:1089 read, verbatim:
//
//	if t == proto.ErrorResponse && (n < 4 || n > proto.MaxErrlen) {
//	        msg, _ := cn.buf.ReadString('\x00')
//
// A server that sends the ErrorResponse type byte with a declared length
// outside [4, 30000] and then never sends a NUL byte makes that ReadString
// grow a Go string until the process dies. Every version was affected and the
// advisory is "Fixed in: N/A", so there was nothing to upgrade to — moving to
// pgx was the only remedy available.
//
// This test is deliberately written against the *behaviour*, not the version,
// because "we no longer import lib/pq" is a weaker claim than "the read is
// bounded" — a future driver swap could quietly reintroduce it. Measured
// against lib/pq v1.12.3 this exact harness allocated 3087 MB to consume the
// 512 MB the server offered, and was still climbing when the server's own cap
// stopped it. Under pgx the connection is refused after a bounded read.
func TestMaliciousServerCannotDriveUnboundedAllocation(t *testing.T) {
	const (
		offered      = 256 << 20 // what the malicious server is willing to send
		allocCeiling = 64 << 20  // far below `offered`; lib/pq exceeded it ~48x
	)

	var served atomic.Int64
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, _ = c.Read(make([]byte, 4096)) // consume the startup packet

				// 'E' + a length field far above MaxErrlen: the exact shape that
				// sends lib/pq into the unbounded ReadString.
				hdr := make([]byte, 5)
				hdr[0] = 'E'
				binary.BigEndian.PutUint32(hdr[1:], 0xFFFFFFF0)
				if _, err := c.Write(hdr); err != nil {
					return
				}
				// Never a NUL byte, so a read-until-NUL can never terminate.
				junk := make([]byte, 1<<16)
				for i := range junk {
					junk[i] = 'A'
				}
				for served.Load() < offered {
					n, err := c.Write(junk)
					served.Add(int64(n))
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()

	dsn := fmt.Sprintf("postgres://u:p@%s/db?sslmode=disable&connect_timeout=5", ln.Addr().String())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // one attempt, so the measurement covers one handshake

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	done := make(chan error, 1)
	go func() { done <- db.Ping() }()

	select {
	case err := <-done:
		runtime.ReadMemStats(&after)
		grew := after.TotalAlloc - before.TotalAlloc

		if err == nil {
			t.Fatal("Ping succeeded against a server that speaks no PostgreSQL — " +
				"the handshake must fail, not be coerced into something usable")
		}
		if grew > allocCeiling {
			t.Errorf("the handshake allocated %d MB against a hostile server "+
				"(ceiling %d MB, server offered %d MB) — the pre-protocol error "+
				"read is no longer bounded; this is GO-2026-6173 reintroduced",
				grew>>20, allocCeiling>>20, int64(offered)>>20)
		}
		if n := len(err.Error()); n > 1<<20 {
			t.Errorf("the driver built a %d-byte error string from server-controlled "+
				"bytes; such a string is logged and can reach an LLM prompt", n)
		}
		t.Logf("refused after a bounded read: err=%d bytes, allocated %d MB, "+
			"server managed to send %d MB of the %d MB it offered",
			len(err.Error()), grew>>20, served.Load()>>20, int64(offered)>>20)

	case <-time.After(30 * time.Second):
		runtime.ReadMemStats(&after)
		t.Fatalf("Ping never returned in 30s; server sent %d MB and the client "+
			"allocated %d MB — an unbounded read",
			served.Load()>>20, (after.TotalAlloc-before.TotalAlloc)>>20)
	}
}
