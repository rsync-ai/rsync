package main

import (
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestRemoteDatabaseViolation pins the fail-loud guard that prevents the
// "silent dev-postgres fallback": a staging/prod gateway must never come up
// wired to the in-cluster dev Postgres. See requireRemoteDatabase.
func TestRemoteDatabaseViolation(t *testing.T) {
	const localCompose = "postgres://user:password@postgres:5432/pipeline_db?sslmode=disable"
	const managed = "postgres://svc:placeholder@pg-managed.example.com:5432/staging?sslmode=require"

	cases := []struct {
		name          string
		requireRemote bool
		databaseURL   string
		wantViolation bool
	}{
		// Marker off (dev/e2e): local Postgres is legitimate, never flag.
		{"marker off ignores local", false, localCompose, false},
		{"marker off ignores empty", false, "", false},

		// Marker on (staging/prod): local/empty must fail loud.
		{"local compose service flagged", true, localCompose, true},
		{"localhost flagged", true, "postgres://u:p@localhost:5432/db", true},
		{"loopback v4 flagged", true, "postgres://u:p@127.0.0.1:5432/db", true},
		{"empty url flagged", true, "", true},

		// Marker on + genuine remote host: must pass.
		{"managed remote ok", true, managed, false},
		{"other remote host ok", true, "postgres://u:p@db.internal.example.com:5432/app", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := remoteDatabaseViolation(tc.requireRemote, tc.databaseURL)
			if (got != "") != tc.wantViolation {
				t.Fatalf("remoteDatabaseViolation(%v, %q) = %q; wantViolation=%v",
					tc.requireRemote, tc.databaseURL, got, tc.wantViolation)
			}
		})
	}
}

func TestEnvIsTrue(t *testing.T) {
	truthy := []string{"1", "true", "TRUE", "Yes", " on "}
	for _, v := range truthy {
		t.Setenv("RSYNC_TEST_FLAG", v)
		if !envIsTrue("RSYNC_TEST_FLAG") {
			t.Errorf("envIsTrue(%q) = false; want true", v)
		}
	}
	falsy := []string{"", "0", "false", "no", "off", "maybe"}
	for _, v := range falsy {
		t.Setenv("RSYNC_TEST_FLAG", v)
		if envIsTrue("RSYNC_TEST_FLAG") {
			t.Errorf("envIsTrue(%q) = true; want false", v)
		}
	}
}

// A multi-broker bootstrap list is routinely written with spaces after the
// commas. strings.Split hands the Kafka clients a space-padded address that
// never resolves, and the sarama consumer groups pass the list to the broker
// verbatim, so the trimming has to happen here to reach all of them.
func TestResolveKafkaBrokers(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"spaces after commas", "b1:9092, b2:9092 , b3:9092", []string{"b1:9092", "b2:9092", "b3:9092"}},
		{"trailing comma", "b1:9092,", []string{"b1:9092"}},
		{"single broker", "kafka:29092", []string{"kafka:29092"}},
		{"unset falls back", "", []string{"localhost:9092"}},
		{"separators only falls back", " , ,", []string{"localhost:9092"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveKafkaBrokers(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("resolveKafkaBrokers(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("resolveKafkaBrokers(%q) = %q, want %q", tc.raw, got, tc.want)
				}
			}
		})
	}
}

// TestMainExitsWhenTheDatabaseIsUnreachable runs the real main() in a child
// process against a port nothing listens on. The gateway used to log "using
// mock data" and keep serving, with /ready stuck at 503 schema_not_migrated
// for the life of the process; it must exit non-zero instead so a restart
// policy can recover it once Postgres is up.
func TestMainExitsWhenTheDatabaseIsUnreachable(t *testing.T) {
	if os.Getenv("RSYNC_GATEWAY_MAIN_HELPER") == "1" {
		main()
		return
	}

	closed := freeAddr(t) // bound and released: connecting to it is refused
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainExitsWhenTheDatabaseIsUnreachable$")
	cmd.Env = append(os.Environ(),
		"RSYNC_GATEWAY_MAIN_HELPER=1",
		"DATABASE_URL=postgres://u:p@"+closed+"/db?sslmode=disable",
		"DB_CONNECT_TIMEOUT=0", // one attempt: the retry loop has its own tests
		"ENVIRONMENT=development",
		"RSYNC_REQUIRE_REMOTE_DB=",
		"PORT="+strings.Split(freeAddr(t), ":")[1],
	)

	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	if err := func() error {
		var buf strings.Builder
		cmd.Stdout, cmd.Stderr = &buf, &buf
		if err := cmd.Start(); err != nil {
			return err
		}
		go func() { err := cmd.Wait(); done <- result{[]byte(buf.String()), err} }()
		return nil
	}(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	select {
	case r := <-done:
		exitErr, ok := r.err.(*exec.ExitError)
		if !ok || exitErr.ExitCode() == 0 {
			t.Fatalf("main() returned %v with an unreachable database; want a non-zero exit\n%s", r.err, r.out)
		}
		if !strings.Contains(string(r.out), "Database unreachable") {
			t.Fatalf("exited non-zero, but not on the database check (exit %d)\n%s", exitErr.ExitCode(), r.out)
		}
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("main() was still running 30s after its only database attempt failed -- the gateway kept serving without a database")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}
