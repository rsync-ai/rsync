package main

import "testing"

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
