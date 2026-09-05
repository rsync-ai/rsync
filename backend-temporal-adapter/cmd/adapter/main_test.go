package main

import "testing"

// TestRemoteDatabaseViolation pins the fail-loud guard that prevents the
// "silent dev-postgres fallback" on the adapter: when wired to the local dev
// Postgres in a remote-DB deployment, scheduled runs silently skip as
// "orphaned" because the (Azure) pipeline isn't in the local DB. See
// requireRemoteDatabase.
func TestRemoteDatabaseViolation(t *testing.T) {
	cases := []struct {
		name          string
		requireRemote bool
		host          string
		wantViolation bool
	}{
		// Marker off (dev/e2e): local Postgres is legitimate, never flag.
		{"marker off ignores local", false, "postgres", false},
		{"marker off ignores empty", false, "", false},

		// Marker on (staging/prod): local/empty host must fail loud.
		{"compose service host flagged", true, "postgres", true},
		{"localhost flagged", true, "localhost", true},
		{"loopback v4 flagged", true, "127.0.0.1", true},
		{"empty host flagged", true, "", true},
		{"whitespace-padded local flagged", true, "  postgres  ", true},

		// Marker on + genuine remote host: must pass.
		{"managed remote ok", true, "pg-managed.example.com", false},
		{"other remote host ok", true, "db.internal.example.com", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := remoteDatabaseViolation(tc.requireRemote, tc.host)
			if (got != "") != tc.wantViolation {
				t.Fatalf("remoteDatabaseViolation(%v, %q) = %q; wantViolation=%v",
					tc.requireRemote, tc.host, got, tc.wantViolation)
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
