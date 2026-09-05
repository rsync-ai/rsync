package handlers

import "testing"

// No build tag on purpose. CI runs `go test ./...` with no -tags, so a guard
// behind one would never run there — which is the same as having no guard.

// The single most important property: unset means the demo does not exist.
// Cloud leaves RSYNC_DEMO_DESTINATION_DSN unset, so if this ever returned a
// usable destination by default, app.rsync.ai would start offering a bundled
// warehouse it does not have.
func TestDemoDestinationIsAbsentUnlessConfigured(t *testing.T) {
	t.Setenv("RSYNC_DEMO_DESTINATION_DSN", "")

	dest, err := demoDestinationConfig()
	if err != nil {
		t.Fatalf("unset DSN should not be an error, got %v", err)
	}
	if dest != nil {
		t.Fatalf("unset DSN must yield no destination, got %+v", dest)
	}
}

func TestDemoDestinationIgnoresWhitespaceOnlyValue(t *testing.T) {
	t.Setenv("RSYNC_DEMO_DESTINATION_DSN", "   ")

	dest, err := demoDestinationConfig()
	if err != nil {
		t.Fatalf("whitespace DSN should not be an error, got %v", err)
	}
	if dest != nil {
		t.Fatalf("whitespace DSN must yield no destination, got %+v", dest)
	}
}

func TestDemoDestinationParsesQuickstartDSN(t *testing.T) {
	// Fake credential: this is the shape docker-compose.quickstart.yml renders.
	t.Setenv("RSYNC_DEMO_DESTINATION_DSN", "postgres://rsync:FAKEPLACEHOLDER@demo-warehouse:5432/demo_warehouse")

	dest, err := demoDestinationConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dest == nil {
		t.Fatal("expected a destination")
	}

	// These five keys are exactly the postgresql connector's required_config.
	// Getting one wrong produces a connection that fails only at pipeline time.
	for _, tc := range []struct{ field, got, want string }{
		{"host", dest.Host, "demo-warehouse"},
		{"port", dest.Port, "5432"},
		{"database", dest.Database, "demo_warehouse"},
		{"user", dest.User, "rsync"},
		{"password", dest.Password, "FAKEPLACEHOLDER"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.field, tc.got, tc.want)
		}
	}
}

func TestDemoDestinationDefaultsPort(t *testing.T) {
	t.Setenv("RSYNC_DEMO_DESTINATION_DSN", "postgres://rsync:FAKEPLACEHOLDER@demo-warehouse/demo_warehouse")

	dest, err := demoDestinationConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dest.Port != "5432" {
		t.Errorf("port = %q, want the postgres default 5432", dest.Port)
	}
}

// A typo must be loud. If a malformed value fell back to "unavailable", a
// mistyped compose file would present as the feature quietly not existing.
func TestDemoDestinationRejectsMalformedValues(t *testing.T) {
	cases := map[string]string{
		"wrong scheme":  "mysql://rsync:FAKEPLACEHOLDER@demo-warehouse:5432/demo_warehouse",
		"no database":   "postgres://rsync:FAKEPLACEHOLDER@demo-warehouse:5432",
		"no user":       "postgres://demo-warehouse:5432/demo_warehouse",
		"no host":       "postgres:///demo_warehouse",
		"not a url":     "://",
		"bare hostname": "demo-warehouse",
	}

	for name, dsn := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("RSYNC_DEMO_DESTINATION_DSN", dsn)

			dest, err := demoDestinationConfig()
			if err == nil {
				t.Fatalf("expected an error for %q, got destination %+v", dsn, dest)
			}
			if dest != nil {
				t.Errorf("a rejected DSN must yield no destination, got %+v", dest)
			}
		})
	}
}
