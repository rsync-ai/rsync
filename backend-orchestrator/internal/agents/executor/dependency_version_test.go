package executor

import (
	"errors"
	"testing"
)

type fakeResolver struct {
	got []string
	ver string
	err error
}

func (f *fakeResolver) ResolveConcreteVersion(name, version string) (string, error) {
	f.got = []string{name, version}
	return f.ver, f.err
}

func TestConcreteVersionOrRequested(t *testing.T) {
	t.Run("latest resolves to the concrete version, as the destination does", func(t *testing.T) {
		r := &fakeResolver{ver: "v1.0.0"}
		if got := concreteVersionOrRequested(r, "mongodb", "latest"); got != "v1.0.0" {
			t.Fatalf("got %q, want v1.0.0", got)
		}
		if r.got[0] != "mongodb" || r.got[1] != "latest" {
			t.Fatalf("resolver called with %v", r.got)
		}
	})
	t.Run("an empty version is asked for as latest, never recorded as mongodb@", func(t *testing.T) {
		r := &fakeResolver{ver: "v2.1.0"}
		if got := concreteVersionOrRequested(r, "mongodb", "  "); got != "v2.1.0" {
			t.Fatalf("got %q, want v2.1.0", got)
		}
		if r.got[1] != "latest" {
			t.Fatalf("resolver asked for %q, want latest", r.got[1])
		}
	})
	t.Run("a failed resolve keeps the requested version", func(t *testing.T) {
		r := &fakeResolver{err: errors.New("latest.json missing")}
		if got := concreteVersionOrRequested(r, "mongodb", "latest"); got != "latest" {
			t.Fatalf("got %q, want latest", got)
		}
	})
	t.Run("no resolver keeps the requested version", func(t *testing.T) {
		if got := concreteVersionOrRequested(nil, "mongodb", ""); got != "latest" {
			t.Fatalf("got %q, want latest", got)
		}
	})
}
