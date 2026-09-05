package handlers

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
)

func TestNamespaceWildcard(t *testing.T) {
	cases := []struct {
		in     string
		wantNS string
		wantOK bool
	}{
		{"public.*", "public", true},
		{"blended_cost.*", "blended_cost", true},
		{"  staging.*  ", "staging", true},
		{"*", "", false},           // whole-source token, not a namespace wildcard
		{"public.users", "", false}, // concrete table
		{".*", "", false},           // empty namespace
		{"a.b.*", "a.b", true},      // two-level namespace (warehouse-style)
		{"", "", false},
	}
	for _, c := range cases {
		ns, ok := namespaceWildcard(c.in)
		if ns != c.wantNS || ok != c.wantOK {
			t.Errorf("namespaceWildcard(%q) = (%q,%v), want (%q,%v)", c.in, ns, ok, c.wantNS, c.wantOK)
		}
	}
}

func TestHasSelectionSentinel(t *testing.T) {
	cases := []struct {
		in   []string
		want bool
	}{
		{[]string{"*"}, true},
		{[]string{"public.*"}, true},
		{[]string{"public.users", "orders"}, false},
		{[]string{"public.users", "staging.*"}, true},
		{nil, false},
		{[]string{""}, false},
	}
	for _, c := range cases {
		if got := hasSelectionSentinel(c.in); got != c.want {
			t.Errorf("hasSelectionSentinel(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func fakeDiscover(tables []TableMetadata, err error) resolverDiscoverFunc {
	return func(_ context.Context, _ string, _ int) ([]TableMetadata, error) {
		return tables, err
	}
}

func multiSchemaFixture() []TableMetadata {
	return []TableMetadata{
		{Schema: "public", Name: "users"},
		{Schema: "public", Name: "orders"},
		{Schema: "blended_cost", Name: "costs"},
		{Schema: "blended_cost", Name: "routes"},
		{Schema: "staging", Name: "rate_cards"},
	}
}

func TestResolveSelectionSentinels(t *testing.T) {
	ctx := context.Background()

	t.Run("no sentinel passes through without discovery", func(t *testing.T) {
		in := []string{"public.users", "public.orders"}
		called := false
		discover := func(_ context.Context, _ string, _ int) ([]TableMetadata, error) {
			called = true
			return nil, nil
		}
		out, expanded, err := resolveSelectionSentinels(ctx, "c1", in, discover)
		if err != nil || expanded || called {
			t.Fatalf("expected passthrough, got out=%v expanded=%v called=%v err=%v", out, expanded, called, err)
		}
		if !reflect.DeepEqual(out, in) {
			t.Fatalf("passthrough mutated input: %v", out)
		}
	})

	t.Run("whole-source star expands to every schema.table", func(t *testing.T) {
		out, expanded, err := resolveSelectionSentinels(ctx, "c1", []string{"*"}, fakeDiscover(multiSchemaFixture(), nil))
		if err != nil || !expanded {
			t.Fatalf("err=%v expanded=%v", err, expanded)
		}
		want := []string{"blended_cost.costs", "blended_cost.routes", "public.orders", "public.users", "staging.rate_cards"}
		got := append([]string(nil), out...)
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("whole-source resolved = %v, want %v", got, want)
		}
	})

	t.Run("namespace wildcard filters to that schema", func(t *testing.T) {
		out, expanded, err := resolveSelectionSentinels(ctx, "c1", []string{"blended_cost.*"}, fakeDiscover(multiSchemaFixture(), nil))
		if err != nil || !expanded {
			t.Fatalf("err=%v expanded=%v", err, expanded)
		}
		want := []string{"blended_cost.costs", "blended_cost.routes"}
		got := append([]string(nil), out...)
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("namespace-wildcard resolved = %v, want %v", got, want)
		}
	})

	t.Run("mixed sentinel + explicit table dedups", func(t *testing.T) {
		out, _, err := resolveSelectionSentinels(ctx, "c1", []string{"staging.*", "public.users"}, fakeDiscover(multiSchemaFixture(), nil))
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"public.users", "staging.rate_cards"}
		got := append([]string(nil), out...)
		sort.Strings(got)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mixed resolved = %v, want %v", got, want)
		}
	})

	t.Run("discovery error propagates", func(t *testing.T) {
		_, expanded, err := resolveSelectionSentinels(ctx, "c1", []string{"*"}, fakeDiscover(nil, errors.New("boom")))
		if err == nil || !expanded {
			t.Fatalf("expected error surfaced with expanded=true, got err=%v expanded=%v", err, expanded)
		}
	})
}
