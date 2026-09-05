package registry

import (
	"testing"
	"time"
)

// newTestRegistry builds a registry with a hand-rolled cache so we don't
// have to round-trip through DB or filesystem during unit tests. The
// expiry is set far in the future to avoid the async refresh kicking
// in during a test.
func newTestRegistry(entries map[string]string) *ConnectorRegistry {
	r := &ConnectorRegistry{
		cache:       make(map[string]*ConnectorCapabilities),
		cacheTTL:    time.Hour,
		cacheExpiry: time.Now().Add(time.Hour),
	}
	for key, displayName := range entries {
		r.cache[key] = &ConnectorCapabilities{
			ConnectorType: key,
			DisplayName:   displayName,
		}
	}
	return r
}

// TestGetCapabilities_ExactMatch is the baseline — the registry must
// still find a connector by its exact canonical key.
func TestGetCapabilities_ExactMatch(t *testing.T) {
	r := newTestRegistry(map[string]string{
		"shopify-admin-graphql": "Shopify",
		"postgresql":            "PostgreSQL",
	})

	caps := r.GetCapabilities("shopify-admin-graphql")
	if caps == nil {
		t.Fatal("expected exact match for shopify-admin-graphql to return caps")
	}
	if caps.ConnectorType != "shopify-admin-graphql" {
		t.Errorf("connector_type = %q, want shopify-admin-graphql", caps.ConnectorType)
	}
}

// TestGetCapabilities_VendorPrefixAlias is the regression guard for the
// chat journey bug: typing "Sync Shopify to PostgreSQL" mapped to the
// token "shopify", but the catalog only has "shopify-admin-graphql".
// Without alias resolution the resolver dies with `No connection found
// for 'shopify'` and the chat-driven pipeline can't even reach the
// connection-picker modal.
func TestGetCapabilities_VendorPrefixAlias(t *testing.T) {
	r := newTestRegistry(map[string]string{
		"shopify-admin-graphql": "Shopify",
		"postgresql":            "PostgreSQL",
	})

	caps := r.GetCapabilities("shopify")
	if caps == nil {
		t.Fatal("expected 'shopify' to alias-resolve to shopify-admin-graphql")
	}
	if caps.ConnectorType != "shopify-admin-graphql" {
		t.Errorf("alias resolved to %q, want shopify-admin-graphql", caps.ConnectorType)
	}
}

// TestGetCapabilities_AliasNormalisesUnderscores covers the case where
// the chat NL produced "shopify_admin_graphql" (a connector_type with
// underscores) but the canonical key uses dashes. The normaliser
// converts before comparison.
func TestGetCapabilities_AliasNormalisesUnderscores(t *testing.T) {
	r := newTestRegistry(map[string]string{
		"shopify-admin-graphql": "Shopify",
	})

	caps := r.GetCapabilities("shopify_admin_graphql")
	if caps == nil || caps.ConnectorType != "shopify-admin-graphql" {
		t.Errorf("expected underscore-form alias to resolve; got %+v", caps)
	}
}

// TestGetCapabilities_NoFalsePositiveOnShortQuery prevents the alias
// fallback from being too greedy. A 1- or 3-character query must NOT
// substring-match "postgresql" or "shopify-admin-graphql" — that would
// mask real "unknown connector" errors and route pipelines to the
// wrong type.
func TestGetCapabilities_NoFalsePositiveOnShortQuery(t *testing.T) {
	r := newTestRegistry(map[string]string{
		"postgresql":            "PostgreSQL",
		"shopify-admin-graphql": "Shopify",
	})

	for _, q := range []string{"p", "po", "pos", "s"} {
		if caps := r.GetCapabilities(q); caps != nil {
			t.Errorf("short query %q should not alias-match, got %s", q, caps.ConnectorType)
		}
	}
}

// TestGetCapabilities_PrefixNotSubstring keeps the matcher strict: only
// vendor-prefix (with a dash boundary) qualifies, not any substring.
// `admin` substring-matches `shopify-admin-graphql` but is NOT a vendor
// prefix — it must miss.
func TestGetCapabilities_PrefixNotSubstring(t *testing.T) {
	r := newTestRegistry(map[string]string{
		"shopify-admin-graphql": "Shopify",
	})

	if caps := r.GetCapabilities("admin"); caps != nil {
		t.Errorf("substring 'admin' should not alias-match shopify-admin-graphql, got %s", caps.ConnectorType)
	}
}

// TestGetCapabilities_UnknownReturnsNil — the alias fallback must
// gracefully report "no such connector" when neither the exact key nor
// a vendor-prefix variant exists in the cache. The downstream resolver
// then falls through to its name/alias/substring lookups and ultimately
// surfaces a clean HITL "missing connector" response.
func TestGetCapabilities_UnknownReturnsNil(t *testing.T) {
	r := newTestRegistry(map[string]string{
		"postgresql": "PostgreSQL",
	})

	if caps := r.GetCapabilities("snowflake"); caps != nil {
		t.Errorf("unknown connector should return nil, got %s", caps.ConnectorType)
	}
	if caps := r.GetCapabilities(""); caps != nil {
		t.Error("empty query should return nil")
	}
}
