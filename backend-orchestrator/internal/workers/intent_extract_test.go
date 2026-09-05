package workers

import "testing"

// TestExtractExplicitSourceDest_GuardsArticles verifies the fix for the
// connector-resolution failure where `from the "shopify" source ... into the
// "..." destination` captured the article "the" as a connector id. The
// flexible from/into branch must only trust matches where BOTH tokens are
// registered connector ids; article/role-noun phrasing falls through to proper
// connection resolution (ok=false) instead of resolving a bogus id.
func TestExtractExplicitSourceDest_GuardsArticles(t *testing.T) {
	cases := []struct {
		name           string
		msg            string
		wantSrc, wantD string
		wantOK         bool
	}{
		{
			name: "article after from/into is not extracted",
			msg:  `loads products from the "shopify" source connection into the "azure-pg-test-dst" postgresql destination`,
			// "the" must not be returned; named-connection resolution handles this case upstream.
			wantOK: false,
		},
		{
			name:    "real connectors still extracted (flexible branch)",
			msg:     `stream from mysql table app.users into postgresql table public.users`,
			wantSrc: "mysql", wantD: "postgresql", wantOK: true,
		},
		{
			name:    "anchored sync still works",
			msg:     `sync postgresql to snowflake`,
			wantSrc: "postgresql", wantD: "snowflake", wantOK: true,
		},
		{
			name:   "noun phrase not mistaken for connector",
			msg:    `copy shopify products to the destination`,
			wantOK: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src, dst, ok := extractExplicitSourceDest(c.msg)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v (src=%q dst=%q)", ok, c.wantOK, src, dst)
			}
			if c.wantOK && (src != c.wantSrc || dst != c.wantD) {
				t.Fatalf("got (%q,%q), want (%q,%q)", src, dst, c.wantSrc, c.wantD)
			}
		})
	}
}
