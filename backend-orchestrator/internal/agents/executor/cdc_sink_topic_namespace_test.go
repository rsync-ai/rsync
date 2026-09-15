package executor

import (
	"strings"
	"testing"
)

// TestDeriveCDCTopicPartsIsNamespaceAware locks in the MongoDB multi-collection CDC
// delivery fix.
//
// Debezium topics are "{prefix}.{db}.{table}", but the platform namespaces its own
// topics ("rsync."), so the name on the wire is "{ns}{prefix}.{db}.{table}". The old
// derivation split the FULL name on its first dot, which took "rsync" as the connector
// prefix and the connector id as the db qualifier. buildCDCSinkTopics then emitted
// "rsync.shop.customers" instead of "rsync.cdc-ec6d3a3b.shop.customers": the sink
// subscribed to topics nobody writes to, reported Stable, and delivered zero rows while
// the pipeline showed 100% healthy.
//
// This is not MongoDB-specific — every CDC source reaches this same derivation.
func TestDeriveCDCTopicPartsIsNamespaceAware(t *testing.T) {
	cases := []struct {
		name          string
		prefixEnv     *string // nil = unset (selects the "rsync." default)
		topic         string
		wantPrefix    string
		wantQualifier string
	}{
		{
			name:          "namespaced mongodb topic (the live failure)",
			prefixEnv:     nil,
			topic:         "rsync.cdc-ec6d3a3b.shop.customers",
			wantPrefix:    "rsync.cdc-ec6d3a3b",
			wantQualifier: "shop",
		},
		{
			name:          "namespaced postgres topic",
			prefixEnv:     nil,
			topic:         "rsync.cdc-4631bd14.public.orders",
			wantPrefix:    "rsync.cdc-4631bd14",
			wantQualifier: "public",
		},
		{
			// The migration lever: an empty prefix must degrade to the historical
			// behaviour byte-for-byte, so deployments that set it stay unaffected.
			name:          "empty prefix degrades to legacy behaviour",
			prefixEnv:     strptr(""),
			topic:         "cdc-4631bd14.e2e_db.big_table",
			wantPrefix:    "cdc-4631bd14",
			wantQualifier: "e2e_db",
		},
		{
			// A bare legacy topic on a namespaced deployment must NOT have a
			// namespace invented for it — the prefix has to match what is on the wire.
			name:          "bare topic on a namespaced deployment keeps its bare prefix",
			prefixEnv:     nil,
			topic:         "cdc-4631bd14.e2e_db.big_table",
			wantPrefix:    "cdc-4631bd14",
			wantQualifier: "e2e_db",
		},
		{
			// SQL Server carries an extra database segment; the qualifier is still
			// the segment immediately after the connector prefix.
			name:          "sqlserver four-segment topic",
			prefixEnv:     nil,
			topic:         "rsync.cdc-99aa.salesdb.dbo.invoices",
			wantPrefix:    "rsync.cdc-99aa",
			wantQualifier: "salesdb",
		},
		{
			name:          "operator-supplied prefix",
			prefixEnv:     strptr("acme."),
			topic:         "acme.cdc-77bb.shop.orders",
			wantPrefix:    "acme.cdc-77bb",
			wantQualifier: "shop",
		},
		{
			name:       "empty topic yields nothing",
			prefixEnv:  nil,
			topic:      "",
			wantPrefix: "",
		},
		{
			name:       "unsplittable topic yields nothing",
			prefixEnv:  nil,
			topic:      "rsync.cdc-only",
			wantPrefix: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withTopicPrefix(t, tc.prefixEnv)

			prefix, qualifier := deriveCDCTopicParts(tc.topic)
			if prefix != tc.wantPrefix {
				t.Errorf("prefix = %q, want %q (topic %q)", prefix, tc.wantPrefix, tc.topic)
			}
			if qualifier != tc.wantQualifier {
				t.Errorf("dbQualifier = %q, want %q (topic %q)", qualifier, tc.wantQualifier, tc.topic)
			}

			// The property that actually matters, and the one the old code violated:
			// the derived prefix must be a real prefix OF THE TOPIC ITSELF. "rsync"
			// passes a naive HasPrefix check, which is why this asserts the full
			// "{prefix}.{qualifier}." head instead.
			if tc.wantPrefix != "" {
				head := prefix + "." + qualifier + "."
				if !strings.HasPrefix(tc.topic, head) {
					t.Errorf("derived head %q is not a prefix of the live topic %q", head, tc.topic)
				}
			}
		})
	}
}

// TestBuildCDCSinkTopicsReproducesTheLiveTopic is the end-to-end statement of the bug:
// feed the real MongoDB values through derivation and rebuild, and the list must contain
// the topic Debezium is actually writing to.
func TestBuildCDCSinkTopicsReproducesTheLiveTopic(t *testing.T) {
	withTopicPrefix(t, nil)

	const live = "rsync.cdc-ec6d3a3b.shop.customers"
	tables := []string{"shop.customers", "shop.orders", "shop.products"}

	prefix, qualifier := deriveCDCTopicParts(live)
	topics := buildCDCSinkTopics(prefix, qualifier, "mongodb", tables, "")

	if len(topics) != len(tables) {
		t.Fatalf("got %d topics, want %d: %v", len(topics), len(tables), topics)
	}
	var found bool
	for _, tp := range topics {
		if tp == live {
			found = true
		}
		if !strings.HasPrefix(tp, "rsync.cdc-ec6d3a3b.") {
			t.Errorf("topic %q is not under the connector prefix — nobody writes to it", tp)
		}
	}
	if !found {
		t.Errorf("rebuilt topics %v do not include the live Debezium topic %q", topics, live)
	}
}

// TestEnsureProviderTopicSubscribed covers the connector-agnostic backstop: whatever a
// future derivation bug does, the sink must never end up subscribed to a set that
// excludes the topic the provider just reported.
func TestEnsureProviderTopicSubscribed(t *testing.T) {
	const live = "rsync.cdc-ec6d3a3b.shop.customers"

	t.Run("correct derivation is passed through untouched", func(t *testing.T) {
		in := []string{live, "rsync.cdc-ec6d3a3b.shop.orders"}
		got := ensureProviderTopicSubscribed(in, live, "p1")
		if len(got) != len(in) {
			t.Fatalf("list was modified: %v", got)
		}
	})

	t.Run("bogus derivation is repaired, not silently accepted", func(t *testing.T) {
		// Exactly what the old code produced.
		in := []string{"rsync.shop.customers", "rsync.shop.orders"}
		got := ensureProviderTopicSubscribed(in, live, "p1")
		if len(got) != len(in)+1 || got[0] != live {
			t.Fatalf("provider topic was not restored: %v", got)
		}
	})

	t.Run("case-insensitive so Oracle and SQL Server do not false-positive", func(t *testing.T) {
		in := []string{"RSYNC.CDC-EC6D3A3B.SHOP.CUSTOMERS"}
		got := ensureProviderTopicSubscribed(in, live, "p1")
		if len(got) != 1 {
			t.Fatalf("case difference treated as a mismatch: %v", got)
		}
	})

	t.Run("empty inputs are no-ops", func(t *testing.T) {
		if got := ensureProviderTopicSubscribed(nil, live, "p1"); got != nil {
			t.Fatalf("want nil, got %v", got)
		}
		in := []string{"a.b.c"}
		if got := ensureProviderTopicSubscribed(in, "", "p1"); len(got) != 1 {
			t.Fatalf("empty provider topic should be a no-op, got %v", got)
		}
	})
}
