package handlers

import (
	"reflect"
	"testing"
)

// TestParseTableIntent locks KI-NLCHAT-TABLENAME-IGNORED: an explicit single
// table name in the NL request must be extracted (so the create can pre-select it
// and skip the 33-table HITL), while multi-table / no-table phrasings return nil
// so those requests still fall through to the table-selection HITL.
func TestParseTableIntent(t *testing.T) {
	cases := []struct {
		name string
		msg  string
		want []string
	}{
		// Positive — singular "<name> table"
		{"copy the X table", "copy the customers table from test-mysql into pg-dest", []string{"customers"}},
		{"sync the X table", "sync the orders table from mysql to postgres", []string{"orders"}},
		{"bare X table", "move customers table to pg-dest", []string{"customers"}},
		{"underscore name", "copy the order_items table", []string{"order_items"}},
		{"schema-qualified", "sync the public.customers table", []string{"public.customers"}},
		// Positive — "table <name>"
		{"table then name", "sync from mysql, table customers, to postgres", []string{"customers"}},
		// Negative — no table named → defer to HITL
		{"no table keyword", "sync data from mysql to postgres", nil},
		{"everything", "copy everything from test-mysql to pg-dest", nil},
		{"bare the table", "sync the table from mysql to postgres", nil},
		// Negative — plural / multi-table → defer to HITL
		{"all tables", "sync all tables from mysql to postgres", nil},
		{"plural tables", "copy the users and orders tables to pg-dest", nil},
		{"every table", "replicate every table from mysql", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseTableIntent(c.msg)
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("parseTableIntent(%q) = %v, want %v", c.msg, got, c.want)
			}
		})
	}
}

// TestShouldPreselectNamedTables locks the safety gate: pre-select ONLY on a clean
// cache resolution; any missing / ambiguous / cold-cache result defers to HITL.
func TestShouldPreselectNamedTables(t *testing.T) {
	cases := []struct {
		name      string
		qualified []string
		missing   []string
		ambiguous map[string][]string
		ok        bool
		want      bool
	}{
		{"clean resolve", []string{"public.customers"}, nil, nil, true, true},
		{"cache cold (ok=false)", nil, nil, nil, false, false},
		{"missing name", nil, []string{"custmers"}, nil, true, false},
		{"ambiguous name", nil, nil, map[string][]string{"customers": {"a.customers", "b.customers"}}, true, false},
		{"empty qualified", nil, nil, nil, true, false},
		{"partial: one qualified but one missing", []string{"public.orders"}, []string{"custmers"}, nil, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := shouldPreselectNamedTables(c.qualified, c.missing, c.ambiguous, c.ok); got != c.want {
				t.Fatalf("shouldPreselectNamedTables(%v,%v,%v,%v) = %v, want %v",
					c.qualified, c.missing, c.ambiguous, c.ok, got, c.want)
			}
		})
	}
}
