package handlers

import (
	"reflect"
	"testing"
)

// TestReGenerateCommandNoOvermatch verifies fix #3: the generate-command regex no
// longer swallows a multi-word pipeline request into a bogus connector id (the
// capture class previously allowed spaces).
func TestReGenerateCommandNoOvermatch(t *testing.T) {
	matches := []struct {
		msg  string
		name string
	}{
		{"generate acme-crm", "acme-crm"},
		{"generate connector for shopify", "shopify"},
		{"build acme_crm connector", "acme_crm"},
		{"create hubspot", "hubspot"},
	}
	for _, tc := range matches {
		m := reGenerateCommand.FindStringSubmatch(tc.msg)
		if len(m) != 2 || m[1] != tc.name {
			t.Errorf("reGenerateCommand(%q) = %#v, want capture %q", tc.msg, m, tc.name)
		}
	}
	nonMatches := []string{
		"generate acme and sync mysql to postgres", // must fall through to pair parse (#3)
		"generate my new crm system now please",
		"generate acme crm", // multi-word name no longer accepted (kebab required)
		"sync mysql to postgres",
	}
	for _, msg := range nonMatches {
		if m := reGenerateCommand.FindStringSubmatch(msg); m != nil {
			t.Errorf("reGenerateCommand(%q) unexpectedly matched: capture=%q", msg, m[1])
		}
	}
}

// TestMergeUniqueColumns verifies the union helper used to combine masking columns
// from the original request and the confirmation message (fix #1: a late
// "yes but mask email" must not drop the mask).
func TestMergeUniqueColumns(t *testing.T) {
	cases := []struct {
		a, b, want []string
	}{
		{[]string{"email"}, []string{"phone"}, []string{"email", "phone"}},
		{[]string{"email"}, []string{"email"}, []string{"email"}},
		{nil, []string{"ssn"}, []string{"ssn"}},
		{[]string{"a", "b"}, []string{"b", "c"}, []string{"a", "b", "c"}},
		{nil, nil, nil},
	}
	for _, tc := range cases {
		got := mergeUniqueColumns(tc.a, tc.b)
		if len(got) == 0 && len(tc.want) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("mergeUniqueColumns(%v,%v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
