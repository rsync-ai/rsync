package naming

import "testing"

func TestIsSuspiciousIdentifier(t *testing.T) {
	bad := []string{"the", "The", " the ", `"the"`, "a", "an", "into", "destination", "TABLE"}
	for _, s := range bad {
		if !IsSuspiciousIdentifier(s) {
			t.Errorf("IsSuspiciousIdentifier(%q) = false, want true", s)
		}
	}
	good := []string{"shopify", "public", "analytics", "shop_isolation_a", "warehouse2"}
	for _, s := range good {
		if IsSuspiciousIdentifier(s) {
			t.Errorf("IsSuspiciousIdentifier(%q) = true, want false", s)
		}
	}
}

func TestValidateNamespace(t *testing.T) {
	cases := []struct {
		name    string
		wantErr bool
	}{
		{"shopify", false},
		{"public", false},
		{"shop_isolation_a", false},
		{"_staging", false},
		{"analytics2024", false},
		{"", true},                       // empty
		{"the", true},                    // stopword
		{"destination", true},            // pipeline vocabulary
		{"2024analytics", true},          // leading digit
		{"my-schema", true},              // hyphen not allowed
		{"shop;DROP TABLE", true},        // injection chars
		{"public.products", true},        // dot not allowed (qualified, not a bare ns)
		{"схема", true},                  // non-ASCII
		{string(make([]byte, 64)), true}, // too long
	}
	for _, c := range cases {
		got := ValidateNamespace(c.name)
		if (got != "") != c.wantErr {
			t.Errorf("ValidateNamespace(%q) = %q, wantErr=%v", c.name, got, c.wantErr)
		}
	}
}
