package config

import "testing"

// The usage panel is a BILLING surface: plan, pipeline/query limits, trial
// expiry, metered transfer GB. Its default therefore follows plan enforcement
// rather than standing alone, so a self-host that has already opted out of
// billing does not have to opt out twice. These cases pin BOTH directions --
// a one-sided table would keep passing if the derivation were deleted.
func TestResolveUsagePanelFollowsBillingUnlessPinned(t *testing.T) {
	const (
		unset = "\x00" // distinct from the empty string, which is a real value
	)
	cases := []struct {
		name    string
		billing string
		panel   string
		want    bool
	}{
		// Cloud sets neither variable. The panel must show.
		{"both unset is cloud", unset, unset, true},
		// The OSS quickstart compose and the Helm chart set only the billing
		// flag. The panel must disappear with no second flag to remember.
		{"billing off hides the panel", "false", unset, false},
		{"billing 0 hides the panel", "0", unset, false},
		// billingEnforced() trims before parsing; the derived default must too,
		// or a value with stray whitespace silently re-enables the panel on a
		// self-host whose quotas are not enforced.
		{"billing off with whitespace still hides", " false ", unset, false},
		// Fail toward the cloud behaviour on junk, exactly as billing does.
		{"unparseable billing shows the panel", "yes-please", unset, true},
		{"billing on shows the panel", "true", unset, true},
		// The explicit flag wins in both directions.
		{"pinned on beats billing off", "false", "true", true},
		{"pinned off beats billing on", "true", "false", false},
		{"pinned off beats billing unset", unset, "false", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.billing != unset {
				t.Setenv("RSYNC_BILLING_ENFORCED", tc.billing)
			} else {
				t.Setenv("RSYNC_BILLING_ENFORCED", "")
			}
			if tc.panel != unset {
				t.Setenv("FEATURE_USAGE_PANEL", tc.panel)
			} else {
				t.Setenv("FEATURE_USAGE_PANEL", "")
			}
			if got := resolveUsagePanel(); got != tc.want {
				t.Fatalf("resolveUsagePanel() = %v, want %v (billing=%q panel=%q)",
					got, tc.want, tc.billing, tc.panel)
			}
		})
	}
}

// The frontend reads this flag over /api/v1/features at runtime, which is the
// only path that reaches a PREBUILT image: NEXT_PUBLIC_* is inlined at Next.js
// build time, so an OSS compose cannot move it. Dropping the key from the
// response silently restores the panel's build-time default (shown) on every
// self-host, so the key's presence is worth asserting on its own.
func TestToAPIResponseCarriesUsagePanel(t *testing.T) {
	for _, want := range []bool{true, false} {
		resp := (&FeatureFlags{UsagePanel: want}).ToAPIResponse()
		got, ok := resp["usage_panel"]
		if !ok {
			t.Fatalf("usage_panel missing from ToAPIResponse(); keys present: %v", keysOf(resp))
		}
		if got != want {
			t.Fatalf("usage_panel = %v, want %v", got, want)
		}
	}
}

func keysOf(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
