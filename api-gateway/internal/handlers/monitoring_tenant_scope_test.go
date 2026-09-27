package handlers

import (
	"strings"
	"testing"

	"api-gateway/internal/security"
)

// TestSentinelIssueTenantPredicate pins the tenant scope on
// GET /api/v1/monitoring/sentinel/issues.
//
// The route gated only on the PLATFORM role axis (power_user or admin). That axis
// is workspace-blind, and the table is not: for 'cdc_pipeline' / 'batch_pipeline'
// rows the component_id IS a pipeline id and metadata.pipeline_name is its name,
// so a power_user in any one workspace could enumerate every other tenant's
// pipeline ids, names and failure descriptions.
func TestSentinelIssueTenantPredicate(t *testing.T) {
	const caller = "11111111-1111-1111-1111-111111111111"

	t.Run("non-admin is scoped to their workspaces", func(t *testing.T) {
		for _, role := range []security.UserRole{security.RolePowerUser, security.RoleUser} {
			frag, args := sentinelIssueTenantPredicate(role, caller, 3)
			if frag == "" {
				t.Fatalf("role %v got NO predicate — every tenant's rows are readable", role)
			}
			// The membership join is the whole point: without it the predicate
			// could be present and still cross-tenant.
			for _, want := range []string{"workspace_members", "wm.user_id::text = $3", "component_id"} {
				if !strings.Contains(frag, want) {
					t.Errorf("role %v predicate is missing %q:\n%s", role, want, frag)
				}
			}
			// Infrastructure rows name Kafka topics and containers across every
			// workspace, so a non-admin must not reach them at all.
			if !strings.Contains(frag, "component_type IN ('cdc_pipeline', 'batch_pipeline')") {
				t.Errorf("role %v can still read deployment-wide infrastructure rows:\n%s", role, frag)
			}
			if len(args) != 1 || args[0] != caller {
				t.Errorf("role %v bound args = %v; want exactly the caller id", role, args)
			}
		}
	})

	// Control: without this case the test above would also pass on a predicate
	// that is returned unconditionally, which would silently blank the admin
	// health page rather than scope it.
	t.Run("platform admin keeps the unscoped view", func(t *testing.T) {
		frag, args := sentinelIssueTenantPredicate(security.RoleAdmin, caller, 3)
		if frag != "" || args != nil {
			t.Fatalf("admin was scoped; the admin health surface renders cross-workspace rows by design:\n%s", frag)
		}
	})

	// The placeholder must follow the caller's index, not be hard-coded: the
	// fragment is appended after a variable number of optional filters.
	t.Run("placeholder tracks the argument index", func(t *testing.T) {
		for _, idx := range []int{1, 4, 7} {
			frag, _ := sentinelIssueTenantPredicate(security.RolePowerUser, caller, idx)
			if !strings.Contains(frag, "$"+itoa(idx)) {
				t.Errorf("argIdx %d did not reach the placeholder:\n%s", idx, frag)
			}
		}
	})
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
