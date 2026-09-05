package heal

import (
	"strings"
	"testing"
)

// TestStatusFromErrorPrefixRecoversTheEncodedStatus covers the whole table.
//
// The two credential statuses are the regression guard. The previous
// implementation was a hand-written switch that sliced by a literal count —
// `len(errMsg) > 30 && errMsg[:30] == "waiting_for_credential_reauth"` — and
// those literals are 29 and 28 characters, so both comparisons were between a
// 30-character slice and a shorter string and could never be true. Every
// credential failure was therefore diagnosed from its raw executions.status
// instead of the auth status the executor had encoded, and the healer never
// routed those runs to RefreshAuth.
func TestStatusFromErrorPrefixRecoversTheEncodedStatus(t *testing.T) {
	tests := []struct {
		name   string
		status string
		errMsg string
		want   string
	}{
		{
			name:   "silent_drop_detected",
			status: "failed",
			errMsg: "silent_drop_detected: source had 12000 rows, destination landed 0",
			want:   "silent_drop_detected",
		},
		{
			name:   "silent_partial_drop_detected",
			status: "failed",
			errMsg: "silent_partial_drop_detected: 3 of 40 tables landed nothing",
			want:   "silent_partial_drop_detected",
		},
		{
			// 29 chars — unreachable before the fix.
			name:   "waiting_for_credential_reauth",
			status: "credential_check_failed",
			errMsg: "waiting_for_credential_reauth: token expired",
			want:   "waiting_for_credential_reauth",
		},
		{
			// 28 chars — unreachable before the fix.
			name:   "waiting_for_credential_scope",
			status: "credential_check_failed",
			errMsg: "waiting_for_credential_scope: missing read:tables",
			want:   "waiting_for_credential_scope",
		},
		{
			name:   "no encoded prefix falls back to the status column",
			status: "failed",
			errMsg: "dial tcp 10.0.0.4:5432: connect: connection refused",
			want:   "failed",
		},
		{
			name:   "empty error message falls back",
			status: "error",
			errMsg: "",
			want:   "error",
		},
		{
			// Only a PREFIX counts. A status quoted mid-sentence is somebody
			// describing a failure, not the executor encoding one.
			name:   "status mentioned mid-string is not a prefix",
			status: "failed",
			errMsg: "retry after silent_drop_detected on the previous attempt",
			want:   "failed",
		},
		{
			name:   "both empty stays empty",
			status: "",
			errMsg: "",
			want:   "",
		},
		{
			// The prefix is matched exactly as written; the executor emits it
			// lowercase and this is not a normalising function.
			name:   "match is case-sensitive",
			status: "failed",
			errMsg: "Silent_Drop_Detected: something",
			want:   "failed",
		},
		{
			// The bare status with no separator is still a prefix of itself.
			name:   "bare status with no detail",
			status: "failed",
			errMsg: "silent_drop_detected",
			want:   "silent_drop_detected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := statusFromErrorPrefix(tt.status, tt.errMsg); got != tt.want {
				t.Errorf("statusFromErrorPrefix(%q, %q) = %q, want %q",
					tt.status, tt.errMsg, got, tt.want)
			}
		})
	}
}

// The invariant that actually matters: statusFromErrorPrefix returns on first
// match, so no entry may be shadowed by an earlier one it starts with. Nothing
// in today's table shadows anything — this catches the day someone adds
// "silent_drop_detected_partial" below "silent_drop_detected" and silently
// makes it unreachable, which is the same class of bug that made the two
// credential statuses dead before the HasPrefix rewrite.
func TestNoErrPrefixStatusIsShadowedByAnEarlierOne(t *testing.T) {
	for i, earlier := range errPrefixStatuses {
		for j := i + 1; j < len(errPrefixStatuses); j++ {
			if strings.HasPrefix(errPrefixStatuses[j], earlier) {
				t.Errorf("errPrefixStatuses[%d] %q starts with [%d] %q — "+
					"first match wins, so the longer one is unreachable; list it first",
					j, errPrefixStatuses[j], i, earlier)
			}
		}
	}
}

// The table is additionally kept ordered longest-first, which is what the
// comment above it promises. That ordering makes the invariant above hold
// automatically for any future addition, so it is cheaper to maintain than to
// reason about case by case.
func TestErrPrefixStatusesAreOrderedLongestFirst(t *testing.T) {
	for i := 1; i < len(errPrefixStatuses); i++ {
		if len(errPrefixStatuses[i]) > len(errPrefixStatuses[i-1]) {
			t.Errorf("errPrefixStatuses[%d] %q (%d chars) is longer than [%d] %q (%d chars)",
				i, errPrefixStatuses[i], len(errPrefixStatuses[i]),
				i-1, errPrefixStatuses[i-1], len(errPrefixStatuses[i-1]))
		}
	}
}

// Every entry must be a status the RuleBasedDiagnoser can act on, and none may
// carry the separator — the value is compared as a prefix of the raw message,
// so a trailing ":" here would stop it matching "status: detail".
func TestErrPrefixStatusesAreCleanStatusTokens(t *testing.T) {
	for _, s := range errPrefixStatuses {
		if s == "" {
			t.Error("empty entry: strings.HasPrefix(anything, \"\") is always true, " +
				"so this would swallow every error message")
			continue
		}
		if strings.ContainsAny(s, ": \t\n") {
			t.Errorf("%q contains a separator or whitespace — it is matched against the "+
				"raw message as a prefix, so it must be the bare status token", s)
		}
		if strings.TrimSpace(s) != s {
			t.Errorf("%q has surrounding whitespace", s)
		}
	}
}
