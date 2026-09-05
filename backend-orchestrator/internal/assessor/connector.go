// Generic connector pre-flight assessor (universal fallback).
//
// Purpose: give EVERY source type — not just the databases with a dedicated
// deep assessor (mysql/postgresql) — a real, read-only readiness check before
// a pipeline runs. SaaS / REST / GraphQL / cloud-storage / warehouse sources
// don't have server-config knobs like wal_level; their "required configuration"
// is instead: (a) all required connection fields are present, and (b) the
// supplied credentials actually authenticate AND carry the permissions/scopes
// the connector needs. We verify all of that by asking the connector itself,
// via its standard MCP `test_connection` tool — the SAME check the connector
// runs at sync time, surfaced up-front.
//
// This assessor is READ-ONLY: it never mutates source configuration. It only
// connects and reports. It is wired as the Registry's default (fallback), so
// any source without a dedicated assessor is still covered.
//
// Classification of the connector's response into actionable checks:
//   - missing required fields      → CONNECTOR_CONFIG_INCOMPLETE (error)
//   - 401/403/auth/scope/permission → CONNECTOR_AUTH_FAILED      (error)
//   - any other connect failure     → CONNECTOR_CONNECTION_FAILED (error)
//   - could not run the check at all → CONNECTOR_ASSESS_UNAVAILABLE (warning,
//     advisory — an rsync-side infra hiccup must not hard-block the user)
package assessor

import (
	"context"
	"fmt"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// connectorExecutor is the slice of *mcp.Client this assessor needs. Declared
// as an interface so the assessor is unit-testable with a fake and never
// couples to the full client surface.
type connectorExecutor interface {
	Execute(req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error)
}

// ConnectorAssessor implements SourceAssessor generically for any connector by
// delegating the readiness check to the connector's own MCP `test_connection`.
type ConnectorAssessor struct {
	client connectorExecutor
}

// NewConnectorAssessor builds the generic fallback assessor. Pass the shared
// *mcp.Client (it satisfies connectorExecutor).
func NewConnectorAssessor(client connectorExecutor) *ConnectorAssessor {
	return &ConnectorAssessor{client: client}
}

// SourceType is a label only — this assessor is installed via Registry.SetDefault
// (the fallback), not Register, so the name is never used for dispatch.
func (a *ConnectorAssessor) SourceType() string { return "connector" }

// resolveConnectorType picks the connector identity from the Input / config,
// trying the explicit field first then the keys connections.Manager.Get injects.
func resolveConnectorType(in Input) string {
	candidates := []string{
		in.SourceType,
		in.ConnectionConfig["connector_type"],
		in.ConnectionConfig["type"],
		in.ConnectionConfig["connection_type"],
	}
	for _, c := range candidates {
		if v := strings.TrimSpace(c); v != "" {
			return strings.ToLower(v)
		}
	}
	return ""
}

func resolveConnectorVersion(in Input) string {
	candidates := []string{in.Version, in.ConnectionConfig["connector_version"]}
	for _, c := range candidates {
		if v := strings.TrimSpace(c); v != "" {
			return v
		}
	}
	return "latest"
}

func (a *ConnectorAssessor) Assess(ctx context.Context, in Input) (*Result, error) {
	connType := resolveConnectorType(in)
	version := resolveConnectorVersion(in)

	r := &Result{SourceType: connType, Checks: []Check{}}

	if connType == "" {
		// We genuinely don't know what this source is — surface as a warning
		// (advisory) rather than a hard block, since we cannot point the user
		// at a specific fix.
		r.Checks = append(r.Checks, Check{
			Code:     "CONNECTOR_TYPE_UNKNOWN",
			Severity: SeverityWarning,
			Passed:   false,
			Message:  "Could not determine the source connector type, so source readiness could not be checked.",
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Re-select the source connection in the pipeline builder",
					"Confirm the source connection was created successfully",
				},
				EstimatedMinutes: 2,
			},
		})
		Summarize(r)
		return r, nil
	}

	if a.client == nil {
		r.Checks = append(r.Checks, unavailableCheck(connType, "assessment client not configured"))
		Summarize(r)
		return r, nil
	}

	// Ask the connector to test itself. This validates required-field presence
	// (server-side ValidateConfig) AND performs the real connect+auth handshake.
	resp, err := a.client.Execute(mcp.ExecuteRequest{
		Connector: connType,
		Version:   version,
		Operation: "test_connection",
		Config:    in.ConnectionConfig,
	})
	if err != nil {
		// Transport / infra error running the probe — advisory, do not block.
		r.Checks = append(r.Checks, unavailableCheck(connType, err.Error()))
		Summarize(r)
		return r, nil
	}

	if resp != nil && resp.Success {
		r.Checks = append(r.Checks, Check{
			Code:     "CONNECTOR_CONNECTION",
			Severity: SeverityInfo,
			Passed:   true,
			Message:  fmt.Sprintf("Connected and authenticated to %s successfully — required configuration and credentials are valid.", connType),
		})
		// KI-1-extend: test_connection only proves the credential authenticates,
		// NOT that it has read scope for each SELECTED object. A SaaS token can
		// pass auth yet lack access to a specific resource (e.g. Shopify "this app
		// isn't approved to access protected customer data"), which today surfaces
		// only at run time as a mid-pipeline failure. Probe each selected table
		// with a cheap read (export limit=1) up-front so a scope/permission gap
		// blocks at assess time with the connector's real error. Read-only; only a
		// clear permission/scope wall blocks — any other outcome stays advisory so
		// a connector quirk or rsync-side hiccup can never become a false block.
		for _, table := range in.Tables {
			if t := strings.TrimSpace(table); t != "" {
				r.Checks = append(r.Checks, a.probeTableRead(connType, version, in.ConnectionConfig, t))
			}
		}
		Summarize(r)
		return r, nil
	}

	msg := ""
	if resp != nil {
		msg = strings.TrimSpace(resp.Error)
	}
	if msg == "" {
		msg = "the connector reported the connection is not ready"
	}
	r.Checks = append(r.Checks, classifyConnectorFailure(connType, msg))
	Summarize(r)
	return r, nil
}

// probeTableRead does a read-only export(limit=1) against one SELECTED table to
// confirm the credential actually has read scope for it — not just that it
// authenticates. The only BLOCKING outcome is a clear auth/scope/permission wall
// (CONNECTOR_TABLE_READ_FORBIDDEN); a transport error or any other connector
// failure is advisory, so a connector that implements export oddly or an
// rsync-side hiccup can never turn into a false block on a source that's fine.
func (a *ConnectorAssessor) probeTableRead(connType, version string, cfg map[string]string, table string) Check {
	resp, err := a.client.Execute(mcp.ExecuteRequest{
		Connector: connType,
		Version:   version,
		Operation: "export",
		Config:    cfg,
		Params: map[string]interface{}{
			"table":  table,
			"format": "json",
			"limit":  1,
		},
	})
	if err != nil {
		return Check{
			Code:     "CONNECTOR_TABLE_READ_UNVERIFIED",
			Severity: SeverityWarning,
			Passed:   false,
			Message:  fmt.Sprintf("Could not verify read access to %q on %s: %s", table, connType, truncate(err.Error(), 200)),
		}
	}
	if resp != nil && resp.Success {
		return Check{
			Code:     "CONNECTOR_TABLE_READABLE",
			Severity: SeverityInfo,
			Passed:   true,
			Message:  fmt.Sprintf("Read access to %q on %s confirmed.", table, connType),
		}
	}
	msg := ""
	if resp != nil {
		msg = strings.TrimSpace(resp.Error)
	}
	if msg == "" {
		msg = "the connector could not read this table"
	}
	// Only a permission/scope/auth wall blocks — this is the actual KI-1-extend
	// bug (e.g. Shopify "this app isn't approved to access the Customer object").
	if containsAny(strings.ToLower(msg),
		"denied", "permission", "scope", "forbidden", "unauthorized",
		"not approved", "not authorized", "401", "403") {
		return Check{
			Code:     "CONNECTOR_TABLE_READ_FORBIDDEN",
			Severity: SeverityError,
			Passed:   false,
			Message:  fmt.Sprintf("Source credential cannot read %q on %s — grant the required scope/permission: %s", table, connType, truncate(msg, 200)),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					fmt.Sprintf("Grant the %s credential read access / the required OAuth scope for %q", connType, table),
					"Reconnect the source (re-run OAuth) so the new scope takes effect",
					"Re-run the readiness check",
				},
				EstimatedMinutes: 5,
			},
		}
	}
	// Any other read failure (table missing, connector quirk, transient): advisory.
	return Check{
		Code:     "CONNECTOR_TABLE_READ_UNVERIFIED",
		Severity: SeverityWarning,
		Passed:   false,
		Message:  fmt.Sprintf("Could not verify read access to %q on %s: %s", table, connType, truncate(msg, 200)),
	}
}

// unavailableCheck builds the advisory (non-blocking) "couldn't run the check"
// finding. We deliberately keep this a warning: an rsync-side problem reaching
// the connector must not permanently block a user whose source may be fine.
func unavailableCheck(connType, detail string) Check {
	return Check{
		Code:     "CONNECTOR_ASSESS_UNAVAILABLE",
		Severity: SeverityWarning,
		Passed:   false,
		Message:  fmt.Sprintf("Could not verify %s source readiness: %s", connType, truncate(detail, 200)),
		Remediation: &diagnose.Remediation{
			Steps: []string{
				"Retry the readiness check in a moment",
				"If it persists, the source may be unreachable from rsync — verify network/firewall access",
			},
			EstimatedMinutes: 3,
		},
	}
}

// classifyConnectorFailure turns a connector test_connection error string into
// the most actionable Check. All branches are 'error' severity (they block):
// every one is a real, user-fixable readiness problem.
func classifyConnectorFailure(connType, msg string) Check {
	lower := strings.ToLower(msg)

	switch {
	case strings.Contains(lower, "missing required config"),
		strings.Contains(lower, "required field"),
		strings.Contains(lower, "is required"):
		return Check{
			Code:     "CONNECTOR_CONFIG_INCOMPLETE",
			Severity: SeverityError,
			Passed:   false,
			Message:  fmt.Sprintf("%s connection is missing required configuration: %s", connType, truncate(msg, 200)),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Open the source connection and fill in every required field",
					"Required fields are listed in the message above",
					"Re-test the connection after saving",
				},
				DocURL:           diagnose.ErrorDocURL("connector-config-incomplete"),
				EstimatedMinutes: 5,
			},
		}

	case containsAny(lower, "401", "403", "unauthorized", "forbidden",
		"authentication", "authenticate", "invalid token", "token expired",
		"invalid api key", "invalid_grant", "access denied", "permission",
		"scope", "insufficient", "not authorized"):
		return Check{
			Code:     "CONNECTOR_AUTH_FAILED",
			Severity: SeverityError,
			Passed:   false,
			Message:  fmt.Sprintf("%s credentials failed to authenticate or lack the required permissions/scopes: %s", connType, truncate(msg, 200)),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Verify the API token / key / OAuth grant for this source is valid and not expired",
					"Confirm the credential carries the read scopes/permissions this connector needs",
					"Re-authorize or paste a fresh token, then re-test the connection",
				},
				DocURL:           diagnose.ErrorDocURL("connector-auth-failed"),
				EstimatedMinutes: 10,
			},
		}

	default:
		return Check{
			Code:     "CONNECTOR_CONNECTION_FAILED",
			Severity: SeverityError,
			Passed:   false,
			Message:  fmt.Sprintf("Could not connect to %s: %s", connType, truncate(msg, 200)),
			Remediation: &diagnose.Remediation{
				Steps: []string{
					"Verify the host/endpoint, port and credentials in the source connection",
					"Confirm the source is reachable from rsync (network/firewall/IP allow-list)",
					"Re-test the connection after correcting it",
				},
				DocURL:           diagnose.ErrorDocURL("connector-connection-failed"),
				EstimatedMinutes: 10,
			},
		}
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
