package executor

import (
	"strings"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
)

// discoveryFailureMaxLen caps the connector's error text carried by
// DiscoveryFailedError. The text reaches the UI and can reach an LLM prompt
// (the table-selection "reason"), so it is scrubbed and bounded.
const discoveryFailureMaxLen = 500

// DiscoveryFailedError means the connector answered discover_schema but
// reported that discovery itself failed (bad host, auth, missing database).
//
// The MCP call succeeds in that case — connectors put the failure in the
// result's overall_status and warnings instead of returning success:false — so
// without this check a failed discovery reached every caller as an empty table
// list and the UI said "No tables found" instead of the real error.
type DiscoveryFailedError struct {
	Connector string
	// Message is the connector's own error text, already scrubbed.
	Message string
}

func (e *DiscoveryFailedError) Error() string {
	return e.Message
}

// discoveryFailure returns a *DiscoveryFailedError when a discover_schema
// result says discovery failed, and nil otherwise. It treats a result as failed
// when:
//
//   - overall_status is "failed", or
//   - overall_status is "partial_success", no tables came back, and at least one
//     warning has severity "error". The database connectors set "failed" and
//     then call add_warning(..., "error", ...), which overwrites the status with
//     "partial_success" (postgresql connector.py add_warning), so a refused
//     connection arrives looking like a partial result with zero tables.
//
// A partial_success that still returned tables is a real partial result and is
// left alone, as is a success with zero tables (an empty database).
func discoveryFailure(connectorType string, result map[string]interface{}) error {
	if result == nil {
		return nil
	}
	status, _ := result["overall_status"].(string)
	status = strings.ToLower(strings.TrimSpace(status))

	switch status {
	case "failed":
	case "partial_success":
		if discoveredTableCount(result) > 0 || len(errorSeverityWarnings(result)) == 0 {
			return nil
		}
	default:
		return nil
	}

	msg := strings.Join(discoveryErrorMessages(result), "; ")
	if msg == "" {
		msg = "schema discovery failed"
	}
	return &DiscoveryFailedError{
		Connector: connectorType,
		Message:   llmscrub.ScrubMax(msg, discoveryFailureMaxLen),
	}
}

func discoveredTableCount(result map[string]interface{}) int {
	tables, _ := result["tables"].([]interface{})
	return len(tables)
}

// errorSeverityWarnings returns the messages of structured warnings whose
// severity is "error".
func errorSeverityWarnings(result map[string]interface{}) []string {
	objs, _ := result["warnings_objects"].([]interface{})
	var out []string
	for _, o := range objs {
		m, ok := o.(map[string]interface{})
		if !ok {
			continue
		}
		if sev, _ := m["severity"].(string); !strings.EqualFold(strings.TrimSpace(sev), "error") {
			continue
		}
		if text, _ := m["message"].(string); strings.TrimSpace(text) != "" {
			out = append(out, strings.TrimSpace(text))
		}
	}
	return out
}

// discoveryErrorMessages collects the connector's error text, most specific
// first: a top-level error/errors field, then error-severity structured
// warnings, then the plain warnings_messages list (the only field some
// connectors, e.g. mongodb, fill). Duplicates are dropped.
func discoveryErrorMessages(result map[string]interface{}) []string {
	var out []string
	seen := map[string]bool{}
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		out = append(out, s)
	}

	if s, ok := result["error"].(string); ok {
		add(s)
	}
	if list, ok := result["errors"].([]interface{}); ok {
		for _, e := range list {
			if s, ok := e.(string); ok {
				add(s)
			}
		}
	}
	for _, s := range errorSeverityWarnings(result) {
		add(s)
	}
	// Plain messages also hold non-fatal warnings; use them only when nothing
	// more specific was reported.
	if len(out) == 0 {
		if list, ok := result["warnings_messages"].([]interface{}); ok {
			for _, e := range list {
				if s, ok := e.(string); ok {
					add(s)
				}
			}
		}
	}
	return out
}
