package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
)

// orchestratorErrorDetailMaxLen bounds the message taken from an orchestrator
// error body so a large or unexpected body never lands whole in a response.
const orchestratorErrorDetailMaxLen = 500

// orchestratorErrorDetail extracts the human-readable reason from a non-200
// orchestrator response body.
//
// The orchestrator answers `{"error": "<short label>", "details": "<reason>"}`.
// For /agent/discover-schema a 422 carries the connector's own message in
// `details` (e.g. a DNS or auth error), which is what the user needs to see —
// callers used to forward the raw JSON text instead. Falls back to `error`,
// then to the trimmed body, then to the HTTP status.
func orchestratorErrorDetail(statusCode int, body []byte) string {
	var parsed struct {
		Error   string `json:"error"`
		Details string `json:"details"`
		Message string `json:"message"`
	}
	msg := ""
	if err := json.Unmarshal(body, &parsed); err == nil {
		for _, candidate := range []string{parsed.Details, parsed.Message, parsed.Error} {
			if s := strings.TrimSpace(candidate); s != "" {
				msg = s
				break
			}
		}
	} else {
		msg = strings.TrimSpace(string(body))
	}
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", statusCode)
	}
	if r := []rune(msg); len(r) > orchestratorErrorDetailMaxLen {
		msg = string(r[:orchestratorErrorDetailMaxLen]) + "…"
	}
	return msg
}
