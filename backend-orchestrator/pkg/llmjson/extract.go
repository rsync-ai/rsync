// Package llmjson holds the one implementation of "get a JSON object out of an
// LLM reply" that every Go service in this repo shares.
//
// Models fence their output even when the prompt says "bare JSON only". Handing
// that raw string to json.Unmarshal fails with
// `invalid character '`' looking for beginning of value`, and because every
// caller in this repo treats a parse error as "the LLM path is unavailable",
// the failure is silent: the user gets a canned fallback and the log carries one
// warning. The healer hit this in 2026-07 (PR #364); the chat pipeline hit it
// again in 2026-09 against Vertex AI's Gemini, which fences every reply.
//
// This lives in pkg/ rather than beside one caller because api-gateway cannot
// import backend-orchestrator's internal/ tree, and a second copy is how the
// next caller ends up with the old behaviour. Same reasoning as pkg/llmscrub.
package llmjson

import "strings"

// ExtractObject pulls a JSON object out of an LLM response that may be wrapped
// in a markdown code fence (```json … ```) or padded with prose.
//
// Callers pass the result straight to json.Unmarshal. When no object is found
// the trimmed input is returned unchanged, so the caller still surfaces a real
// parse error rather than an empty-string one.
func ExtractObject(s string) string {
	s = strings.TrimSpace(s)
	// Strip a leading ``` / ```json fence and its matching trailing ```.
	if strings.HasPrefix(s, "```") {
		if nl := strings.IndexByte(s, '\n'); nl != -1 {
			s = s[nl+1:] // drop the opening fence line (``` or ```json)
		} else {
			s = strings.TrimPrefix(s, "```")
		}
		if idx := strings.LastIndex(s, "```"); idx != -1 {
			s = s[:idx] // drop the closing fence
		}
		s = strings.TrimSpace(s)
	}
	// If prose still surrounds the object, slice from the first { to the last }.
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		if start := strings.IndexByte(s, '{'); start != -1 {
			if end := strings.LastIndexByte(s, '}'); end > start {
				s = s[start : end+1]
			}
		}
	}
	return strings.TrimSpace(s)
}
