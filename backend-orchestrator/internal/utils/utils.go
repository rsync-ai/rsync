// Package utils provides shared utility functions for the backend orchestrator
package utils

// SafePrefix returns the first n characters of s, or the whole string if shorter
func SafePrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// SafeID8 returns the first 8 characters of an ID, or the full ID if shorter
// Returns "unknown" if the ID is empty
func SafeID8(id string) string {
	if id == "" {
		return "unknown"
	}
	return SafePrefix(id, 8)
}

