package security

import (
	"regexp"
	"strings"
)

// SensitiveKeys is the list of keys that should be masked in logs
var SensitiveKeys = []string{
	"password",
	"secret",
	"secret_key",
	"secret_access_key",
	"api_key",
	"apikey",
	"token",
	"access_key",
	"access_key_id",
	"refresh_token",
	"access_token",
	"bearer",
	"authorization",
	"credentials",
	"credentials_json",
	"private_key",
	"client_secret",
	"aws_secret_access_key",
	"aws_access_key_id",
	"connection_string",
	"conn_str",
	"db_password",
	// PII fields
	"email",
	"phone",
	"phone_number",
	"address",
	"ssn",
	"social_security",
	"credit_card",
	"user_id", // when used as query predicate
	"customer_id",
}

// MaskedValue is the replacement for sensitive values
const MaskedValue = "********"

// MaskConnectionString masks passwords and secrets in connection strings
func MaskConnectionString(connStr string) string {
	if connStr == "" {
		return connStr
	}

	// Mask password patterns like: password=xxx, pwd=xxx, passwd=xxx
	patterns := []string{
		`(?i)(password|pwd|passwd)=([^;&\s]+)`,
		`(?i)(secret|api_key|token)=([^;&\s]+)`,
	}

	result := connStr
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		result = re.ReplaceAllString(result, "${1}="+MaskedValue)
	}

	return result
}

// MaskJSON masks sensitive fields in a JSON-like string (for logging raw payloads)
func MaskJSON(jsonStr string) string {
	if jsonStr == "" {
		return jsonStr
	}

	result := jsonStr
	for _, key := range SensitiveKeys {
		// Match patterns like "password": "value" or "password":"value"
		pattern := `(?i)"` + regexp.QuoteMeta(key) + `"\s*:\s*"[^"]*"`
		re := regexp.MustCompile(pattern)
		result = re.ReplaceAllString(result, `"`+key+`": "`+MaskedValue+`"`)
	}

	return result
}

// isSensitiveKey checks if a key should be masked
func isSensitiveKey(key string) bool {
	keyLower := strings.ToLower(key)
	for _, sensitive := range SensitiveKeys {
		if keyLower == sensitive || strings.Contains(keyLower, sensitive) {
			return true
		}
	}
	return false
}
