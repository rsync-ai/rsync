package handlers

import "testing"

// Preview redaction is keyed on the RESULT column name, so an aggregate alias is
// judged by the same rule as a real column. Short keywords like "tin" used to be
// matched as bare substrings, which redacted `COUNT(DISTINCT id) AS distinct_ids`
// (dis-TIN-ct) down to `***` while `max_id` beside it rendered fine.
func TestIsPIIColumnName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		// The regression: ordinary words that merely contain a short keyword.
		{"distinct_ids", false},
		{"distinct_count", false},
		{"routing_key", false},   // rou-TIN-g
		{"setting_name", false},  // set-TIN-g
		{"running_total", false}, // run-NIN-g
		{"warning_count", false}, // war-NIN-g
		{"using_index", false},   // u-SIN-g
		{"business_unit", false}, // bu-SIN-ess
		{"company_name", false},  // com-PAN-y
		{"expansion", false},     // ex-PAN-sion
		{"excellent_score", false},
		{"max_id", false},
		{"id", false},
		{"user_id", false},
		{"created_at", false},

		// MySQL returns the whole expression as the result column name, which is
		// how the original prod repro looked: only the DISTINCT column was `***`,
		// its three neighbours in the same row rendered fine.
		{"count(DISTINCT product_id)", false},
		{"COUNT(DISTINCT product_id)", false},
		{"count(*)", false},
		{"min(product_id)", false},
		{"max(product_id)", false},

		// Still PII: the same short keywords as their own token.
		{"tin", true},
		{"tin_number", true},
		{"taxpayer_tin", true},
		{"customerTin", true},
		{"tin2", true},
		{"TIN", true},
		{"pan", true},
		{"pan_number", true},
		{"customerPAN", true},
		{"nin", true},
		{"sin", true},
		{"cell", true},
		{"cell_phone", true},

		// Unambiguous keywords keep matching as substrings.
		{"email", true},
		{"user_email_address", true},
		{"e-mail", true},
		{"phone_number", true},
		{"mobile", true},
		{"msisdn", true},
		{"ssn", true},
		{"social_security_number", true},
		{"national_id", true},
		{"id_number", true},
		{"passport_number", true},
		{"drivers_license", true},
		{"tax_id", true},
		{"aadhaar", true},
		{"credit_card", true},
		{"cc_number", true},
		{"card_number", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPIIColumnName(tc.name); got != tc.want {
				t.Fatalf("isPIIColumnName(%q) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestColumnNameTokens(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"distinct_ids", []string{"distinct", "ids"}},
		{"customerPAN", []string{"customer", "pan"}},
		{"customerTin", []string{"customer", "tin"}},
		{"tin2", []string{"tin"}},
		{"  tin  ", []string{"tin"}},
		{"tax-id", []string{"tax", "id"}},
		{"a.b c", []string{"a", "b", "c"}},
		{"", nil},
		{"123", nil},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := columnNameTokens(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("columnNameTokens(%q) = %v, want %v", tc.in, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("columnNameTokens(%q) = %v, want %v", tc.in, got, tc.want)
				}
			}
		})
	}
}

// Secrets keep bare-substring matching on purpose: over-redacting a credential is
// the safe direction, and none of those keywords are common English substrings.
func TestIsSecretColumnNameUnchanged(t *testing.T) {
	for _, name := range []string{"password", "user_passwd", "pwd_hash", "client_secret", "access_token", "api_key", "apikey", "private_key"} {
		if !isSecretColumnName(name) {
			t.Fatalf("isSecretColumnName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"id", "created_at", "distinct_ids", "status"} {
		if isSecretColumnName(name) {
			t.Fatalf("isSecretColumnName(%q) = true, want false", name)
		}
	}
}
