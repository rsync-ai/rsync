package crypto

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	// Set development environment for testing
	os.Setenv("ENVIRONMENT", "development")
	defer os.Unsetenv("ENVIRONMENT")

	testCases := []struct {
		name      string
		plaintext string
	}{
		{"empty string", ""},
		{"simple text", "Hello, World!"},
		{"special chars", "password!@#$%^&*()"},
		{"unicode", "こんにちは世界"},
		{"json", `{"key": "value", "nested": {"a": 1}}`},
		{"long text", strings.Repeat("a", 10000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encrypt
			ciphertext, err := EncryptString(tc.plaintext)
			if err != nil {
				t.Fatalf("Encrypt failed: %v", err)
			}

			// Ciphertext should be different from plaintext
			if ciphertext == tc.plaintext && tc.plaintext != "" {
				t.Error("Ciphertext should not equal plaintext")
			}

			// Decrypt
			decrypted, err := DecryptString(ciphertext)
			if err != nil {
				t.Fatalf("Decrypt failed: %v", err)
			}

			// Should match original
			if decrypted != tc.plaintext {
				t.Errorf("Decrypted text mismatch: got %q, want %q", decrypted, tc.plaintext)
			}
		})
	}
}

func TestEncryptProducesDifferentCiphertexts(t *testing.T) {
	// Set development environment for testing
	os.Setenv("ENVIRONMENT", "development")
	defer os.Unsetenv("ENVIRONMENT")

	plaintext := "same input"
	ciphertext1, _ := EncryptString(plaintext)
	ciphertext2, _ := EncryptString(plaintext)

	// Due to random nonce, same plaintext should produce different ciphertexts
	if ciphertext1 == ciphertext2 {
		t.Error("Encrypting same text twice should produce different ciphertexts (due to random nonce)")
	}
}

func TestDecryptInvalidCiphertext(t *testing.T) {
	// Set development environment for testing
	os.Setenv("ENVIRONMENT", "development")
	defer os.Unsetenv("ENVIRONMENT")

	testCases := []struct {
		name       string
		ciphertext string
	}{
		{"not base64", "not-valid-base64!!!"},
		{"too short", "YWJj"}, // "abc" in base64
		{"tampered", "dGFtcGVyZWQgZGF0YQ=="},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecryptString(tc.ciphertext)
			if err == nil {
				t.Error("Expected error for invalid ciphertext")
			}
		})
	}
}

func TestGetEncryptionKeyDevMode(t *testing.T) {
	// Clear any existing key
	os.Unsetenv("ENCRYPTION_KEY")
	os.Setenv("ENVIRONMENT", "development")
	defer os.Unsetenv("ENVIRONMENT")

	key := GetEncryptionKey()
	if len(key) != 32 {
		t.Errorf("Expected 32-byte key, got %d bytes", len(key))
	}
}

func TestGetEncryptionKeyWithCustomKey(t *testing.T) {
	customKey := "this-is-a-32-character-test-key!"
	os.Setenv("ENCRYPTION_KEY", customKey)
	os.Setenv("ENVIRONMENT", "development")
	defer func() {
		os.Unsetenv("ENCRYPTION_KEY")
		os.Unsetenv("ENVIRONMENT")
	}()

	key := GetEncryptionKey()
	if len(key) != 32 {
		t.Errorf("Expected 32-byte key, got %d bytes", len(key))
	}
	if string(key) != customKey[:32] {
		t.Error("Key should match custom key (truncated to 32 bytes)")
	}
}

// GetEncryptionKey fails closed in production via log.Fatalf (-> os.Exit(1)),
// not panic, so these cases cannot be caught with recover(). The standard Go
// idiom for testing an os.Exit path is to re-exec the test binary in a
// subprocess and assert it exits non-zero. The child branch sets up its own
// env so it is independent of whatever the parent process inherited.

func TestGetEncryptionKeyFatalInProduction(t *testing.T) {
	if os.Getenv("CRYPTO_FATAL_SUBPROCESS") == "1" {
		// Child: a missing key in production must trigger log.Fatalf.
		os.Unsetenv("ENCRYPTION_KEY")
		os.Unsetenv("ENCRYPTION_KEYS")
		os.Setenv("ENVIRONMENT", "production")
		GetEncryptionKey()
		return // reached only if it did NOT exit -> parent assertion fails
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGetEncryptionKeyFatalInProduction$")
	cmd.Env = append(os.Environ(), "CRYPTO_FATAL_SUBPROCESS=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit (log.Fatalf) when ENCRYPTION_KEY unset in production; got success. output:\n%s", out)
	}
	if _, ok := err.(*exec.ExitError); !ok {
		t.Fatalf("expected *exec.ExitError from subprocess, got %T: %v", err, err)
	}
}

func TestGetEncryptionKeyFatalShortKey(t *testing.T) {
	if os.Getenv("CRYPTO_FATAL_SUBPROCESS") == "1" {
		// Child: a too-short key in production must trigger log.Fatalf.
		os.Unsetenv("ENCRYPTION_KEYS")
		os.Setenv("ENCRYPTION_KEY", "tooshort")
		os.Setenv("ENVIRONMENT", "production")
		GetEncryptionKey()
		return // reached only if it did NOT exit -> parent assertion fails
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGetEncryptionKeyFatalShortKey$")
	cmd.Env = append(os.Environ(), "CRYPTO_FATAL_SUBPROCESS=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit (log.Fatalf) for too-short key in production; got success. output:\n%s", out)
	}
	if _, ok := err.(*exec.ExitError); !ok {
		t.Fatalf("expected *exec.ExitError from subprocess, got %T: %v", err, err)
	}
}

func TestEncryptDecryptWithBytes(t *testing.T) {
	// Set development environment for testing
	os.Setenv("ENVIRONMENT", "development")
	defer os.Unsetenv("ENVIRONMENT")

	// Test with binary data
	plaintext := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}

	ciphertext, err := Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	decrypted, err := Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	if len(decrypted) != len(plaintext) {
		t.Fatalf("Length mismatch: got %d, want %d", len(decrypted), len(plaintext))
	}

	for i := range plaintext {
		if decrypted[i] != plaintext[i] {
			t.Errorf("Byte mismatch at position %d: got %x, want %x", i, decrypted[i], plaintext[i])
		}
	}
}
