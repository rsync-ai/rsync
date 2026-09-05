package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"strings"
)

var (
	// ErrInvalidKey is returned when the encryption key is invalid
	ErrInvalidKey = errors.New("invalid encryption key")
	// ErrInvalidCiphertext is returned when the ciphertext is invalid
	ErrInvalidCiphertext = errors.New("invalid ciphertext")
)

const (
	// ciphertext format: "enc.v1:<key_id>:<base64(nonce+ciphertext)>"
	ciphertextV1Prefix = "enc.v1:"
)

// GetEncryptionKey returns the encryption key from environment variable.
// SECURITY: This function enforces strict key requirements:
// - In production (ENVIRONMENT != "development"/"dev"), ENCRYPTION_KEY is REQUIRED
// - Key must be at least 32 characters for AES-256
// - In development mode only, falls back to a default key for convenience
func GetEncryptionKey() []byte {
	keys := getKeyring()
	if len(keys) == 0 {
		return nil
	}
	return keys[0]
}

func isDevEnv() bool {
	return os.Getenv("ENVIRONMENT") == "development" || os.Getenv("ENVIRONMENT") == "dev"
}

func parseKeyringEnv() []string {
	// Prefer ENCRYPTION_KEYS for rotation support.
	// Format: comma-separated list; first is primary.
	raw := strings.TrimSpace(os.Getenv("ENCRYPTION_KEYS"))
	if raw != "" {
		parts := strings.Split(raw, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			s := strings.TrimSpace(p)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	}

	// Backward compat: single key.
	if v := strings.TrimSpace(os.Getenv("ENCRYPTION_KEY")); v != "" {
		return []string{v}
	}
	return nil
}

func normalizeKeyBytes(key string) ([]byte, error) {
	kb := []byte(key)
	if len(kb) < 32 {
		return nil, ErrInvalidKey
	}
	return kb[:32], nil
}

func keyID(key32 []byte) string {
	sum := sha256.Sum256(key32)
	// Short stable identifier (not secret)
	return hex.EncodeToString(sum[:])[:12]
}

func getKeyring() [][]byte {
	keys := parseKeyringEnv()
	if len(keys) == 0 {
		if isDevEnv() {
			// Only allow default key in explicit development mode
			k, _ := normalizeKeyBytes("dev-only-key-please-change-me!!!") // 32 chars
			return [][]byte{k}
		}
		// Fail-closed in production: silently returning nil here previously
		// caused "encryption disabled" — every secret would be stored in plaintext
		// until someone noticed in logs. Better to refuse to start.
		log.Fatalf("crypto: ENCRYPTION_KEY/ENCRYPTION_KEYS not set in non-dev ENVIRONMENT; refusing to start without encryption")
		return nil
	}

	// Reject known unsafe dev defaults in production
	if !isDevEnv() {
		for _, k := range keys {
			if k == "dev-only-key-please-change-me!!!" || k == "dev-encryption-key-32-bytes-long!!" {
				log.Fatalf("crypto: refusing to use known dev encryption key in non-dev ENVIRONMENT; rotate ENCRYPTION_KEY before deploying")
			}
		}
	}

	out := make([][]byte, 0, len(keys))
	for _, k := range keys {
		kb, err := normalizeKeyBytes(k)
		if err != nil {
			log.Printf("crypto: invalid ENCRYPTION_KEY/ENCRYPTION_KEYS entry (must be >= 32 chars); skipping")
			continue
		}
		out = append(out, kb)
	}
	if len(out) == 0 {
		if isDevEnv() {
			k, _ := normalizeKeyBytes("dev-only-key-please-change-me!!!") // 32 chars
			return [][]byte{k}
		}
		log.Fatalf("crypto: no valid ENCRYPTION_KEY/ENCRYPTION_KEYS entries in non-dev ENVIRONMENT; refusing to start")
		return nil
	}
	return out
}

// Encrypt encrypts plaintext using AES-256-GCM (Galois/Counter Mode)
// This provides both confidentiality and authenticity.
func Encrypt(plaintext []byte) (string, error) {
	keys := getKeyring()
	if len(keys) == 0 {
		return "", ErrInvalidKey
	}
	key := keys[0]

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	// Create GCM mode (Galois/Counter Mode) for authenticated encryption
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Create a nonce (number used once) - must be unique for each encryption
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// Encrypt and authenticate - nonce is prepended to ciphertext
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)

	// Encode to base64 for safe storage in databases and transmission
	enc := base64.StdEncoding.EncodeToString(ciphertext)
	return ciphertextV1Prefix + keyID(key) + ":" + enc, nil
}

// Decrypt decrypts ciphertext using AES-256-GCM
// Returns the original plaintext or an error if decryption/authentication fails
func Decrypt(ciphertextBase64 string) ([]byte, error) {
	keys := getKeyring()
	if len(keys) == 0 {
		return nil, ErrInvalidKey
	}

	// Support both legacy base64-only ciphertext and v1 key-tagged ciphertext.
	raw := strings.TrimSpace(ciphertextBase64)
	keyHint := ""
	if strings.HasPrefix(raw, ciphertextV1Prefix) {
		rest := strings.TrimPrefix(raw, ciphertextV1Prefix)
		parts := strings.SplitN(rest, ":", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
			return nil, ErrInvalidCiphertext
		}
		keyHint = strings.TrimSpace(parts[0])
		raw = parts[1]
	}

	// Decode from base64
	ciphertext, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, ErrInvalidCiphertext
	}

	// Try hinted key first (fast path), then fall back to all keys.
	ordered := make([][]byte, 0, len(keys))
	if keyHint != "" {
		for _, k := range keys {
			if keyID(k) == keyHint {
				ordered = append(ordered, k)
			}
		}
		for _, k := range keys {
			if keyID(k) != keyHint {
				ordered = append(ordered, k)
			}
		}
	} else {
		ordered = keys
	}

	for _, key := range ordered {
		block, err := aes.NewCipher(key)
		if err != nil {
			continue
		}
		gcm, err := cipher.NewGCM(block)
		if err != nil {
			continue
		}
		nonceSize := gcm.NonceSize()
		if len(ciphertext) < nonceSize {
			return nil, ErrInvalidCiphertext
		}
		nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
		plaintext, err := gcm.Open(nil, nonce, ct, nil)
		if err == nil {
			return plaintext, nil
		}
	}

	return nil, ErrInvalidCiphertext
}

// EncryptString encrypts a string and returns base64-encoded ciphertext
func EncryptString(plaintext string) (string, error) {
	return Encrypt([]byte(plaintext))
}

// DecryptString decrypts base64-encoded ciphertext and returns the original string
func DecryptString(ciphertext string) (string, error) {
	plaintext, err := Decrypt(ciphertext)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// RotateCiphertextIfNeeded re-encrypts legacy or non-primary ciphertext using the current primary key.
// It returns the (possibly updated) ciphertext, whether it was rotated, and an error if decryption fails.
func RotateCiphertextIfNeeded(ciphertext string) (string, bool, error) {
	keys := getKeyring()
	if len(keys) == 0 {
		return "", false, ErrInvalidKey
	}
	primaryID := keyID(keys[0])

	raw := strings.TrimSpace(ciphertext)
	if strings.HasPrefix(raw, ciphertextV1Prefix) {
		rest := strings.TrimPrefix(raw, ciphertextV1Prefix)
		parts := strings.SplitN(rest, ":", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == primaryID {
			// Already primary.
			return ciphertext, false, nil
		}
		// Else rotate (decrypt using any key, encrypt with primary).
	} else {
		// Legacy format: rotate.
	}

	plain, err := DecryptString(ciphertext)
	if err != nil {
		return "", false, err
	}
	enc, err := EncryptString(plain)
	if err != nil {
		return "", false, err
	}
	return enc, true, nil
}
