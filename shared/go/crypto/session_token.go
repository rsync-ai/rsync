package crypto

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashSessionToken maps a session bearer token onto the value stored in
// sessions.token. Callers hash on the way in and on the way out; the plaintext
// token exists only in the Set-Cookie header, the client, and the inbound
// Authorization header. It is never written to the database.
//
// Why this exists: the token used to be stored verbatim, and every lookup was an
// equality test against the plaintext. That makes the sessions table a list of
// working credentials rather than a list of references to them. Anything that can
// read one row — a backup, a managed-service snapshot, a read replica, a support
// query, a read-only SQL injection, or a dump carried between clouds — can replay
// it as the user until it expires. Hashing removes that: a stolen row is a
// preimage problem, not a login.
//
// Why SHA-256 and not bcrypt, which users.password_hash correctly uses: a
// password is low-entropy and human-chosen, so it needs a deliberately slow KDF
// to make guessing expensive. A session token here is 122 bits from crypto/rand
// (uuid.New), so it is not guessable at any work factor and a slow KDF buys
// nothing — while costing a bcrypt round on *every authenticated request*, which
// is a self-inflicted denial of service. A single SHA-256 is the standard
// treatment for high-entropy bearer tokens. No salt, deliberately: the lookup is
// by hash, so it must be deterministic, and a per-row salt would force a table
// scan. Salting defends against precomputation across a shared keyspace, which
// does not apply to 122 random bits.
//
// The output is 64 lowercase hex characters, which migration
// 094_sessions_token_hash.sql pins with a CHECK constraint — so a future code
// path that forgets to hash before writing fails loudly at the database instead
// of silently storing a live credential.
func HashSessionToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
