// Package auth implements API-key authentication for the ingest and
// control-plane APIs.
//
// Key format:  ith_<12 hex>_<43 base64url>
//
//	ith_7f3a9c01b2de_Qm9vb...   (prefix = "ith_7f3a9c01b2de")
//
// The prefix is public: it is stored in plaintext, indexed, and used for the
// lookup. The whole key is hashed with SHA-256 and only the hash is stored. A
// key is 256 bits of randomness, not a human password, so there is no
// dictionary to attack and a fast hash is the right tool — bcrypt here would
// cost ~100ms per request and cap ingest at ~10 req/s per core.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

const (
	keyScheme    = "ith"
	prefixHexLen = 12
	secretBytes  = 32
)

// NewKey generates a fresh API key and returns the full key (shown to the
// user exactly once), its lookup prefix and its hash.
func NewKey() (key, prefix string, hash [32]byte) {
	var pid [prefixHexLen / 2]byte
	var secret [secretBytes]byte
	_, _ = rand.Read(pid[:]) // crypto/rand.Read never fails on supported platforms
	_, _ = rand.Read(secret[:])

	prefix = keyScheme + "_" + hex.EncodeToString(pid[:])
	key = prefix + "_" + base64.RawURLEncoding.EncodeToString(secret[:])
	return key, prefix, sha256.Sum256([]byte(key))
}

// SplitKey extracts the lookup prefix and checks the key's shape. Malformed
// keys are rejected before any database work, so garbage in an Authorization
// header costs nothing.
func SplitKey(key string) (prefix string, ok bool) {
	// ith _ <12 hex> _ <43 b64url>
	const want = len(keyScheme) + 1 + prefixHexLen + 1 + 43
	if len(key) != want || !strings.HasPrefix(key, keyScheme+"_") {
		return "", false
	}
	prefix = key[:len(keyScheme)+1+prefixHexLen]
	if key[len(prefix)] != '_' {
		return "", false
	}
	if _, err := hex.DecodeString(prefix[len(keyScheme)+1:]); err != nil {
		return "", false
	}
	if _, err := base64.RawURLEncoding.DecodeString(key[len(prefix)+1:]); err != nil {
		return "", false
	}
	return prefix, true
}

// Verify compares a presented key against a stored hash in constant time.
func Verify(key string, storedHash []byte) bool {
	sum := sha256.Sum256([]byte(key))
	return subtle.ConstantTimeCompare(sum[:], storedHash) == 1
}
