// Opaque challenge-session encoding for the local Cognito dev service.
//
// Per design §5b/§7: the InitiateAuth → RespondToAuthChallenge round-trip
// uses a server-side `challenge_sessions` row plus an opaque `Session`
// string the client carries through the second call. The opaque string is:
//
//   base64url( random32 || HMAC_SHA256(poolPrivateKeyPEM, random32) )
//
// Where:
//   - random32: 32 cryptographically-random bytes (the DB primary key)
//   - HMAC_SHA256: 32-byte tag using the pool's signing key PEM as HMAC key
//
// HMAC-prefix verification runs BEFORE the DB lookup so forged sessions are
// rejected without a query. The signing key bytes are a convenient
// per-pool secret already persisted by GO-COGNITO-1; they never leave the
// store, and HMAC verification is constant-time.
//
// The `random32` prefix is what we INSERT into the `challenge_sessions`
// table as the session id. Decoding peels the HMAC, verifies it with
// constant-time compare, and returns the prefix for caller-side DB lookup.
//
// Why HMAC and not direct random? Two reasons:
//   1. Forged-session early rejection — a random opaque blob from the
//      client can be discarded in nanoseconds without touching SQLite.
//   2. Token-class isolation — the HMAC is keyed on the pool's signing
//      key, so a session minted for pool A can't possibly authenticate
//      against pool B.
package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
)

// challengeRandomBytes is the random prefix length. 32 bytes = 256 bits of
// unguessable entropy, well above the AWS Cognito Session field's
// hard-to-pin lower bound (real Cognito sessions are ~250 chars base64,
// implying 150-180 bytes pre-encoding; 32 is plenty for replay-rejection).
const challengeRandomBytes = 32

// challengeHMACBytes is the SHA-256 tag length (32 bytes). The encoded
// session is therefore base64url-encoded over 64 raw bytes.
const challengeHMACBytes = sha256.Size

// EncodeChallengeSession returns:
//   - sessionID: base64url(random32 || hmac), used as wire `Session` and DB key
//   - randomPrefix: just the 32 random bytes — caller stores this as the row PK
//
// The HMAC is keyed on `privateKeyPEM` bytes (the per-pool signing key from
// the signing_keys table). The PEM bytes are stable for the lifetime of
// the pool (rotation is not implemented in dev), so sessions signed by one
// process can be verified by a restarted process loading the same DB.
func EncodeChallengeSession(privateKeyPEM []byte) (string, [challengeRandomBytes]byte, error) {
	var prefix [challengeRandomBytes]byte
	if len(privateKeyPEM) == 0 {
		return "", prefix, errors.New("empty privateKeyPEM")
	}
	if _, err := rand.Read(prefix[:]); err != nil {
		return "", prefix, fmt.Errorf("read random prefix: %w", err)
	}
	mac := hmac.New(sha256.New, privateKeyPEM)
	mac.Write(prefix[:])
	tag := mac.Sum(nil)

	buf := make([]byte, 0, challengeRandomBytes+challengeHMACBytes)
	buf = append(buf, prefix[:]...)
	buf = append(buf, tag...)
	return base64.RawURLEncoding.EncodeToString(buf), prefix, nil
}

// VerifyChallengeSession parses + verifies the HMAC on a wire `Session`
// string. On success returns the 32-byte random prefix the caller uses to
// look up the challenge_sessions row.
//
// Returns an error on:
//   - malformed base64
//   - wrong total length
//   - HMAC mismatch (constant-time compared via hmac.Equal)
//
// All callers map any error to NotAuthorizedException — the dev service
// MUST NOT leak which mode failed to clients (matches AWS behavior).
func VerifyChallengeSession(sessionID string, privateKeyPEM []byte) ([challengeRandomBytes]byte, error) {
	var prefix [challengeRandomBytes]byte
	if sessionID == "" {
		return prefix, errors.New("empty session id")
	}
	if len(privateKeyPEM) == 0 {
		return prefix, errors.New("empty privateKeyPEM")
	}
	raw, err := base64.RawURLEncoding.DecodeString(sessionID)
	if err != nil {
		return prefix, fmt.Errorf("decode session base64: %w", err)
	}
	if len(raw) != challengeRandomBytes+challengeHMACBytes {
		return prefix, fmt.Errorf("session has wrong length: got %d want %d",
			len(raw), challengeRandomBytes+challengeHMACBytes)
	}
	copy(prefix[:], raw[:challengeRandomBytes])
	gotTag := raw[challengeRandomBytes:]

	mac := hmac.New(sha256.New, privateKeyPEM)
	mac.Write(prefix[:])
	wantTag := mac.Sum(nil)

	// Constant-time compare — protects against timing attacks even though
	// this is a dev service.
	if !hmac.Equal(gotTag, wantTag) {
		return prefix, errors.New("session HMAC mismatch")
	}
	return prefix, nil
}

// challengeSessionDBKey returns the canonical DB key for a random prefix.
// We hex-encode the 32 random bytes so the `session` column is plain ASCII
// (easier to inspect with `sqlite3` CLI) without any padding ambiguity.
func challengeSessionDBKey(prefix [challengeRandomBytes]byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 2*challengeRandomBytes)
	for i, b := range prefix {
		out[2*i] = hex[b>>4]
		out[2*i+1] = hex[b&0x0f]
	}
	return string(out)
}
