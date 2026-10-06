// Cognito app-client secret hashing and refresh identity extraction.
package cognito

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// computeSecretHash matches the Go provider's exact computation:
//
//	Base64(HMAC_SHA256(client_secret, username + client_id))
//
// `username` is exactly what the caller put in the AuthParameters; we do
// NOT lowercase, trim, or otherwise normalise — neither does the provider.
func computeSecretHash(clientSecret, username, clientID string) string {
	mac := hmac.New(sha256.New, []byte(clientSecret))
	mac.Write([]byte(username + clientID))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// verifySecretHash returns nil iff the client has no secret OR `provided`
// equals the expected hash. Constant-time compare via hmac.Equal protects
// against timing attacks even though this is a dev service.
//
// On mismatch, callers map to NotAuthorizedException (matches what the Go
// provider expects when the platform forwards a hash mismatch — the typed
// error decoder maps the code, not the message).
func verifySecretHash(clientSecret, username, clientID, provided string) error {
	if clientSecret == "" {
		// No secret configured → SECRET_HASH is silently ignored. Real
		// Cognito does the same when ClientSecret is unset. We keep this
		// permissive so tests written against secret-less clients aren't
		// forced to thread a hash through every call.
		return nil
	}
	if provided == "" {
		return errors.New("SECRET_HASH required when client has secret")
	}
	expected := computeSecretHash(clientSecret, username, clientID)
	if !hmac.Equal([]byte(expected), []byte(provided)) {
		return errors.New("SECRET_HASH mismatch")
	}
	return nil
}

// refreshTokenSubUnverified parses the JWT WITHOUT verifying its signature
// to pull out the `sub` claim. Used by REFRESH_TOKEN_AUTH SECRET_HASH
// enforcement, which has to compute the expected hash from the token's
// embedded user id BEFORE verifying the token (chicken-and-egg). Caller
// MUST still call VerifyRefreshToken afterwards — this is just a peek.
func refreshTokenSubUnverified(refreshToken string) (string, error) {
	// JWT layout: header.payload.signature, all base64url, '.' separated.
	parts := strings.Split(refreshToken, ".")
	if len(parts) != 3 {
		return "", errors.New("malformed jwt")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("decode jwt payload: %w", err)
	}
	var claims map[string]interface{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("parse jwt payload: %w", err)
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", errors.New("jwt missing sub")
	}
	return sub, nil
}
