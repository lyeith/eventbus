// Tier-C polish helpers (GO-COGNITO-6):
//
//   - SECRET_HASH validation        (design §3k)
//   - Deterministic TOTP            (design §3e note)
//   - Configurable password policy  (design §3g)
//
// Each helper is independent and called from the relevant handler in
// cognito_handlers.go / cognito_management.go. They live here together so a
// future operator reading "what do we polish past Tier B?" finds one file
// rather than three.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"
)

// --- SECRET_HASH (§3k) -------------------------------------------------

// computeSecretHash matches the Go provider's exact computation:
//
//	Base64(HMAC_SHA256(client_secret, username + client_id))
//
// See go/libs/platform-lib/internal/auth/providers/cognito.go:1050-1057.
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

// --- TOTP (§3e note) ---------------------------------------------------

// validateTOTPCode validates a 6-digit TOTP code against a base32-encoded
// secret. Uses pquerna/otp's TOTP validator with skew=1 (i.e. accepts the
// previous, current, and next 30-second windows) so a clock drift of a few
// seconds either side doesn't fail a legitimate code. Returns nil on
// success, an error on mismatch.
//
// If `secret` is empty, returns errFallbackToAnyDigits — caller should
// degrade to the "any 6 digits" path for the cheap dev case where no
// totp_secret is enrolled.
func validateTOTPCode(secret, code string) error {
	if secret == "" {
		return errFallbackToAnyDigits
	}
	ok, err := totp.ValidateCustom(code, secret, time.Now(), totp.ValidateOpts{
		Period:    30,
		Skew:      1,
		Digits:    otp.DigitsSix,
		Algorithm: otp.AlgorithmSHA1,
	})
	if err != nil {
		// totp.ValidateCustom returns an error on malformed base32 / etc.
		// Map all of these to "code mismatch" so we never leak which
		// failure mode hit.
		return fmt.Errorf("totp validate: %w", err)
	}
	if !ok {
		return errors.New("totp code mismatch")
	}
	return nil
}

// errFallbackToAnyDigits is the sentinel returned by validateTOTPCode when
// the user has no TOTP secret enrolled. The handler then falls back to the
// "any 6 digits" path that's been the default since GO-COGNITO-5.
var errFallbackToAnyDigits = errors.New("no totp secret; fall back to any-6-digit acceptance")

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

// --- Password policy (§3g) --------------------------------------------

// PasswordPolicy is the per-pool configuration. Parsed from / serialised to
// JSON; nil pointer means "no policy → accept any non-empty password".
//
// Field semantics match Cognito's PasswordPolicyType
// (https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_PasswordPolicyType.html)
// minus TemporaryPasswordValidityDays, which is irrelevant in dev.
type PasswordPolicy struct {
	MinLength        int  `json:"min_length"        yaml:"min_length"`
	RequireUppercase bool `json:"require_uppercase" yaml:"require_uppercase"`
	RequireLowercase bool `json:"require_lowercase" yaml:"require_lowercase"`
	RequireDigits    bool `json:"require_digits"    yaml:"require_digits"`
	RequireSymbols   bool `json:"require_symbols"   yaml:"require_symbols"`
}

// Validate applies the policy to `password` and returns nil on success or
// an error whose message names every failed rule. Empty password is always
// rejected ("password is required"); a nil policy still requires non-empty.
//
// Multiple failures are listed in a single message, matching Cognito's
// behaviour and giving the operator one round-trip to fix everything.
func (p *PasswordPolicy) Validate(password string) error {
	var failures []string
	if password == "" {
		return errors.New("password is required")
	}
	if p == nil {
		return nil
	}
	if p.MinLength > 0 && len(password) < p.MinLength {
		failures = append(failures, fmt.Sprintf("be at least %d characters", p.MinLength))
	}
	hasUpper, hasLower, hasDigit, hasSymbol := false, false, false, false
	for _, r := range password {
		switch {
		case unicode.IsUpper(r):
			hasUpper = true
		case unicode.IsLower(r):
			hasLower = true
		case unicode.IsDigit(r):
			hasDigit = true
		case unicode.IsPunct(r) || unicode.IsSymbol(r):
			hasSymbol = true
		}
	}
	if p.RequireUppercase && !hasUpper {
		failures = append(failures, "contain an uppercase letter")
	}
	if p.RequireLowercase && !hasLower {
		failures = append(failures, "contain a lowercase letter")
	}
	if p.RequireDigits && !hasDigit {
		failures = append(failures, "contain a digit")
	}
	if p.RequireSymbols && !hasSymbol {
		failures = append(failures, "contain a symbol")
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("password must %s", strings.Join(failures, ", "))
}

// loadPoolPasswordPolicy fetches and decodes the pool's password policy.
// Returns (nil, nil) when no policy is configured (the common case).
//
// JSON parse errors are swallowed at this layer — we log nothing because
// callers can't do anything useful with a malformed seed. The handler
// treats "couldn't parse" as "no policy", matching the existing behaviour
// where a missing column means accept-anything.
func loadPoolPasswordPolicy(ctx context.Context, store *CognitoStore, poolID string) (*PasswordPolicy, error) {
	raw, err := store.GetPoolPasswordPolicy(ctx, poolID)
	if err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var p PasswordPolicy
	if jerr := json.Unmarshal([]byte(raw), &p); jerr != nil {
		// Stored JSON is corrupt — fall through to "no policy" rather than
		// fail the whole AdminCreateUser. The dev DB is throwaway; an
		// operator who hand-edits this column gets what they deserve.
		return nil, nil
	}
	return &p, nil
}
