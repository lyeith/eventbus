// Per-pool password policy fixtures and validation.
package cognito

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

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
		// Preserve the emulator's existing permissive fallback for malformed fixtures.
		return nil, nil
	}
	return &p, nil
}
