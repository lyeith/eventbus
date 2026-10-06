// Per-pool password policy fixtures and validation.
package cognito

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// PasswordPolicy is the per-pool configuration. Parsed from / serialised to
// JSON; nil pointer means "no policy → accept any non-empty password".
//
// Field semantics match Cognito's PasswordPolicyType
// (https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_PasswordPolicyType.html)
// and retains the snake_case fixture spelling for existing local stores.
type PasswordPolicy struct {
	MinLength                     int  `json:"min_length"        yaml:"min_length"`
	RequireUppercase              bool `json:"require_uppercase" yaml:"require_uppercase"`
	RequireLowercase              bool `json:"require_lowercase" yaml:"require_lowercase"`
	RequireDigits                 bool `json:"require_digits"    yaml:"require_digits"`
	TemporaryPasswordValidityDays int  `json:"temporary_password_validity_days" yaml:"temporary_password_validity_days"`
	RequireSymbols                bool `json:"require_symbols"   yaml:"require_symbols"`
}

// UnmarshalJSON accepts AWS PasswordPolicyType member names as well as the
// original fixture names. Canonical AWS fields take precedence when both exist.
func (p *PasswordPolicy) UnmarshalJSON(data []byte) error {
	type fixturePolicy PasswordPolicy
	var legacy fixturePolicy
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	var aws struct {
		MinimumLength                 *int
		RequireUppercase              *bool
		RequireLowercase              *bool
		RequireDigits                 *bool
		RequireNumbers                *bool
		RequireSymbols                *bool
		TemporaryPasswordValidityDays *int
	}
	if err := json.Unmarshal(data, &aws); err != nil {
		return err
	}
	*p = PasswordPolicy(legacy)
	if aws.MinimumLength != nil {
		p.MinLength = *aws.MinimumLength
	}
	if aws.RequireUppercase != nil {
		p.RequireUppercase = *aws.RequireUppercase
	}
	if aws.RequireLowercase != nil {
		p.RequireLowercase = *aws.RequireLowercase
	}
	if aws.RequireDigits != nil {
		p.RequireDigits = *aws.RequireDigits
	}
	if aws.RequireNumbers != nil {
		p.RequireDigits = *aws.RequireNumbers
	}
	if aws.RequireSymbols != nil {
		p.RequireSymbols = *aws.RequireSymbols
	}
	if aws.TemporaryPasswordValidityDays != nil {
		p.TemporaryPasswordValidityDays = *aws.TemporaryPasswordValidityDays
	}
	return nil
}

// MarshalJSON emits the AWS PasswordPolicyType contract. Existing fixture
// spellings remain accepted on input.
func (p PasswordPolicy) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		MinimumLength                 int
		RequireUppercase              bool
		RequireLowercase              bool
		RequireNumbers                bool
		RequireSymbols                bool
		TemporaryPasswordValidityDays int
	}{p.MinLength, p.RequireUppercase, p.RequireLowercase, p.RequireDigits, p.RequireSymbols, p.TemporaryPasswordValidityDays})
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
	if p.MinLength > 0 && utf8.RuneCountInString(password) < p.MinLength {
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
// Invalid persisted policy is an error; authentication never bypasses a policy
// because its fixture is malformed.
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
		return nil, fmt.Errorf("decode pool password policy: %w", jerr)
	}
	return &p, nil
}
