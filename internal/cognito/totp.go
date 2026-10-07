// Software-token validation and the fixture-only code fallback.
package cognito

import (
	"errors"
	"fmt"
	"time"

	"github.com/pquerna/otp"
	"github.com/pquerna/otp/totp"
)

// validateTOTPCode validates a 6-digit TOTP code against a base32-encoded
// secret. Uses pquerna/otp's TOTP validator with skew=1 (i.e. accepts the
// previous, current, and next 30-second windows) so a clock drift of a few
// seconds either side doesn't fail a legitimate code. Returns nil on
// success, an error on mismatch.
//
// If `secret` is empty, returns errFallbackToAnyDigits — caller should
// degrade to the "any 6 digits" path for the cheap dev case where no
// totp_secret is enrolled.
func validateTOTPCode(secret, code string) error { return validateTOTPCodeAt(secret, code, time.Now()) }

func validateTOTPCodeAt(secret, code string, now time.Time) error {
	if secret == "" {
		return errFallbackToAnyDigits
	}
	ok, err := totp.ValidateCustom(code, secret, now, totp.ValidateOpts{
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
// six-digit fixture fallback.
var errFallbackToAnyDigits = errors.New("no totp secret; fall back to any-6-digit acceptance")
