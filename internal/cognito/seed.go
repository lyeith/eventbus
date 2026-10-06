// YAML fixtures bootstrap deterministic pools, app clients and users.
// Seeding is idempotent; passwords become bcrypt hashes and SRP verifiers.
package cognito

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// CognitoSeedFile is the on-disk YAML schema. See examples/cognito_pools.yaml
// for a runnable example. Keep this file roughly in sync with consumers.yaml's
// approachable, comment-friendly style.
type CognitoSeedFile struct {
	Pools []CognitoSeedPool `yaml:"pools"`
}

type CognitoSeedPool struct {
	ID             string              `yaml:"id"`
	Region         string              `yaml:"region"`
	SignIn         *CognitoSeedSignIn  `yaml:"sign_in"`
	PasswordPolicy *PasswordPolicy     `yaml:"password_policy"`
	Clients        []CognitoSeedClient `yaml:"clients"`
	Users          []CognitoSeedUser   `yaml:"users"`
}

// CognitoSeedSignIn makes fixture sign-in rules explicit; omitted case sensitivity
// defaults to true, matching pools created by the AWS API.
type CognitoSeedSignIn struct {
	EmailAsUsername bool  `yaml:"email_as_username"`
	EmailAlias      bool  `yaml:"email_alias"`
	CaseSensitive   *bool `yaml:"case_sensitive"`
}

type CognitoSeedClient struct {
	ID     string `yaml:"id"`
	Secret string `yaml:"secret"`
}

// CognitoSeedUser carries an optional `totp_secret` (base32) — when set,
// the dev service validates SOFTWARE_TOKEN_MFA codes against it via
// pquerna/otp instead of the "any 6 digits" fallback. Useful for tests
// that exercise real TOTP behaviour deterministically.
type CognitoSeedUser struct {
	Username   string            `yaml:"username"`
	Enabled    *bool             `yaml:"enabled"`
	Email      string            `yaml:"email"`
	Password   string            `yaml:"password"`
	MFAEnabled bool              `yaml:"mfa_enabled"`
	TOTPSecret string            `yaml:"totp_secret"`
	Attributes map[string]string `yaml:"attributes"`
}

// LoadCognitoSeed reads + parses the YAML seed file at path. It does NOT
// touch the database; call ApplyCognitoSeed for that.
func LoadCognitoSeed(path string) (*CognitoSeedFile, error) {
	if path == "" {
		return nil, errors.New("seed path required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cognito seed %q: %w", path, err)
	}
	var f CognitoSeedFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse cognito seed %q: %w", path, err)
	}
	for i, p := range f.Pools {
		if p.ID == "" {
			return nil, fmt.Errorf("pool %d: id is required", i)
		}
		if p.Region == "" {
			f.Pools[i].Region = "us-east-1"
		}
		for j, u := range p.Users {
			if u.Email == "" && u.Username == "" {
				return nil, fmt.Errorf("pool %q user %d: email is required when username is omitted", p.ID, j)
			}
			if u.Password == "" {
				return nil, fmt.Errorf("pool %q user %q: password is required", p.ID, u.Email)
			}
		}
	}
	return &f, nil
}

// ApplyCognitoSeed writes the loaded seed into the store. Safe to invoke on
// every process start. Existing identities and unchanged lifecycle state are
// preserved; changed fixture passwords replace both bcrypt and SRP credentials.
func ApplyCognitoSeed(ctx context.Context, store *CognitoStore, seed *CognitoSeedFile) error {
	if store == nil {
		return errors.New("nil cognito store")
	}
	if seed == nil {
		return nil
	}
	for _, pool := range seed.Pools {
		if err := store.UpsertPool(ctx, pool.ID, pool.Region); err != nil {
			return fmt.Errorf("upsert pool %q: %w", pool.ID, err)
		}
		if pool.SignIn != nil {
			configuration := PoolSignInConfig{EmailAsUsername: pool.SignIn.EmailAsUsername, EmailAlias: pool.SignIn.EmailAlias, CaseSensitive: true}
			if pool.SignIn.CaseSensitive != nil {
				configuration.CaseSensitive = *pool.SignIn.CaseSensitive
			}
			if err := store.SetPoolSignInConfig(ctx, pool.ID, configuration); err != nil {
				return fmt.Errorf("set sign_in for pool %q: %w", pool.ID, err)
			}
		}
		// Per-pool password policy . JSON-encode for
		// storage; the password_policy.go loader decodes on demand.
		if pool.PasswordPolicy != nil {
			raw, err := json.Marshal(pool.PasswordPolicy)
			if err != nil {
				return fmt.Errorf("marshal password_policy for pool %q: %w", pool.ID, err)
			}
			if err := store.SetPoolPasswordPolicy(ctx, pool.ID, string(raw)); err != nil {
				return fmt.Errorf("set password_policy for pool %q: %w", pool.ID, err)
			}
		}
		// Eagerly create the signing key so the JWKS endpoint is ready
		// without waiting for the first sign call.
		if _, err := store.EnsureSigningKey(ctx, pool.ID); err != nil {
			return fmt.Errorf("ensure signing key for pool %q: %w", pool.ID, err)
		}
		for _, c := range pool.Clients {
			if c.ID == "" {
				return fmt.Errorf("pool %q: client id is required", pool.ID)
			}
			if err := store.UpsertClient(ctx, c.ID, pool.ID, c.Secret); err != nil {
				return fmt.Errorf("upsert client %q: %w", c.ID, err)
			}
		}
		for _, u := range pool.Users {
			sub, err := store.UpsertSeedUser(ctx, pool.ID, u.Username, u.Email, u.Password, u.MFAEnabled)
			if err != nil {
				return fmt.Errorf("upsert user %q: %w", u.Email, err)
			}
			if u.Enabled != nil {
				if err := store.SetUserEnabled(ctx, sub, *u.Enabled); err != nil {
					return fmt.Errorf("set enabled for %q: %w", u.Email, err)
				}
			}
			// Per-user TOTP secret . Empty secret is the
			// default; non-empty enables deterministic TOTP validation.
			if u.TOTPSecret != "" {
				if err := store.SetUserTOTPSecret(ctx, sub, u.TOTPSecret); err != nil {
					return fmt.Errorf("set totp_secret for %q: %w", u.Email, err)
				}
			}
			// Legacy email fixtures remain verified by default. Explicit fixture
			// attributes can override verification without changing the username.
			if u.Email != "" {
				if err := store.SetUserAttribute(ctx, sub, "email", u.Email); err != nil {
					return fmt.Errorf("set email attr for %q: %w", u.Email, err)
				}
				verified := "true"
				if value, exists := u.Attributes["email_verified"]; exists {
					verified = value
				}
				if err := store.SetUserAttribute(ctx, sub, "email_verified", verified); err != nil {
					return fmt.Errorf("set email_verified for %q: %w", u.Email, err)
				}
			}
			for k, v := range u.Attributes {
				if err := store.SetUserAttribute(ctx, sub, k, v); err != nil {
					return fmt.Errorf("set attr %q for %q: %w", k, u.Email, err)
				}
			}
		}
	}
	return nil
}
