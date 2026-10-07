// Shared native attribute transitions, including contact verification ownership.
package cognito

import (
	"context"
	"database/sql"
)

func normalizeAttributeUpdates(updates map[string]string, config PoolSignInConfig) map[string]string {
	normalized := make(map[string]string, len(updates))
	for name, value := range updates {
		if name == "email" {
			value = normalizeSignIn(value, config)
		}
		normalized[name] = value
	}
	return normalized
}

// Contact replacement invalidates its old verification before schema validation.
// Permission checks still see caller-supplied updates, never generated flags.
func attributeValidationState(current, updates map[string]string) map[string]string {
	result := make(map[string]string, len(current))
	for name, value := range current {
		result[name] = value
	}
	for _, name := range []string{"email", "phone_number"} {
		if value, changed := updates[name]; changed && value != current[name] {
			result[name+"_verified"] = "false"
		}
	}
	return result
}

func mergeAttributeUpdates(current, updates map[string]string, administrator bool) map[string]string {
	merged := make(map[string]string, len(current)+len(updates))
	for name, value := range current {
		merged[name] = value
	}
	for name, value := range updates {
		if value == "" {
			delete(merged, name)
		} else {
			merged[name] = value
		}
	}
	for _, name := range []string{"email", "phone_number"} {
		value, changed := updates[name]
		if !changed || value == current[name] {
			continue
		}
		if value == "" {
			delete(merged, name+"_verified")
		} else {
			merged[name+"_verified"] = "false"
		}
		if explicit, ok := updates[name+"_verified"]; administrator && ok {
			merged[name+"_verified"] = explicit
		}
	}
	return merged
}

// persistAttributeChanges participates in the caller's identity transaction.
// It never commits credentials or status and never delivers a notification.
func (s *CognitoStore) persistAttributeChanges(ctx context.Context, tx *sql.Tx, sub string, current, merged map[string]string) (bool, error) {
	changed := false
	for name, value := range merged {
		if name == "sub" || current[name] == value {
			continue
		}
		changed = true
		if _, err := tx.ExecContext(ctx, `INSERT INTO user_attributes(sub,name,value)VALUES(?,?,?)ON CONFLICT(sub,name)DO UPDATE SET value=excluded.value`, sub, name, value); err != nil {
			return false, err
		}
	}
	for name := range current {
		if name == "sub" {
			continue
		}
		if _, exists := merged[name]; !exists {
			changed = true
			if _, err := tx.ExecContext(ctx, `DELETE FROM user_attributes WHERE sub=? AND name=?`, sub, name); err != nil {
				return false, err
			}
		}
	}
	for _, name := range []string{"email", "phone_number"} {
		if merged[name] != current[name] {
			if _, err := tx.ExecContext(ctx, `DELETE FROM verification_codes WHERE sub=? AND attribute_name=?`, sub, name); err != nil {
				return false, err
			}
		}
	}
	return changed, nil
}
