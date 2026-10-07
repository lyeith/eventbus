// Native app-client attribute rights. Administrative and trigger views bypass
// these projections; only client-facing user reads, writes and ID claims use them.
package cognito

import "sort"

var profileAttributeNames = []string{
	"name", "family_name", "given_name", "middle_name", "nickname",
	"preferred_username", "profile", "picture", "website", "gender",
	"birthdate", "zoneinfo", "locale",
}

// NormalizeClientAttributes validates a persisted ReadAttributes or
// WriteAttributes setting. nil preserves the omitted native setting, while an
// explicit empty list grants no optional rights. oidc:profile remains a native
// setting in readback and expands when permissions are evaluated.
func NormalizeClientAttributes(schema []SchemaAttribute, requested []string, write bool) ([]string, error) {
	index, err := normalizedSchemaIndex(schema)
	if err != nil {
		return nil, err
	}
	if requested == nil {
		return nil, nil
	}
	result := make([]string, 0, len(requested))
	seen := make(map[string]bool, len(requested))
	for _, name := range requested {
		if name != "oidc:profile" {
			attribute, exists := index[name]
			if !exists {
				return nil, invalidAttribute("Attribute %q is not defined in the user pool schema", name)
			}
			if write && (name == "sub" || name == "email_verified" || name == "phone_number_verified" || *attribute.DeveloperOnlyAttribute) {
				return nil, invalidAttribute("Attribute %s cannot be granted app-client write access", name)
			}
		}
		if !seen[name] {
			result = append(result, name)
			seen[name] = true
		}
	}
	return result, nil
}

func clientAttributeRights(index map[string]SchemaAttribute, requested []string, write bool) map[string]bool {
	rights := make(map[string]bool)
	if requested == nil {
		// The CreateUserPoolClient operation defines omitted permissions as
		// standard attributes; custom/dev attributes require explicit access.
		for name := range index {
			if isStandardAttribute(name) && (!write || (name != "sub" && name != "email_verified" && name != "phone_number_verified")) {
				rights[name] = true
			}
		}
	} else {
		for _, name := range requested {
			if name == "oidc:profile" {
				for _, profile := range profileAttributeNames {
					rights[profile] = true
				}
			} else {
				rights[name] = true
			}
		}
	}
	if write {
		// Required user values must remain writable for registration and
		// completing NEW_PASSWORD_REQUIRED, regardless of optional rights.
		for name, attribute := range index {
			if name != "sub" && *attribute.Required && !*attribute.DeveloperOnlyAttribute {
				rights[name] = true
			}
		}
	}
	return rights
}

// FilterClientReadAttributes returns a fresh readable attribute map. The
// system-owned sub identity is always available. Unknown stored attributes and
// protocol claims cannot become token claims through an attribute projection.
func FilterClientReadAttributes(schema []SchemaAttribute, requested []string, attributes map[string]string) map[string]string {
	result := make(map[string]string)
	index, err := normalizedSchemaIndex(schema)
	if err != nil {
		return result // Invalid configuration fails closed; admit it at creation.
	}
	rights := clientAttributeRights(index, requested, false)
	for name, value := range attributes {
		if _, exists := index[name]; !exists {
			continue
		}
		if name == "sub" || (rights[name] && !IsReservedTokenClaim(name)) {
			result[name] = value
		}
	}
	return result
}

// ValidateClientWriteAttributes checks permission only. Call
// ValidateUserAttributes separately for type, mutability and required values.
// Admin APIs and trigger payloads don't call this client-specific admission.
func ValidateClientWriteAttributes(schema []SchemaAttribute, requested []string, updates map[string]string) error {
	index, err := normalizedSchemaIndex(schema)
	if err != nil {
		return err
	}
	rights := clientAttributeRights(index, requested, true)
	names := make([]string, 0, len(updates))
	for name := range updates {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		attribute, exists := index[name]
		if !exists {
			return invalidAttribute("Attribute %q is not defined in the user pool schema", name)
		}
		if name == "sub" || name == "email_verified" || name == "phone_number_verified" || *attribute.DeveloperOnlyAttribute || !rights[name] {
			return deniedAttribute("App client is not authorized to write attribute %s", name)
		}
	}
	return nil
}

// IsReservedTokenClaim identifies signed token protocol claims. They are
// populated by token issuance and must never be overwritten by user attributes.
func IsReservedTokenClaim(name string) bool {
	switch name {
	case "iss", "sub", "aud", "exp", "iat", "nbf", "auth_time", "token_use", "client_id",
		"jti", "origin_jti", "nonce", "at_hash", "c_hash", "scope", "event_id", "username",
		"cognito:username", "cognito:groups", "cognito:roles", "cognito:preferred_role":
		return true
	default:
		return false
	}
}
