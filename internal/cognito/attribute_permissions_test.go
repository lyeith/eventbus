package cognito

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func permissionTestSchema(t *testing.T) []SchemaAttribute {
	t.Helper()
	schema, err := NormalizeSchema([]SchemaAttribute{
		{Name: "email", Required: schemaFlag(true)},
		{Name: "tier", Mutable: schemaFlag(true)},
		{Name: "audit", DeveloperOnlyAttribute: schemaFlag(true), Mutable: schemaFlag(true)},
		{Name: "exp", Mutable: schemaFlag(true)},
	})
	require.NoError(t, err)
	return schema
}

func TestNormalizeClientAttributesPreservesNativeSettings(t *testing.T) {
	schema := permissionTestSchema(t)
	omitted, err := NormalizeClientAttributes(schema, nil, false)
	require.NoError(t, err)
	assert.Nil(t, omitted)
	empty, err := NormalizeClientAttributes(schema, []string{}, false)
	require.NoError(t, err)
	assert.NotNil(t, empty)
	assert.Empty(t, empty)
	input := []string{"oidc:profile", "email", "custom:tier", "dev:audit", "email"}
	permissions, err := NormalizeClientAttributes(schema, input, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"oidc:profile", "email", "custom:tier", "dev:audit"}, permissions)
	input[0] = "custom:exp"
	assert.Equal(t, "oidc:profile", permissions[0])
	writes, err := NormalizeClientAttributes(schema, []string{"oidc:profile", "custom:tier"}, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"oidc:profile", "custom:tier"}, writes)
	for _, name := range []string{"missing", "tier", "oidc:email"} {
		_, err := NormalizeClientAttributes(schema, []string{name}, false)
		assertAttributeError(t, err, "InvalidParameterException")
	}
	for _, name := range []string{"sub", "email_verified", "phone_number_verified", "dev:audit"} {
		_, err := NormalizeClientAttributes(schema, []string{name}, true)
		assertAttributeError(t, err, "InvalidParameterException")
	}
}

func TestFilterClientReadAttributesDefaultsAndRestrictions(t *testing.T) {
	schema := permissionTestSchema(t)
	attributes := map[string]string{
		"sub": "identity", "email": "ada@example.com", "email_verified": "true",
		"phone_number_verified": "false", "given_name": "Ada", "address": "Somewhere",
		"updated_at": "123", "custom:tier": "paid", "dev:audit": "internal",
		"exp": "0", "cognito:groups": "administrators", "custom:exp": "application-value",
		"custom:missing": "not-in-schema",
	}
	defaults := FilterClientReadAttributes(schema, nil, attributes)
	assert.Equal(t, map[string]string{
		"sub": "identity", "email": "ada@example.com", "email_verified": "true",
		"phone_number_verified": "false", "given_name": "Ada", "address": "Somewhere", "updated_at": "123",
	}, defaults)
	restricted := FilterClientReadAttributes(schema, []string{"custom:tier", "dev:audit", "custom:exp"}, attributes)
	assert.Equal(t, map[string]string{
		"sub": "identity", "custom:tier": "paid", "dev:audit": "internal", "custom:exp": "application-value",
	}, restricted)
	assert.Equal(t, map[string]string{"sub": "identity"}, FilterClientReadAttributes(schema, []string{}, attributes))
	restricted["custom:tier"] = "changed"
	assert.Equal(t, "paid", attributes["custom:tier"])
}

func TestOIDCProfileExpansionExcludesOtherScopes(t *testing.T) {
	schema := permissionTestSchema(t)
	attributes := map[string]string{
		"sub": "identity", "given_name": "Ada", "birthdate": "2000-01-01", "locale": "en",
		"email": "ada@example.com", "phone_number": "+12065550100", "address": "Somewhere",
		"updated_at": "123", "custom:tier": "paid",
	}
	read := FilterClientReadAttributes(schema, []string{"oidc:profile"}, attributes)
	assert.Equal(t, map[string]string{"sub": "identity", "given_name": "Ada", "birthdate": "2000-01-01", "locale": "en"}, read)
	require.NoError(t, ValidateClientWriteAttributes(schema, []string{"oidc:profile"}, map[string]string{"given_name": "Grace", "locale": "en-GB"}))
	for _, name := range []string{"phone_number", "address", "updated_at", "custom:tier"} {
		assertAttributeError(t, ValidateClientWriteAttributes(schema, []string{"oidc:profile"}, map[string]string{name: "value"}), "NotAuthorizedException")
	}
}

func TestValidateClientWriteAttributesRequiredExceptionAndAdminBoundary(t *testing.T) {
	schema := permissionTestSchema(t)
	// Required email is writable even when the app client has only custom
	// rights or an explicitly empty write list. Optional attributes are not.
	require.NoError(t, ValidateClientWriteAttributes(schema, []string{}, map[string]string{"email": "ada@example.com"}))
	require.NoError(t, ValidateClientWriteAttributes(schema, []string{"custom:tier"}, map[string]string{"email": "ada@example.com", "custom:tier": "paid"}))
	require.NoError(t, ValidateClientWriteAttributes(schema, nil, map[string]string{"given_name": "Ada"}))
	assertAttributeError(t, ValidateClientWriteAttributes(schema, nil, map[string]string{"custom:tier": "paid"}), "NotAuthorizedException")
	assertAttributeError(t, ValidateClientWriteAttributes(schema, []string{}, map[string]string{"given_name": "Ada"}), "NotAuthorizedException")
	for _, name := range []string{"sub", "email_verified", "phone_number_verified", "dev:audit"} {
		// A malformed stored permission cannot grant the app these rights.
		assertAttributeError(t, ValidateClientWriteAttributes(schema, []string{name}, map[string]string{name: "true"}), "NotAuthorizedException")
	}
	assertAttributeError(t, ValidateClientWriteAttributes(schema, []string{"missing"}, map[string]string{"missing": "value"}), "InvalidParameterException")
	// Native admin admission bypasses client rights while retaining schema
	// validation. Read projections also leave the administrator's map intact.
	admin := map[string]string{"email": "ada@example.com", "email_verified": "true", "dev:audit": "internal"}
	require.NoError(t, ValidateUserAttributes(schema, nil, admin, false, true, true))
	read := FilterClientReadAttributes(schema, []string{}, admin)
	assert.Empty(t, read)
	assert.Equal(t, "internal", admin["dev:audit"])
}

func TestReservedTokenClaimsStayOwnedByIssuance(t *testing.T) {
	for _, name := range []string{"iss", "sub", "aud", "exp", "iat", "nbf", "auth_time", "token_use", "client_id", "jti", "origin_jti", "nonce", "at_hash", "c_hash", "scope", "event_id", "username", "cognito:username", "cognito:groups", "cognito:roles", "cognito:preferred_role"} {
		assert.True(t, IsReservedTokenClaim(name), "%s", name)
	}
	for _, name := range []string{"email", "given_name", "custom:exp", "dev:aud"} {
		assert.False(t, IsReservedTokenClaim(name), "%s", name)
	}
	// Invalid persisted schemas fail closed and never project unchecked data.
	assert.Empty(t, FilterClientReadAttributes([]SchemaAttribute{{Name: "invalid name"}}, nil, map[string]string{"email": "ada@example.com"}))
}
