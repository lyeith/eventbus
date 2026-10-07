package cognito

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertAttributeError(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)
	var native *AttributeValidationError
	require.True(t, errors.As(err, &native), "expected typed attribute validation error, got %T", err)
	assert.Equal(t, code, native.Code)
	assert.NotEmpty(t, native.Message)
}

func TestNormalizeSchemaNativeDefaultsAndCanonicalCustomNames(t *testing.T) {
	input := []SchemaAttribute{
		{Name: "email", Required: schemaFlag(true)},
		{Name: "membership", StringAttributeConstraints: &StringAttributeConstraints{MinLength: "2", MaxLength: "30"}},
		{Name: "internal", DeveloperOnlyAttribute: schemaFlag(true), Mutable: schemaFlag(true), AttributeDataType: "Number"},
		{Name: "custom:enabled", AttributeDataType: "Boolean", Mutable: schemaFlag(true)},
	}
	schema, err := NormalizeSchema(input)
	require.NoError(t, err)
	require.Len(t, schema, 23)
	index, err := normalizedSchemaIndex(schema)
	require.NoError(t, err)
	assert.True(t, *index["sub"].Required)
	assert.False(t, *index["sub"].Mutable)
	assert.True(t, *index["email"].Required)
	assert.True(t, *index["email"].Mutable)
	assert.False(t, *index["custom:membership"].Mutable)
	assert.Equal(t, "String", index["custom:membership"].AttributeDataType)
	assert.True(t, *index["dev:internal"].DeveloperOnlyAttribute)
	assert.Equal(t, "Boolean", index["phone_number_verified"].AttributeDataType)
	assert.Equal(t, "Number", index["updated_at"].AttributeDataType)
	assert.Equal(t, "0", index["updated_at"].NumberAttributeConstraints.MinValue)
	assert.Equal(t, "10", index["birthdate"].StringAttributeConstraints.MinLength)

	// Pool configuration is a snapshot; neither flags nor constraints alias
	// the creation request, and Describe values normalize without drift.
	*input[0].Required = false
	input[1].StringAttributeConstraints.MaxLength = "1"
	assert.True(t, *index["email"].Required)
	assert.Equal(t, "30", index["custom:membership"].StringAttributeConstraints.MaxLength)
	again, err := NormalizeSchema(schema)
	require.NoError(t, err)
	assert.Equal(t, schema, again)
	*again[0].Required = false
	assert.True(t, *schema[0].Required)
}

func TestNormalizeSchemaRefusesInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name  string
		input []SchemaAttribute
	}{
		{"empty name", []SchemaAttribute{{Name: ""}}},
		{"name length", []SchemaAttribute{{Name: strings.Repeat("a", 21)}}},
		{"name whitespace", []SchemaAttribute{{Name: "billing tier"}}},
		{"duplicate canonical name", []SchemaAttribute{{Name: "tier"}, {Name: "custom:tier"}}},
		{"required custom", []SchemaAttribute{{Name: "tier", Required: schemaFlag(true)}}},
		{"mutable sub", []SchemaAttribute{{Name: "sub", Mutable: schemaFlag(true)}}},
		{"optional sub", []SchemaAttribute{{Name: "sub", Required: schemaFlag(false)}}},
		{"sub type", []SchemaAttribute{{Name: "sub", AttributeDataType: "Number"}}},
		{"required verification", []SchemaAttribute{{Name: "email_verified", Required: schemaFlag(true)}}},
		{"developer prefix conflict", []SchemaAttribute{{Name: "dev:tier", DeveloperOnlyAttribute: schemaFlag(false)}}},
		{"custom prefix conflict", []SchemaAttribute{{Name: "custom:tier", DeveloperOnlyAttribute: schemaFlag(true)}}},
		{"unknown type", []SchemaAttribute{{Name: "tier", AttributeDataType: "Integer"}}},
		{"wrong constraint type", []SchemaAttribute{{Name: "tier", AttributeDataType: "Number", StringAttributeConstraints: &StringAttributeConstraints{MinLength: "1"}}}},
		{"boolean constraints", []SchemaAttribute{{Name: "flag", AttributeDataType: "Boolean", NumberAttributeConstraints: &NumberAttributeConstraints{MinValue: "0"}}}},
		{"negative length", []SchemaAttribute{{Name: "tier", StringAttributeConstraints: &StringAttributeConstraints{MinLength: "-1"}}}},
		{"maximum above limit", []SchemaAttribute{{Name: "tier", StringAttributeConstraints: &StringAttributeConstraints{MaxLength: "2049"}}}},
		{"length order", []SchemaAttribute{{Name: "tier", StringAttributeConstraints: &StringAttributeConstraints{MinLength: "5", MaxLength: "4"}}}},
		{"numeric order", []SchemaAttribute{{Name: "tier", AttributeDataType: "Number", NumberAttributeConstraints: &NumberAttributeConstraints{MinValue: "10", MaxValue: "9.9"}}}},
		{"nonfinite bound", []SchemaAttribute{{Name: "tier", AttributeDataType: "Number", NumberAttributeConstraints: &NumberAttributeConstraints{MaxValue: "Inf"}}}},
		{"numeric magnitude", []SchemaAttribute{{Name: "tier", AttributeDataType: "Number", NumberAttributeConstraints: &NumberAttributeConstraints{MaxValue: "1e309"}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := NormalizeSchema(test.input)
			assertAttributeError(t, err, "InvalidParameterException")
		})
	}
}

func TestNormalizeSchemaCustomAttributeQuotaAndUnicodeNames(t *testing.T) {
	input := make([]SchemaAttribute, 0, 51)
	for index := range 50 {
		input = append(input, SchemaAttribute{Name: "attr" + strings.Repeat("x", index/10) + string(rune('A'+index%10))})
	}
	// The generated suffixes remain unique across every group of ten.
	schema, err := NormalizeSchema(input)
	require.NoError(t, err)
	assert.Len(t, schema, 70)
	_, err = NormalizeSchema(append(input, SchemaAttribute{Name: "fiftyfirst"}))
	assertAttributeError(t, err, "InvalidParameterException")
	unicodeSchema, err := NormalizeSchema([]SchemaAttribute{{Name: "職種"}})
	require.NoError(t, err)
	assert.Equal(t, "custom:職種", unicodeSchema[len(unicodeSchema)-1].Name)
}

func TestValidateUserAttributesRequiredAndAdministratorRules(t *testing.T) {
	schema, err := NormalizeSchema([]SchemaAttribute{
		{Name: "email", Required: schemaFlag(true)},
		{Name: "given_name", Required: schemaFlag(true)},
		{Name: "role", Mutable: schemaFlag(true)},
		{Name: "billing", DeveloperOnlyAttribute: schemaFlag(true), Mutable: schemaFlag(true)},
	})
	require.NoError(t, err)
	// Administrators may create an incomplete user, but registration and the
	// later initial-password flow require the omitted values.
	require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:role": "reader"}, false, true, true))
	assert.Equal(t, []string{"email", "given_name"}, MissingRequiredAttributes(schema, nil))
	assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"email": "user@example.com"}, true, true, false), "InvalidParameterException")
	current := map[string]string{"email": "user@example.com"}
	updates := map[string]string{"given_name": "Ada"}
	require.NoError(t, ValidateUserAttributes(schema, current, updates, true, false, false))
	assert.Equal(t, map[string]string{"email": "user@example.com"}, current)
	assert.Equal(t, map[string]string{"given_name": "Ada"}, updates)

	assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"sub": "caller-controlled"}, false, true, true), "InvalidParameterException")
	assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"role": "admin"}, false, true, true), "InvalidParameterException")
	assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"dev:billing": "paid"}, false, true, false), "NotAuthorizedException")
	require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"dev:billing": "paid"}, false, true, true))
	assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"email_verified": "true"}, false, true, false), "NotAuthorizedException")
	assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"email_verified": "true"}, false, true, true), "InvalidParameterException")
	require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"email": "user@example.com", "email_verified": "true"}, false, true, true))
}

func TestValidateUserAttributesImmutableAfterCreation(t *testing.T) {
	schema, err := NormalizeSchema([]SchemaAttribute{{Name: "customer_id"}, {Name: "notes", Mutable: schemaFlag(true)}})
	require.NoError(t, err)
	require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:customer_id": "first"}, false, true, false))
	for _, administrator := range []bool{false, true} {
		assertAttributeError(t, ValidateUserAttributes(schema, map[string]string{"custom:customer_id": "first"}, map[string]string{"custom:customer_id": "second"}, false, false, administrator), "InvalidParameterException")
		// An immutable optional attribute cannot be populated late either.
		assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:customer_id": "late"}, false, false, administrator), "InvalidParameterException")
	}
	require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:notes": "late"}, false, false, false))
}

func TestValidateUserAttributesDataTypesAndBounds(t *testing.T) {
	schema, err := NormalizeSchema([]SchemaAttribute{
		{Name: "label", Mutable: schemaFlag(true), StringAttributeConstraints: &StringAttributeConstraints{MinLength: "2", MaxLength: "3"}},
		{Name: "score", AttributeDataType: "Number", Mutable: schemaFlag(true), NumberAttributeConstraints: &NumberAttributeConstraints{MinValue: "0.1", MaxValue: "0.100000000000000000000000000000000000000000000000001"}},
		{Name: "enabled", AttributeDataType: "Boolean", Mutable: schemaFlag(true)},
		{Name: "joined", AttributeDataType: "DateTime", Mutable: schemaFlag(true)},
	})
	require.NoError(t, err)
	valid := []map[string]string{
		{"custom:label": "職種"},
		{"custom:score": "1e-1"},
		{"custom:score": "0.100000000000000000000000000000000000000000000000001"},
		{"custom:enabled": "false"},
		{"custom:joined": "2026-10-07T11:22:33.123456789+08:00"},
		{"birthdate": "2000-02-29"},
		{"phone_number": "+441234567890"},
	}
	for _, attributes := range valid {
		require.NoError(t, ValidateUserAttributes(schema, nil, attributes, false, true, true), "%v", attributes)
	}
	invalid := []map[string]string{
		{"custom:label": "職"},
		{"custom:label": "abcd"},
		{"custom:score": "0.100000000000000000000000000000000000000000000000002"},
		{"custom:score": "0.099999999999999999999999999999999999999999999999999"},
		{"custom:score": "NaN"},
		{"custom:score": "1/10"},
		{"custom:score": "0x10"},
		{"custom:enabled": "TRUE"},
		{"custom:enabled": "1"},
		{"custom:joined": "2026-10-07"},
		{"birthdate": "2001-02-29"},
		{"email": "invalid-email"},
		{"email": "Ada <ada@example.com>"},
		{"phone_number": "+44 (1234) 567890"},
	}
	for _, attributes := range invalid {
		assertAttributeError(t, ValidateUserAttributes(schema, nil, attributes, false, true, true), "InvalidParameterException")
	}
}

func TestNumericAttributeComparisonIsExactAndExponentBounded(t *testing.T) {
	schema, err := NormalizeSchema([]SchemaAttribute{
		{Name: "exact", AttributeDataType: "Number", NumberAttributeConstraints: &NumberAttributeConstraints{MinValue: "0.1", MaxValue: "0.1"}},
		{Name: "negative", AttributeDataType: "Number", NumberAttributeConstraints: &NumberAttributeConstraints{MinValue: "-2.50", MaxValue: "-0.25"}},
		{Name: "unbounded", AttributeDataType: "Number"},
	})
	require.NoError(t, err)
	for _, value := range []string{"0.1", "1e-1", "+.10", "10e-2", "0.100000000000000000000000000000000000000000000000000"} {
		require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:exact": value}, false, true, true), value)
	}
	for _, value := range []string{"-2.5", "-.25", "-125e-2"} {
		require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:negative": value}, false, true, true), value)
	}
	for _, value := range []string{"-2.500000000000000000000000000000000000000000000000001", "-0.249999999999999999999999999999999999999999999999999"} {
		assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:negative": value}, false, true, true), "InvalidParameterException")
	}
	// Extreme exponents are compared without constructing 10^exponent.
	require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:unbounded": "1e-99999999999999999999"}, false, true, true))
	require.NoError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:unbounded": "-0e99999999999999999999"}, false, true, true))
	assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{"custom:unbounded": "1e99999999999999999999"}, false, true, true), "InvalidParameterException")
}

func TestValidateUserAttributesTypedDeletionRetainsSchemaBoundaries(t *testing.T) {
	schema, err := NormalizeSchema([]SchemaAttribute{
		{Name: "email", Required: schemaFlag(true)},
		{Name: "score", AttributeDataType: "Number", Mutable: schemaFlag(true)},
		{Name: "enabled", AttributeDataType: "Boolean", Mutable: schemaFlag(true)},
		{Name: "joined", AttributeDataType: "DateTime", Mutable: schemaFlag(true)},
		{Name: "fixed", AttributeDataType: "Number"},
	})
	require.NoError(t, err)
	current := map[string]string{
		"email": "user@example.com", "custom:score": "1", "custom:enabled": "true",
		"custom:joined": "2026-10-07T10:00:00Z", "custom:fixed": "2",
	}
	for _, administrator := range []bool{false, true} {
		for _, name := range []string{"custom:score", "custom:enabled", "custom:joined"} {
			require.NoError(t, ValidateUserAttributes(schema, current, map[string]string{name: ""}, true, false, administrator), name)
			// Empty typed values at creation remain invalid, rather than
			// borrowing the update API's deletion convention.
			assertAttributeError(t, ValidateUserAttributes(schema, nil, map[string]string{name: ""}, false, true, administrator), "InvalidParameterException")
		}
		assertAttributeError(t, ValidateUserAttributes(schema, current, map[string]string{"email": ""}, false, false, administrator), "InvalidParameterException")
		assertAttributeError(t, ValidateUserAttributes(schema, current, map[string]string{"custom:fixed": ""}, false, false, administrator), "InvalidParameterException")
	}
	assert.Equal(t, "1", current["custom:score"], "validation does not delete stored input")
}
