// Native user-pool schema normalization and user attribute admission.
package cognito

import (
	"fmt"
	"math/big"
	"net/mail"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// SchemaAttribute is Cognito's SchemaAttributeType. Pointer flags distinguish
// omitted request values from explicit false before normalization.
type SchemaAttribute struct {
	Name                       string                      `json:"Name"`
	AttributeDataType          string                      `json:"AttributeDataType,omitempty"`
	DeveloperOnlyAttribute     *bool                       `json:"DeveloperOnlyAttribute,omitempty"`
	Mutable                    *bool                       `json:"Mutable,omitempty"`
	Required                   *bool                       `json:"Required,omitempty"`
	StringAttributeConstraints *StringAttributeConstraints `json:"StringAttributeConstraints,omitempty"`
	NumberAttributeConstraints *NumberAttributeConstraints `json:"NumberAttributeConstraints,omitempty"`
}

type StringAttributeConstraints struct {
	MinLength string `json:"MinLength,omitempty"`
	MaxLength string `json:"MaxLength,omitempty"`
}

type NumberAttributeConstraints struct {
	MinValue string `json:"MinValue,omitempty"`
	MaxValue string `json:"MaxValue,omitempty"`
}

// AttributeValidationError carries the native error category without coupling
// schema and permission decisions to an HTTP transport.
type AttributeValidationError struct {
	Code    string
	Message string
}

func (e *AttributeValidationError) Error() string { return e.Message }

func invalidAttribute(format string, args ...any) error {
	return &AttributeValidationError{Code: "InvalidParameterException", Message: fmt.Sprintf(format, args...)}
}

func deniedAttribute(format string, args ...any) error {
	return &AttributeValidationError{Code: "NotAuthorizedException", Message: fmt.Sprintf(format, args...)}
}

var standardAttributeNames = []string{
	"sub", "name", "given_name", "family_name", "middle_name", "nickname",
	"preferred_username", "profile", "picture", "website", "email", "email_verified",
	"gender", "birthdate", "zoneinfo", "locale", "phone_number", "phone_number_verified",
	"address", "updated_at",
}

func schemaFlag(value bool) *bool { return &value }

func isStandardAttribute(name string) bool {
	for _, standard := range standardAttributeNames {
		if name == standard {
			return true
		}
	}
	return false
}

func defaultAttributeSchema() []SchemaAttribute {
	result := make([]SchemaAttribute, 0, len(standardAttributeNames))
	for _, name := range standardAttributeNames {
		attribute := SchemaAttribute{
			Name: name, AttributeDataType: "String", DeveloperOnlyAttribute: schemaFlag(false),
			Mutable: schemaFlag(name != "sub"), Required: schemaFlag(name == "sub"),
			StringAttributeConstraints: &StringAttributeConstraints{MinLength: "0", MaxLength: "2048"},
		}
		switch name {
		case "sub":
			attribute.StringAttributeConstraints.MinLength = "1"
		case "birthdate":
			attribute.StringAttributeConstraints = &StringAttributeConstraints{MinLength: "10", MaxLength: "10"}
		case "email_verified", "phone_number_verified":
			attribute.AttributeDataType = "Boolean"
			attribute.StringAttributeConstraints = nil
		case "updated_at":
			attribute.AttributeDataType = "Number"
			attribute.StringAttributeConstraints = nil
			attribute.NumberAttributeConstraints = &NumberAttributeConstraints{MinValue: "0"}
		}
		result = append(result, attribute)
	}
	return result
}

// NormalizeSchema merges selected standard properties into the native default
// schema and assigns custom:/dev: names. Its result is independent of its input
// and can be persisted and normalized again without changing meaning.
func NormalizeSchema(requested []SchemaAttribute) ([]SchemaAttribute, error) {
	result := defaultAttributeSchema()
	indexes := make(map[string]int, len(result)+len(requested))
	for index, attribute := range result {
		indexes[attribute.Name] = index
	}
	seen := make(map[string]bool, len(requested))
	customCount := 0
	for _, request := range requested {
		name, developer, err := normalizeSchemaName(request.Name, request.DeveloperOnlyAttribute)
		if err != nil {
			return nil, err
		}
		if seen[name] {
			return nil, invalidAttribute("Duplicate schema attribute %q", name)
		}
		seen[name] = true
		standard := isStandardAttribute(name)
		var attribute SchemaAttribute
		if standard {
			attribute = result[indexes[name]]
		} else {
			customCount++
			if customCount > 50 {
				return nil, invalidAttribute("A user pool can have at most 50 custom or developer attributes")
			}
			attribute = SchemaAttribute{Name: name, AttributeDataType: "String", Mutable: schemaFlag(false), Required: schemaFlag(false)}
		}
		attribute.DeveloperOnlyAttribute = schemaFlag(developer)
		if request.AttributeDataType != "" {
			if request.AttributeDataType != attribute.AttributeDataType {
				attribute.StringAttributeConstraints = nil
				attribute.NumberAttributeConstraints = nil
			}
			attribute.AttributeDataType = request.AttributeDataType
		}
		if request.Mutable != nil {
			attribute.Mutable = schemaFlag(*request.Mutable)
		}
		if request.Required != nil {
			attribute.Required = schemaFlag(*request.Required)
		}
		if !standard && *attribute.Required {
			return nil, invalidAttribute("Custom and developer attributes cannot be required: %s", name)
		}
		if name == "sub" && (attribute.AttributeDataType != "String" || *attribute.Mutable || !*attribute.Required) {
			return nil, invalidAttribute("The sub attribute is required, immutable and system generated")
		}
		if (name == "email_verified" || name == "phone_number_verified") && (attribute.AttributeDataType != "Boolean" || !*attribute.Mutable || *attribute.Required) {
			return nil, invalidAttribute("The %s verification attribute cannot be reconfigured", name)
		}
		if request.StringAttributeConstraints != nil {
			constraints := *request.StringAttributeConstraints
			attribute.StringAttributeConstraints = &constraints
		}
		if request.NumberAttributeConstraints != nil {
			constraints := *request.NumberAttributeConstraints
			attribute.NumberAttributeConstraints = &constraints
		}
		if err := normalizeAttributeConstraints(&attribute); err != nil {
			return nil, err
		}
		if standard {
			result[indexes[name]] = attribute
		} else {
			indexes[name] = len(result)
			result = append(result, attribute)
		}
	}
	return result, nil
}

func normalizeSchemaName(name string, requestedDeveloper *bool) (string, bool, error) {
	developer := requestedDeveloper != nil && *requestedDeveloper
	prefix := ""
	switch {
	case strings.HasPrefix(name, "custom:"):
		prefix, name = "custom:", strings.TrimPrefix(name, "custom:")
		if developer {
			return "", false, invalidAttribute("custom: attributes cannot be marked developer-only")
		}
	case strings.HasPrefix(name, "dev:"):
		prefix, name = "dev:", strings.TrimPrefix(name, "dev:")
		if requestedDeveloper != nil && !*requestedDeveloper {
			return "", false, invalidAttribute("dev: attributes must be marked developer-only")
		}
		developer = true
	}
	// Native built-ins include phone_number_verified, whose name is longer
	// than the custom attribute name limit.
	if prefix == "" && isStandardAttribute(name) && !developer {
		return name, false, nil
	}
	if name == "" || utf8.RuneCountInString(name) > 20 || !utf8.ValidString(name) {
		return "", false, invalidAttribute("Schema attribute names must contain 1 to 20 characters")
	}
	for _, character := range name {
		if !unicode.IsLetter(character) && !unicode.IsMark(character) && !unicode.IsSymbol(character) && !unicode.IsNumber(character) && !unicode.IsPunct(character) {
			return "", false, invalidAttribute("Invalid schema attribute name %q", name)
		}
	}
	if developer {
		return "dev:" + name, true, nil
	}
	return "custom:" + name, false, nil
}

func normalizeAttributeConstraints(attribute *SchemaAttribute) error {
	switch attribute.AttributeDataType {
	case "String":
		if attribute.NumberAttributeConstraints != nil {
			return invalidAttribute("Number constraints are invalid for string attribute %s", attribute.Name)
		}
		if attribute.StringAttributeConstraints == nil {
			attribute.StringAttributeConstraints = &StringAttributeConstraints{}
		}
		constraints := attribute.StringAttributeConstraints
		if constraints.MinLength == "" {
			constraints.MinLength = "0"
		}
		if constraints.MaxLength == "" {
			constraints.MaxLength = "2048"
		}
		minimum, minError := strconv.Atoi(constraints.MinLength)
		maximum, maxError := strconv.Atoi(constraints.MaxLength)
		if minError != nil || maxError != nil || minimum < 0 || maximum < minimum || maximum > 2048 {
			return invalidAttribute("String constraints for %s must satisfy 0 <= minimum <= maximum <= 2048", attribute.Name)
		}
		constraints.MinLength, constraints.MaxLength = strconv.Itoa(minimum), strconv.Itoa(maximum)
	case "Number":
		if attribute.StringAttributeConstraints != nil {
			return invalidAttribute("String constraints are invalid for number attribute %s", attribute.Name)
		}
		if attribute.NumberAttributeConstraints == nil {
			return nil
		}
		constraints := attribute.NumberAttributeConstraints
		var minimum, maximum *attributeNumber
		var err error
		if constraints.MinValue != "" {
			minimum, err = parseAttributeNumber(constraints.MinValue, 131072)
			if err != nil {
				return invalidAttribute("Invalid numeric minimum for %s", attribute.Name)
			}
		}
		if constraints.MaxValue != "" {
			maximum, err = parseAttributeNumber(constraints.MaxValue, 131072)
			if err != nil {
				return invalidAttribute("Invalid numeric maximum for %s", attribute.Name)
			}
		}
		if minimum != nil && maximum != nil && minimum.Cmp(maximum) > 0 {
			return invalidAttribute("Numeric minimum exceeds maximum for %s", attribute.Name)
		}
	case "Boolean", "DateTime":
		if attribute.StringAttributeConstraints != nil || attribute.NumberAttributeConstraints != nil {
			return invalidAttribute("%s attributes cannot have string or number constraints", attribute.AttributeDataType)
		}
	default:
		return invalidAttribute("Unsupported AttributeDataType %q", attribute.AttributeDataType)
	}
	return nil
}

var attributeNumberPattern = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// attributeNumber keeps decimal digits and a power of ten. Comparing decimal
// order, then significant digits, is exact and does not allocate enormous
// powers for untrusted exponential notation.
type attributeNumber struct {
	negative bool
	digits   string
	exponent *big.Int
}

func parseAttributeNumber(value string, maxLength int) (*attributeNumber, error) {
	if len(value) > maxLength || !attributeNumberPattern.MatchString(value) {
		return nil, fmt.Errorf("invalid decimal number")
	}
	number := &attributeNumber{exponent: new(big.Int)}
	if value[0] == '-' || value[0] == '+' {
		number.negative = value[0] == '-'
		value = value[1:]
	}
	if index := strings.IndexAny(value, "eE"); index != -1 {
		number.exponent.SetString(value[index+1:], 10)
		value = value[:index]
	}
	if index := strings.IndexByte(value, '.'); index != -1 {
		fractionLength := len(value) - index - 1
		number.exponent.Sub(number.exponent, big.NewInt(int64(fractionLength)))
		value = value[:index] + value[index+1:]
	}
	number.digits = strings.TrimLeft(value, "0")
	if number.digits == "" {
		number.negative, number.digits = false, "0"
		number.exponent.SetInt64(0)
		return number, nil
	}
	trimmed := strings.TrimRight(number.digits, "0")
	number.exponent.Add(number.exponent, big.NewInt(int64(len(number.digits)-len(trimmed))))
	number.digits = trimmed
	limit := &attributeNumber{digits: new(big.Int).Lsh(big.NewInt(1), 1023).String(), exponent: new(big.Int)}
	if number.compareMagnitude(limit) > 0 {
		return nil, fmt.Errorf("number exceeds 2^1023")
	}
	return number, nil
}

func (number *attributeNumber) compareMagnitude(other *attributeNumber) int {
	if number.digits == "0" {
		if other.digits == "0" {
			return 0
		}
		return -1
	}
	if other.digits == "0" {
		return 1
	}
	order := new(big.Int).Add(number.exponent, big.NewInt(int64(len(number.digits))))
	otherOrder := new(big.Int).Add(other.exponent, big.NewInt(int64(len(other.digits))))
	if comparison := order.Cmp(otherOrder); comparison != 0 {
		return comparison
	}
	length := max(len(number.digits), len(other.digits))
	for index := range length {
		left, right := byte('0'), byte('0')
		if index < len(number.digits) {
			left = number.digits[index]
		}
		if index < len(other.digits) {
			right = other.digits[index]
		}
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
	}
	return 0
}

func (number *attributeNumber) Cmp(other *attributeNumber) int {
	if number.negative != other.negative {
		if number.negative {
			return -1
		}
		return 1
	}
	comparison := number.compareMagnitude(other)
	if number.negative {
		return -comparison
	}
	return comparison
}

func normalizedSchemaIndex(schema []SchemaAttribute) (map[string]SchemaAttribute, error) {
	normalized, err := NormalizeSchema(schema)
	if err != nil {
		return nil, err
	}
	index := make(map[string]SchemaAttribute, len(normalized))
	for _, attribute := range normalized {
		index[attribute.Name] = attribute
	}
	return index, nil
}

// ValidateUserAttributes admits a proposed write without mutating either map.
// Administrators are exempt from client permissions, not schema constraints.
// Required omissions are permitted for AdminCreateUser; its caller passes
// requireRequired=false and the NEW_PASSWORD_REQUIRED flow collects them later.
func ValidateUserAttributes(schema []SchemaAttribute, current, updates map[string]string, requireRequired, creating, administrator bool) error {
	index, err := normalizedSchemaIndex(schema)
	if err != nil {
		return err
	}
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
		if name == "sub" {
			return invalidAttribute("The sub attribute is system generated and cannot be written")
		}
		if !administrator && (*attribute.DeveloperOnlyAttribute || name == "email_verified" || name == "phone_number_verified") {
			return deniedAttribute("Attribute %s can only be written by an administrator", name)
		}
		if !creating && !*attribute.Mutable {
			return invalidAttribute("Attribute %s is immutable", name)
		}
		// UpdateUserAttributes uses an empty value to delete an optional
		// attribute, including typed custom values. Required values cannot
		// be removed, even by administrators or an unchecked-required write.
		if !creating && updates[name] == "" {
			if *attribute.Required {
				return invalidAttribute("Required attribute %s cannot be deleted", name)
			}
			continue
		}
		if err := validateAttributeValue(attribute, updates[name]); err != nil {
			return err
		}
	}
	merged := make(map[string]string, len(current)+len(updates))
	for name, value := range current {
		merged[name] = value
	}
	for name, value := range updates {
		merged[name] = value
	}
	for _, name := range []string{"email", "phone_number"} {
		if merged[name+"_verified"] == "true" && merged[name] == "" {
			return invalidAttribute("%s is required when %s_verified is true", name, name)
		}
	}
	if requireRequired {
		if missing := MissingRequiredAttributes(schema, merged); len(missing) != 0 {
			return invalidAttribute("Missing required attributes: %s", strings.Join(missing, ", "))
		}
	}
	return nil
}

func validateAttributeValue(attribute SchemaAttribute, value string) error {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > 2048 {
		return invalidAttribute("Attribute %s must be a UTF-8 value of at most 2048 characters", attribute.Name)
	}
	switch attribute.AttributeDataType {
	case "String":
		minimum, _ := strconv.Atoi(attribute.StringAttributeConstraints.MinLength)
		maximum, _ := strconv.Atoi(attribute.StringAttributeConstraints.MaxLength)
		length := utf8.RuneCountInString(value)
		if length < minimum || length > maximum {
			return invalidAttribute("Attribute %s must contain %d to %d characters", attribute.Name, minimum, maximum)
		}
	case "Number":
		number, err := parseAttributeNumber(value, 2048)
		if err != nil {
			return invalidAttribute("Attribute %s must be a finite decimal number", attribute.Name)
		}
		if constraints := attribute.NumberAttributeConstraints; constraints != nil {
			if constraints.MinValue != "" {
				minimum, _ := parseAttributeNumber(constraints.MinValue, 131072)
				if number.Cmp(minimum) < 0 {
					return invalidAttribute("Attribute %s is below its numeric minimum", attribute.Name)
				}
			}
			if constraints.MaxValue != "" {
				maximum, _ := parseAttributeNumber(constraints.MaxValue, 131072)
				if number.Cmp(maximum) > 0 {
					return invalidAttribute("Attribute %s exceeds its numeric maximum", attribute.Name)
				}
			}
		}
	case "Boolean":
		if value != "true" && value != "false" {
			return invalidAttribute("Attribute %s must be true or false", attribute.Name)
		}
	case "DateTime":
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return invalidAttribute("Attribute %s must be an RFC3339 date and time", attribute.Name)
		}
	}
	// Standard attributes retain their documented formats when selected by a
	// pool schema. An empty optional string is permitted unless its minimum
	// or a required-field check forbids it.
	if value != "" {
		switch attribute.Name {
		case "birthdate":
			if _, err := time.Parse("2006-01-02", value); err != nil || len(value) != 10 {
				return invalidAttribute("birthdate must be a valid YYYY-MM-DD date")
			}
		case "email":
			address, err := mail.ParseAddress(value)
			if err != nil || address.Address != value || !strings.Contains(value, "@") {
				return invalidAttribute("email must be a valid email address")
			}
		case "phone_number":
			if len(value) < 2 || value[0] != '+' || strings.ContainsAny(value[1:], "+ -().") {
				return invalidAttribute("phone_number must contain + followed by country code and digits")
			}
			for _, digit := range value[1:] {
				if digit < '0' || digit > '9' {
					return invalidAttribute("phone_number must contain + followed by country code and digits")
				}
			}
		}
	}
	return nil
}

// MissingRequiredAttributes returns stable native attribute names for a
// registration or NEW_PASSWORD_REQUIRED response. sub is generated by Cognito.
func MissingRequiredAttributes(schema []SchemaAttribute, current map[string]string) []string {
	index, err := normalizedSchemaIndex(schema)
	if err != nil {
		return nil // Configuration must be admitted with NormalizeSchema first.
	}
	missing := make([]string, 0)
	for name, attribute := range index {
		if name != "sub" && *attribute.Required && current[name] == "" {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	return missing
}

// ValidateNewPasswordAttributes completes missing required standard attributes
// during first-password assignment. Ordinary updates retain strict immutability;
// already assigned required values cannot be changed in this challenge.
func ValidateNewPasswordAttributes(schema []SchemaAttribute, current, updates map[string]string) error {
	normalized, err := NormalizeSchema(schema)
	if err != nil {
		return err
	}
	for index, attribute := range normalized {
		if attribute.Required != nil && *attribute.Required {
			if _, supplied := updates[attribute.Name]; supplied && current[attribute.Name] != "" {
				return invalidAttribute("NEW_PASSWORD_REQUIRED cannot modify a required attribute that already has a value")
			}
			if current[attribute.Name] == "" && isStandardAttribute(attribute.Name) {
				normalized[index].Mutable = schemaFlag(true)
			}
		}
	}
	return ValidateUserAttributes(normalized, attributeValidationState(current, updates), updates, true, false, false)
}
