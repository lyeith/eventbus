// Native provisioning admits only implemented settings; selected unsupported
// settings fail before any resource state is changed.
package cognito

import (
	"encoding/json"

	"strings"
)

func selectedJSON(value json.RawMessage) bool {
	text := strings.TrimSpace(string(value))
	return text != "" && text != "null" && text != "false" && text != "0" && text != "\"\"" && text != "[]" && text != "{}"
}
func validateNativeFields(raw []byte, supported []string, defaults map[string]string) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	allowed := make(map[string]bool, len(supported))
	for _, name := range supported {
		allowed[name] = true
	}
	for name, value := range fields {
		if allowed[name] {
			continue
		}
		if expected, ok := defaults[name]; ok {
			var normalized any
			if json.Unmarshal(value, &normalized) == nil {
				encoded, _ := json.Marshal(normalized)
				if string(encoded) == expected {
					continue
				}
			}
			return &CapabilityError{Field: name}
		}
		if !selectedJSON(value) {
			continue
		}
		return &CapabilityError{Field: name}
	}
	return nil
}

func (req *createUserPoolRequest) UnmarshalJSON(raw []byte) error {
	type plain createUserPoolRequest
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if err := validateNativeFields(raw, []string{"PoolName", "PoolId", "PasswordPolicy", "Policies", "UsernameAttributes", "AliasAttributes", "UsernameConfiguration", "Schema", "AutoVerifiedAttributes", "AccountRecoverySetting", "AdminCreateUserConfig"}, map[string]string{"MfaConfiguration": "\"OFF\"", "DeletionProtection": "\"INACTIVE\"", "UserPoolTier": "\"ESSENTIALS\""}); err != nil {
		return err
	}
	if decoded.Policies != nil {
		if err := validateNativeFields(decoded.Policies.raw, []string{"PasswordPolicy"}, map[string]string{"SignInPolicy": "{\"AllowedFirstAuthFactors\":[\"PASSWORD\"]}"}); err != nil {
			return err
		}
	}
	*req = createUserPoolRequest(decoded)
	return nil
}
func (env *createPoolPoliciesEnv) UnmarshalJSON(raw []byte) error {
	type plain createPoolPoliciesEnv
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*env = createPoolPoliciesEnv(decoded)
	env.raw = raw
	return nil
}
func (req *createUserPoolClientRequest) UnmarshalJSON(raw []byte) error {
	type plain createUserPoolClientRequest
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if err := validateNativeFields(raw, []string{"UserPoolId", "ClientName", "ClientId", "ClientSecret", "GenerateSecret", "ExplicitAuthFlows", "AuthSessionValidity", "AccessTokenValidity", "IdTokenValidity", "RefreshTokenValidity", "TokenValidityUnits", "ReadAttributes", "WriteAttributes"}, map[string]string{"AllowedOAuthFlowsUserPoolClient": "false", "EnableTokenRevocation": "true", "PreventUserExistenceErrors": "\"LEGACY\"", "SupportedIdentityProviders": "[\"COGNITO\"]", "RefreshTokenRotation": "{\"Feature\":\"DISABLED\"}", "EnablePropagateAdditionalUserContextData": "false"}); err != nil {
		return err
	}
	*req = createUserPoolClientRequest(decoded)
	return nil
}

// CapabilityError identifies an unsupported selected native setting.
type CapabilityError struct{ Field string }

func (e *CapabilityError) Error() string {
	return e.Field + " is not supported by this Cognito emulator"
}
