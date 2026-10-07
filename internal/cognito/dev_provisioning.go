// Development harness request extensions, retained for existing local fixtures.
// AWS-created pools/clients use generated identities and Policies.PasswordPolicy.
// Keep future AWS configuration in the native request/domain types; do not put
// native fields here merely because a local fixture also needs to supply them.
package cognito

import "encoding/json"

// Anonymous embedding preserves the existing flat JSON extension fields while
// making their development-only ownership explicit in the AWS request adapters.
type devCreateUserPoolFields struct {
	PoolID         string          `json:"PoolId"`
	PasswordPolicy json.RawMessage `json:"PasswordPolicy"`
}

type devCreateUserPoolClientFields struct {
	ClientID string `json:"ClientId"`
}
