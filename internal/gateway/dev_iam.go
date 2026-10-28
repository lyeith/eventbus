// Local credential/permission fixtures supply facts to native authentication.
// This adapter never mints credentials or consults ambient IAM/STS providers.
package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

// DevIAMCredential binds one local access key to explicit principal facts.
// Omitted Policy retains the original fixture's explicit allow-all grant;
// a supplied policy, including an empty Statement list, uses native default deny.
type DevIAMCredential struct {
	SecretAccessKey    string            `yaml:"secret_access_key"`
	SessionToken       string            `yaml:"session_token"`
	PrincipalARN       string            `yaml:"principal_arn"`
	AccountID          string            `yaml:"account_id"`
	UserID             string            `yaml:"user_id,omitempty"`
	CallerID           string            `yaml:"caller_id,omitempty"`
	ExpiresAt          time.Time         `yaml:"expires_at,omitempty"`
	IssuedAt           time.Time         `yaml:"issued_at,omitempty"`
	PolicyPrincipalARN string            `yaml:"policy_principal_arn,omitempty"`
	PrincipalTags      map[string]string `yaml:"principal_tags,omitempty"`
	MFAPresent         *bool             `yaml:"mfa_present,omitempty"`
	Policy             map[string]any    `yaml:"policy,omitempty"`
}

func (cfg *Config) validateDevIAM() error {
	_, err := compileDevIAMCredentials(cfg.IAMCredentials)
	return err
}

func compileDevIAMCredentials(entries map[string]DevIAMCredential) (map[string]iamCredential, error) {
	credentials := make(map[string]iamCredential, len(entries))
	for key, fixture := range entries {
		if !identifier.MatchString(key) || len(key) > 128 || fixture.SecretAccessKey == "" {
			return nil, fmt.Errorf("iam_credentials requires valid access-key names and nonempty secrets")
		}
		principal, err := arn.Parse(fixture.PrincipalARN)
		if err != nil || !identifier.MatchString(principal.Partition) || principal.Region != "" ||
			!accountIdentifier.MatchString(fixture.AccountID) || principal.AccountID != fixture.AccountID ||
			strings.ContainsAny(fixture.PrincipalARN, " \t\r\n\x00") || !localIAMPrincipal(principal) {
			return nil, fmt.Errorf("iam_credentials requires a principal IAM/STS ARN matching its explicit account_id")
		}
		if strings.ContainsAny(fixture.SessionToken+fixture.UserID+fixture.CallerID, "\r\n\x00") {
			return nil, fmt.Errorf("iam_credentials token and optional identity IDs cannot contain framing characters")
		}
		if !fixture.IssuedAt.IsZero() && !fixture.ExpiresAt.IsZero() && !fixture.ExpiresAt.After(fixture.IssuedAt) {
			return nil, fmt.Errorf("iam_credentials expires_at must follow issued_at")
		}
		policyPrincipal := fixture.PolicyPrincipalARN
		if policyPrincipal != "" {
			policyARN, err := arn.Parse(policyPrincipal)
			if err != nil || policyARN.Service != "iam" || policyARN.Region != "" || policyARN.Partition != principal.Partition ||
				policyARN.AccountID != fixture.AccountID || !localIAMResource(policyARN.Resource, "role/") ||
				strings.ContainsAny(policyPrincipal, " \t\r\n\x00") || principal.Service != "sts" || !localIAMResource(principal.Resource, "assumed-role/") {
				return nil, fmt.Errorf("iam_credentials policy_principal_arn must bind an assumed-role caller to an IAM role in its account")
			}
		} else if principal.Service != "sts" || !strings.HasPrefix(principal.Resource, "assumed-role/") {
			policyPrincipal = fixture.PrincipalARN
		}
		for tag, value := range fixture.PrincipalTags {
			if tag == "" || strings.ContainsAny(tag+value, "\r\n\x00") {
				return nil, fmt.Errorf("iam_credentials principal_tags require nonempty names without framing characters")
			}
		}
		document := fixture.Policy
		if document == nil {
			document = map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": "*"}}}
		}
		data, err := json.Marshal(document)
		if err != nil {
			return nil, fmt.Errorf("iam_credentials policy must be a JSON policy document")
		}
		policy, err := compileInvokePolicy(data)
		if err != nil {
			return nil, fmt.Errorf("iam_credentials policy: %w", err)
		}
		credentials[key] = iamCredential{
			secretAccessKey: fixture.SecretAccessKey, sessionToken: fixture.SessionToken,
			identity:  iamIdentity{AccessKey: key, AccountID: fixture.AccountID, UserARN: fixture.PrincipalARN, UserID: fixture.UserID, CallerID: fixture.CallerID},
			expiresAt: fixture.ExpiresAt, issuedAt: fixture.IssuedAt, policyPrincipalARN: policyPrincipal,
			principalTags: maps.Clone(fixture.PrincipalTags), mfaPresent: fixture.MFAPresent, policy: policy,
		}
	}
	return credentials, nil
}

func localIAMPrincipal(principal arn.ARN) bool {
	if principal.Service == "iam" {
		return principal.Resource == "root" || localIAMResource(principal.Resource, "user/") || localIAMResource(principal.Resource, "role/")
	}
	if principal.Service == "sts" {
		return localIAMResource(principal.Resource, "assumed-role/") || localIAMResource(principal.Resource, "federated-user/")
	}
	return false
}
func localIAMResource(resource, prefix string) bool {
	return strings.HasPrefix(resource, prefix) && len(resource) > len(prefix)
}

// DevReplaceIAMCredentials validates the entire candidate before publishing one
// detached generation. Failed replacement leaves the active generation intact.
// Already-admitted integration work keeps its normal lifetime.
func (gateway *Gateway) DevReplaceIAMCredentials(entries map[string]DevIAMCredential) error {
	credentials, err := compileDevIAMCredentials(entries)
	if err != nil {
		return err
	}
	gateway.iam.credentials.replace(credentials)
	return nil
}

// cloneDevIAMCredentials detaches JSON policy trees as well as typed fixture
// maps. Invalid policy values remain invalid for validation; they cannot become
// an omitted policy and accidentally receive the compatibility grant.
func cloneDevIAMCredentials(input map[string]DevIAMCredential) map[string]DevIAMCredential {
	if input == nil {
		return nil
	}
	result := make(map[string]DevIAMCredential, len(input))
	for key, fixture := range input {
		fixture.PrincipalTags = maps.Clone(fixture.PrincipalTags)
		if fixture.MFAPresent != nil {
			value := *fixture.MFAPresent
			fixture.MFAPresent = &value
		}
		if fixture.Policy != nil {
			data, err := json.Marshal(fixture.Policy)
			if err == nil {
				decoder := json.NewDecoder(bytes.NewReader(data))
				decoder.UseNumber()
				var document map[string]any
				if decoder.Decode(&document) == nil {
					fixture.Policy = document
				}
			}
		}
		result[key] = fixture
	}
	return result
}
