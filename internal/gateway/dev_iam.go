// Local static credential-to-identity fixtures. This adapter provides explicit
// inputs to native request verification; it is not IAM policy, STS or role
// management and never consults ambient credentials.
package gateway

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

// DevIAMCredential binds an access-key map entry to one explicit local principal.
// Optional IDs are fixture facts; they are never inferred from a principal ARN.
type DevIAMCredential struct {
	SecretAccessKey string `yaml:"secret_access_key"`
	SessionToken    string `yaml:"session_token"`
	PrincipalARN    string `yaml:"principal_arn"`
	AccountID       string `yaml:"account_id"`
	UserID          string `yaml:"user_id"`
	CallerID        string `yaml:"caller_id"`
}

func (cfg *Config) validateDevIAM() error {
	for accessKey, credential := range cfg.IAMCredentials {
		if !identifier.MatchString(accessKey) || len(accessKey) > 128 || credential.SecretAccessKey == "" {
			return fmt.Errorf("iam_credentials requires valid access-key names and nonempty secrets")
		}
		principal, err := arn.Parse(credential.PrincipalARN)
		if err != nil || !identifier.MatchString(principal.Partition) || principal.Region != "" ||
			!accountIdentifier.MatchString(credential.AccountID) || principal.AccountID != credential.AccountID ||
			strings.ContainsAny(credential.PrincipalARN, " \t\r\n\x00") || !localIAMPrincipal(principal) {
			return fmt.Errorf("iam_credentials requires a principal IAM/STS ARN matching its explicit account_id")
		}
		if strings.ContainsAny(credential.SessionToken+credential.UserID+credential.CallerID, "\r\n\x00") {
			return fmt.Errorf("iam_credentials token and optional identity IDs cannot contain framing characters")
		}
	}
	return nil
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

func (cfg Config) devIAMCredentials() map[string]iamCredential {
	credentials := make(map[string]iamCredential, len(cfg.IAMCredentials))
	for key, fixture := range cfg.IAMCredentials {
		credentials[key] = iamCredential{
			secretAccessKey: fixture.SecretAccessKey, sessionToken: fixture.SessionToken,
			identity: iamIdentity{AccessKey: key, AccountID: fixture.AccountID, UserARN: fixture.PrincipalARN, UserID: fixture.UserID, CallerID: fixture.CallerID},
		}
	}
	return credentials
}
