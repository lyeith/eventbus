package gateway

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

// Facts are built from the original ingress and verified principal snapshot.
// Caller-supplied identity headers, application bodies and REQUEST context do not
// populate native IAM condition keys.
func iamRequestFacts(r *http.Request, credential *iamCredential, region string, now time.Time) map[string][]string {
	identity := credential.identity
	facts := map[string][]string{
		"aws:PrincipalAccount": {identity.AccountID},
		"aws:RequestedRegion":  {region},
		"aws:CurrentTime":      {now.UTC().Format(time.RFC3339Nano)},
		"aws:EpochTime":        {strconv.FormatInt(now.Unix(), 10)},
		"aws:SecureTransport":  {strconv.FormatBool(r.TLS != nil)},
	}
	if credential.policyPrincipalARN != "" {
		facts["aws:PrincipalArn"] = []string{credential.policyPrincipalARN}
	}
	if identity.UserID != "" {
		facts["aws:userid"] = []string{identity.UserID}
	}
	principal, _ := arn.Parse(identity.UserARN)
	kind := ""
	switch {
	case principal.Service == "iam" && principal.Resource == "root":
		kind = "Account"
	case principal.Service == "iam" && strings.HasPrefix(principal.Resource, "user/"):
		kind = "User"
		parts := strings.Split(principal.Resource, "/")
		facts["aws:username"] = []string{parts[len(parts)-1]}
	case principal.Service == "iam" && strings.HasPrefix(principal.Resource, "role/"):
		kind = "Role"
	case principal.Service == "sts" && strings.HasPrefix(principal.Resource, "assumed-role/"):
		kind = "AssumedRole"
	case principal.Service == "sts" && strings.HasPrefix(principal.Resource, "federated-user/"):
		kind = "FederatedUser"
	}
	if kind != "" {
		facts["aws:PrincipalType"] = []string{kind}
	}
	source, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		source = r.RemoteAddr
	}
	if net.ParseIP(source) != nil {
		facts["aws:SourceIp"] = []string{source}
	}
	if agent := r.UserAgent(); agent != "" {
		facts["aws:UserAgent"] = []string{agent}
	}
	if referer := r.Referer(); referer != "" {
		facts["aws:Referer"] = []string{referer}
	}
	if !credential.issuedAt.IsZero() {
		facts["aws:TokenIssueTime"] = []string{credential.issuedAt.UTC().Format(time.RFC3339Nano)}
	}
	if credential.mfaPresent != nil {
		facts["aws:MultiFactorAuthPresent"] = []string{strconv.FormatBool(*credential.mfaPresent)}
	}
	for key, value := range credential.principalTags {
		facts["aws:PrincipalTag/"+key] = []string{value}
	}
	return facts
}
