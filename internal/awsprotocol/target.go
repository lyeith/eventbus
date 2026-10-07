package awsprotocol

import "strings"

// TargetAction extracts the operation after the first dot. The caller owns
// service-prefix admission; this helper does not select or authorize a service.
func TargetAction(target string) string {
	if _, action, ok := strings.Cut(target, "."); ok {
		return action
	}
	return ""
}

// JSONForTarget selects the response wire version for a supported AWS target.
// Unknown targets use JSON 1.0; service admission remains the caller's concern.
func JSONForTarget(target string) JSONProtocol {
	if strings.HasPrefix(target, "Firehose_") || strings.HasPrefix(target, "AmazonSSM.") || strings.HasPrefix(target, "secretsmanager.") {
		return JSON11
	}
	return JSON10
}
