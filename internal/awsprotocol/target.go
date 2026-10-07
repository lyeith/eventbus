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
