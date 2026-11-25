package awsprotocol

import (
	"regexp"
	"strings"
)

// LambdaTagsPathPrefix starts the native Lambda resource tag endpoint.
const LambdaTagsPathPrefix = "/2017-03-31/tags/"

var lambdaMappingARN = regexp.MustCompile(`^arn:aws[a-zA-Z0-9-]*:lambda:[a-z0-9-]+:[0-9]{12}:event-source-mapping:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// IsLambdaMappingTagPath selects the event-source mapping variant of the tag
// endpoint. The mapping service still validates resource ownership.
func IsLambdaMappingTagPath(path string) bool {
	resource, ok := strings.CutPrefix(path, LambdaTagsPathPrefix)
	return ok && lambdaMappingARN.MatchString(resource)
}

// LambdaEventInvokeConfigPath parses the versioned Lambda configuration
// collection and member. The Lambda handler validates the function reference.
func LambdaEventInvokeConfigPath(path string) (function string, list, ok bool) {
	resource, prefix := strings.CutPrefix(path, "/2019-09-25/functions/")
	function, operation, slash := strings.Cut(resource, "/")
	if !prefix || !slash || function == "" {
		return "", false, false
	}
	switch operation {
	case "event-invoke-config":
		return function, false, true
	case "event-invoke-config/list":
		return function, true, true
	default:
		return "", false, false
	}
}

func IsLambdaEventInvokeConfigPath(path string) bool {
	_, _, ok := LambdaEventInvokeConfigPath(path)
	return ok
}
