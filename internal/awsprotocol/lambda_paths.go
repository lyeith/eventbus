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
