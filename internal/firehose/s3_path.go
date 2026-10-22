package firehose

import (
	"net/url"
	"strings"

	"github.com/aws/smithy-go/encoding/httpbinding"
)

// s3ObjectURL keeps decoded object identity separate from its strict S3 wire
// encoding. Go's default URL path escaping permits '=' and '+', but S3 canonical
// URIs encode both. Existing endpoint escapes are decoded only within their
// original segment, so an encoded slash remains distinct from a path separator.
func s3ObjectURL(endpoint, bucket, key string) (*url.URL, error) {
	target, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	segments := strings.Split(strings.TrimSuffix(target.EscapedPath(), "/"), "/")
	for index, segment := range segments {
		decoded, err := url.PathUnescape(segment)
		if err != nil {
			return nil, err
		}
		segments[index] = httpbinding.EscapePath(decoded, true)
	}
	rawPath := strings.Join(segments, "/") + "/" + httpbinding.EscapePath(bucket+"/"+key, false)
	path, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, err
	}
	target.Path = path
	target.RawPath = rawPath
	return target, nil
}
