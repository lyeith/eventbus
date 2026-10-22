package firehose

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3ObjectURLPreservesKeyAndEncodedEndpointSegments(t *testing.T) {
	for _, test := range []struct{ name, endpoint, key, path, raw string }{
		{"partition", "http://example.test", "topic=example/year=2026/file", "/bucket/topic=example/year=2026/file", "/bucket/topic%3Dexample/year%3D2026/file"},
		{"reserved", "http://example.test/base%20path", "plus+ space/é/percent%2F%/file", "/base path/bucket/plus+ space/é/percent%2F%/file", "/base%20path/bucket/plus%2B%20space/%C3%A9/percent%252F%25/file"},
		{"encoded-endpoint", "http://example.test/mount%2f/%252F/", "file=+%2F", "/mount//%2F/bucket/file=+%2F", "/mount%2F/%252F/bucket/file%3D%2B%252F"},
		{"encoded-trailing-slash", "http://example.test/mount%2F", "file", "/mount//bucket/file", "/mount%2F/bucket/file"},
		{"no-normalization", "http://example.test/base//", "../folder//file", "/base//bucket/../folder//file", "/base//bucket/../folder//file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, err := s3ObjectURL(test.endpoint+"?unchanged=a%2Bb", "bucket", test.key)
			require.NoError(t, err)
			require.Equal(t, test.path, target.Path)
			require.Equal(t, test.raw, target.RawPath)
			require.Equal(t, test.raw, target.EscapedPath())
			require.Equal(t, "unchanged=a%2Bb", target.RawQuery)
		})
	}
	_, err := s3ObjectURL("http://example.test/invalid%escape", "bucket", "key")
	require.Error(t, err)
}
