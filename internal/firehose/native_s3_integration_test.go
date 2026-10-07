//go:build integration

package firehose

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Fixture cleanup rights are acquired only after a unique bucket is created on
// an explicitly selected loopback RustFS. It never resets another stack/data.
type nativeS3Fixture struct{ endpoint, bucket string }

func newNativeS3Fixture(t *testing.T) *nativeS3Fixture {
	t.Helper()
	endpoint := os.Getenv("S3_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("explicit owned S3_ENDPOINT_URL is required")
	}
	address, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, address.Hostname() == "localhost" || net.ParseIP(address.Hostname()).IsLoopback())
	fixture := &nativeS3Fixture{endpoint: endpoint, bucket: "eventbus-firehose-" + uuid.NewString()}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	code, body, err := fixture.request(ctx, http.MethodPut, "", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code, string(body))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		keys, err := fixture.list(ctx)
		require.NoError(t, err)
		for _, key := range keys {
			code, _, err := fixture.request(ctx, http.MethodDelete, "/"+key, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, code)
		}
		code, _, err := fixture.request(ctx, http.MethodDelete, "", nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, code)
	})
	return fixture
}
func (fixture *nativeS3Fixture) request(ctx context.Context, method, key string, payload []byte) (int, []byte, error) {
	target, err := url.Parse(strings.TrimRight(fixture.endpoint, "/") + "/" + fixture.bucket)
	if err != nil {
		return 0, nil, err
	}
	if strings.HasPrefix(key, "?") {
		target.RawQuery = key[1:]
	} else {
		target.Path += key
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "testtest123"}, request, payloadHash, "s3", "us-east-1", time.Now(), func(options *v4.SignerOptions) { options.DisableURIPathEscaping = true }); err != nil {
		return 0, nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	return response.StatusCode, body, err
}
func (fixture *nativeS3Fixture) list(ctx context.Context) ([]string, error) {
	code, body, err := fixture.request(ctx, http.MethodGet, "?list-type=2", nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, fmt.Errorf("list bucket status %d", code)
	}
	var result struct {
		Contents    []struct{ Key string }
		IsTruncated bool
	}
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if result.IsTruncated {
		return nil, fmt.Errorf("fixture listing unexpectedly truncated")
	}
	keys := make([]string, len(result.Contents))
	for index, object := range result.Contents {
		keys[index] = object.Key
	}
	return keys, nil
}

func TestFirehoseFinalDrainPersistsRecordsOnNativeRustFS(t *testing.T) {
	fixture := newNativeS3Fixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	manager := NewFirehoseManager("us-east-1", "000000000000", fixture.endpoint, "test", "testtest123")
	t.Cleanup(func() { _ = manager.Shutdown() })
	stream, err := manager.CreateStream("native", fixture.bucket, "records/", "", 100, 3600)
	require.NoError(t, err)
	_, err = manager.PutRecord(stream, []byte("{\"record\":1}\n"))
	require.NoError(t, err)
	require.NoError(t, manager.ShutdownContext(ctx))
	keys, err := fixture.list(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	code, body, err := fixture.request(ctx, http.MethodGet, "/"+keys[0], nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "{\"record\":1}\n", string(body))
	require.NoError(t, manager.ShutdownContext(ctx))
	after, err := fixture.list(ctx)
	require.NoError(t, err)
	require.Equal(t, keys, after, "repeated shutdown must not deliver twice")
}

func TestNativeS3PrefixEscapingPreservesSpecialCharacters(t *testing.T) {
	fixture := newNativeS3Fixture(t)
	manager := NewFirehoseManager("us-east-1", "000000000000", fixture.endpoint, "test", "testtest123")
	t.Cleanup(func() { _ = manager.Shutdown() })
	stream, err := manager.CreateStream("escaped", fixture.bucket, "literal space/?#%/", "", 5, 60)
	require.NoError(t, err)
	_, err = manager.PutRecord(stream, []byte("exact payload"))
	require.NoError(t, err)
	require.NoError(t, manager.ShutdownContext(t.Context()))
	keys, err := fixture.list(t.Context())
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.True(t, strings.HasPrefix(keys[0], "literal space/?#%/"), keys[0])
	code, body, err := fixture.request(t.Context(), http.MethodGet, "/"+keys[0], nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "exact payload", string(body))
}
