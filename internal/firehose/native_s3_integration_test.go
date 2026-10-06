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

// Run only on an explicitly supplied, owned local RustFS. The fixture acquires
// cleanup rights only after creating its unique bucket successfully.
func TestFirehoseFinalDrainPersistsRecordsOnNativeRustFS(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("explicit owned S3_ENDPOINT_URL is required")
	}
	address, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.True(t, address.Hostname() == "localhost" || net.ParseIP(address.Hostname()).IsLoopback())
	bucket := "listener-firehose-" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request := func(ctx context.Context, method, key string, payload []byte) (int, []byte, error) {
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(endpoint, "/")+"/"+bucket+key, bytes.NewReader(payload))
		if err != nil {
			return 0, nil, err
		}
		sum := sha256.Sum256(payload)
		hash := hex.EncodeToString(sum[:])
		req.Header.Set("X-Amz-Content-Sha256", hash)
		err = v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "testtest123"}, req, hash, "s3", "us-east-1", time.Now())
		if err != nil {
			return 0, nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		return resp.StatusCode, body, err
	}
	code, body, err := request(ctx, http.MethodPut, "", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code, string(body))
	type contents struct {
		Contents    []struct{ Key string }
		IsTruncated bool
	}
	list := func(ctx context.Context) (contents, error) {
		code, body, err := request(ctx, http.MethodGet, "?list-type=2", nil)
		if err != nil {
			return contents{}, err
		}
		if code != http.StatusOK {
			return contents{}, fmt.Errorf("list bucket status %d", code)
		}
		var items contents
		err = xml.Unmarshal(body, &items)
		if err == nil && items.IsTruncated {
			err = fmt.Errorf("fixture listing unexpectedly truncated")
		}
		return items, err
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		items, err := list(cleanup)
		require.NoError(t, err)
		for _, object := range items.Contents {
			code, _, err := request(cleanup, http.MethodDelete, "/"+object.Key, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, code)
		}
		code, _, err := request(cleanup, http.MethodDelete, "", nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, code)
	})
	fm := NewFirehoseManager("us-east-1", "000000000000", endpoint, "test", "testtest123")
	stream, err := fm.CreateStream("native", bucket, "records/", "", 100, 3600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fm.Shutdown() })
	_, err = fm.PutRecord(stream, []byte("{\"record\":1}\n"))
	require.NoError(t, err)
	require.NoError(t, fm.ShutdownContext(ctx))
	items, err := list(ctx)
	require.NoError(t, err)
	require.Len(t, items.Contents, 1)
	code, body, err = request(ctx, http.MethodGet, "/"+items.Contents[0].Key, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "{\"record\":1}\n", string(body))
	require.NoError(t, fm.ShutdownContext(ctx))
	after, err := list(ctx)
	require.NoError(t, err)
	require.Equal(t, items, after, "repeated shutdown must not deliver twice")
}
