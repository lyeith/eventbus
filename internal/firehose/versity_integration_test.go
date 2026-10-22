//go:build integration && (linux || darwin)

package firehose

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/stretchr/testify/require"
)

// The pinned SDK supplies independent S3 request construction and decoded
// listing/readback. Neither it nor the real provider uses our test verifier.
const versityS3SDK = `import base64, json, os
import boto3, botocore
from botocore.config import Config
assert boto3.__version__ == "1.40.61", boto3.__version__
assert botocore.__version__ == "1.40.76", botocore.__version__
client = boto3.client("s3", endpoint_url=os.environ["VERSITY_ENDPOINT"], region_name="us-east-1",
    aws_access_key_id=os.environ["VERSITY_ACCESS"], aws_secret_access_key=os.environ["VERSITY_SECRET"],
    config=Config(signature_version="s3v4", s3={"addressing_style":"path"},
                  retries={"max_attempts":0}, connect_timeout=2, read_timeout=2,
                  request_checksum_calculation="when_required", response_checksum_validation="when_required"))
bucket = os.environ["VERSITY_BUCKET"]
if os.environ["VERSITY_OPERATION"] == "create":
    client.create_bucket(Bucket=bucket)
    print(json.dumps({"boto3":boto3.__version__, "botocore":botocore.__version__, "bucket_created":True}))
else:
    key, prefix = os.environ["VERSITY_KEY"], os.environ["VERSITY_PREFIX"]
    if os.environ["VERSITY_OPERATION"] == "put":
        client.meta.events.register("before-send.s3.PutObject", lambda request, **kwargs:
            print(json.dumps({"sdk_put_wire_url":request.url}), flush=True))
        client.put_object(Bucket=bucket, Key=key, Body=base64.b64decode(os.environ["VERSITY_BODY"]))
    listing = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    assert not listing.get("IsTruncated"), listing
    keys = [item["Key"] for item in listing.get("Contents", [])]
    assert keys == [key], keys
    response = client.get_object(Bucket=bucket, Key=key)
    try:
        body = response["Body"].read()
    finally:
        response["Body"].close()
    assert body == base64.b64decode(os.environ["VERSITY_BODY"]), body
    print(json.dumps({"key":keys[0], "body_base64":base64.b64encode(body).decode("ascii")}))
client.close()
`

type versityPutObservation struct {
	path, escapedPath, responseBody string
	status                          int
}

type versityResponseCapture struct {
	io.ReadCloser
	body   bytes.Buffer
	closed func(string)
}

func (capture *versityResponseCapture) Read(p []byte) (int, error) {
	n, err := capture.ReadCloser.Read(p)
	if remaining := (64 << 10) - capture.body.Len(); remaining > 0 {
		_, _ = capture.body.Write(p[:min(n, remaining)])
	}
	return n, err
}

func (capture *versityResponseCapture) Close() error {
	err := capture.ReadCloser.Close()
	capture.closed(capture.body.String())
	return err
}

type versityCaptureTransport struct {
	base http.RoundTripper
	mu   sync.Mutex
	puts []versityPutObservation
}

func (transport *versityCaptureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err == nil && request.Method == http.MethodPut {
		observation := versityPutObservation{path: request.URL.Path, escapedPath: request.URL.EscapedPath(), status: response.StatusCode}
		response.Body = &versityResponseCapture{ReadCloser: response.Body, closed: func(body string) {
			observation.responseBody = body
			transport.mu.Lock()
			transport.puts = append(transport.puts, observation)
			transport.mu.Unlock()
		}}
	}
	return response, err
}

func TestFirehoseSignedDeliveryToNativeVersityGW(t *testing.T) {
	binary := os.Getenv("EVENTBUS_TEST_VERSITYGW_BINARY")
	if binary == "" {
		t.Skip("set EVENTBUS_TEST_VERSITYGW_BINARY to an existing VersityGW1.9.0 executable; this test starts its own isolated provider")
	}
	require.True(t, filepath.IsAbs(binary))
	versionContext, stopVersion := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopVersion()
	version, err := exec.CommandContext(versionContext, binary, "--version").CombinedOutput()
	require.NoError(t, err)
	require.Contains(t, string(version), "Version  : 1.9.0\n", "native proof requires the qualified provider version")
	t.Logf("native provider: %s", strings.TrimSpace(string(version)))
	root := t.TempDir()
	for _, name := range []string{"objects", "metadata", "iam"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, name), 0700))
	}
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := reserved.Addr().String()
	require.NoError(t, reserved.Close())
	endpoint := "http://" + address
	credentials := S3Credentials{AccessKeyID: "owned-" + uuid.NewString(), SecretAccessKey: uuid.NewString() + uuid.NewString()}
	bucket := "eventbus-versity-" + uuid.NewString()
	var environment []string
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, "VGW_") && !strings.HasPrefix(item, "ROOT_ACCESS_") && !strings.HasPrefix(item, "ROOT_SECRET_") {
			environment = append(environment, item)
		}
	}
	environment = append(environment, "ROOT_ACCESS_KEY_ID="+credentials.AccessKeyID, "ROOT_SECRET_ACCESS_KEY="+credentials.SecretAccessKey)
	lifetime, stop := context.WithCancel(context.Background())
	command := exec.CommandContext(lifetime, binary, "--port", address, "--region", "us-east-1", "--quiet", "--health", "/health",
		"--iam-dir", filepath.Join(root, "iam"), "posix", "--sidecar", filepath.Join(root, "metadata"), filepath.Join(root, "objects"))
	command.Env, command.Dir = environment, root
	require.NoError(t, localexec.Configure(command))
	logPath := filepath.Join(root, "provider.log")
	output, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	command.Stdout, command.Stderr = output, output
	require.NoError(t, command.Start())
	done := make(chan struct{})
	var providerError error
	go func() { providerError = command.Wait(); close(done) }()
	assertProviderRunning := func(t *testing.T) {
		t.Helper()
		select {
		case <-done:
			t.Fatalf("owned VersityGW exited unexpectedly: %v", providerError)
		default:
		}
	}
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(func() {
		stop() // localexec stops only this owned process group, then Wait joins it.
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owned VersityGW did not stop")
		}
		client.CloseIdleConnections()
		_ = output.Close()
		require.ErrorIs(t, syscall.Kill(command.Process.Pid, 0), syscall.ESRCH, "owned native provider survived cleanup")
		listener, err := net.Listen("tcp", address)
		require.NoError(t, err, "owned native provider listener survived cleanup")
		require.NoError(t, listener.Close())
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("owned provider output:\n%s", data)
		}
	})
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
		}
		response, err := client.Get(endpoint + "/health")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		select {
		case <-done:
			return false
		default:
			return response.StatusCode == http.StatusOK
		}
	}, 5*time.Second, 10*time.Millisecond, "owned native provider did not become ready")
	assertProviderRunning(t)
	repository, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	sdk := func(t *testing.T, operation, key, prefix string, body []byte) {
		t.Helper()
		assertProviderRunning(t)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		probe := exec.CommandContext(ctx, "uv", "run", "--frozen", "python", "-c", versityS3SDK)
		probe.Dir = repository
		probe.Env = append(os.Environ(), "VERSITY_ENDPOINT="+endpoint, "VERSITY_ACCESS="+credentials.AccessKeyID,
			"VERSITY_SECRET="+credentials.SecretAccessKey, "VERSITY_BUCKET="+bucket, "VERSITY_OPERATION="+operation,
			"VERSITY_KEY="+key, "VERSITY_PREFIX="+prefix, "VERSITY_BODY="+base64.StdEncoding.EncodeToString(body))
		require.NoError(t, localexec.Configure(probe))
		result, err := probe.CombinedOutput()
		assertProviderRunning(t)
		require.NoError(t, err, "unchanged pinned S3 SDK operation %s: %s", operation, result)
		t.Logf("native S3 SDK %s: %s", operation, strings.TrimSpace(string(result)))
	}
	sdk(t, "create", "", "", nil)
	for _, test := range []struct{ name, prefix string }{
		{"simple-control", "simple/"},
		{"partition-equals", "topic=example/year=2026/"},
		{"plus", "plus+value/"},
		{"spaces", "space value/"},
		{"unicode", "Unicode-é-東京/"},
		{"literal-percent", "literal%2F-percent%value/"},
	} {
		t.Run("sdk-"+test.name, func(t *testing.T) {
			prefix := "sdk-control/" + test.prefix
			sdk(t, "put", prefix+"exact-object", prefix, []byte("unchanged SDK body"))
		})
		t.Run("firehose-"+test.name, func(t *testing.T) {
			manager := NewFirehoseManagerWithS3Credentials("us-east-1", "000000000000", endpoint, credentials)
			transport := http.DefaultTransport.(*http.Transport).Clone()
			t.Cleanup(transport.CloseIdleConnections)
			capture := &versityCaptureTransport{base: transport}
			manager.httpClient.Transport = capture
			t.Cleanup(func() { _ = manager.Shutdown() })
			stream, err := manager.CreateStream(test.name, bucket, test.prefix, "", 100, 3600)
			require.NoError(t, err)
			body := append([]byte("owned exact payload for "+test.name+"\n"), 0, 0xff, '\n')
			_, err = manager.PutRecord(stream, body)
			require.NoError(t, err)
			flushErr := manager.flushBuffer(t.Context(), stream)
			capture.mu.Lock()
			puts := append([]versityPutObservation(nil), capture.puts...)
			capture.mu.Unlock()
			require.Len(t, puts, 1)
			t.Logf("actual Firehose native PUT status=%d escaped_path=%q", puts[0].status, puts[0].escapedPath)
			if puts[0].status != http.StatusOK {
				var nativeError struct {
					Code             string
					CanonicalRequest string
				}
				require.NoError(t, xml.Unmarshal([]byte(puts[0].responseBody), &nativeError))
				t.Logf("native error code=%q canonical_request=%q", nativeError.Code, nativeError.CanonicalRequest)
			}
			require.NoError(t, flushErr, "native provider must authenticate the actual Firehose PUT")
			require.Equal(t, http.StatusOK, puts[0].status)
			key, ok := strings.CutPrefix(puts[0].path, "/"+bucket+"/")
			require.True(t, ok)
			require.True(t, strings.HasPrefix(key, test.prefix), key)
			sdk(t, "verify", key, test.prefix, body)
			snapshot, ok := manager.Snapshot(stream.Name)
			require.True(t, ok)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.PendingObjects)
			require.NoError(t, manager.ShutdownContext(t.Context()))
		})
	}
}
