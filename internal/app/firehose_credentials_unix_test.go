//go:build linux || darwin

package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/require"
)

// This is CLI composition coverage. The Firehose core independently verifies
// signatures against the selected secret and the receiver's actual wire bytes.
func TestCLIFirehoseS3Credentials(t *testing.T) {
	for _, scenario := range []struct {
		name, accessKey, secretKey, sessionToken string
		flags                                    []string
	}{
		{name: "legacy-defaults", accessKey: "test", secretKey: "testtest123"},
		{name: "custom-with-session-token", accessKey: "CLI_ACCESS_KEY_44", secretKey: "cli-private-secret-44", sessionToken: "cli-session-token-44",
			flags: []string{"--s3-access-key", "CLI_ACCESS_KEY_44", "--s3-secret-key", "cli-private-secret-44", "--s3-session-token", "cli-session-token-44"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			type delivery struct {
				method, path string
				headers      http.Header
				request      *http.Request
				body         []byte
				err          error
			}
			received := make(chan delivery, 4)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseDelivery := func() { releaseOnce.Do(func() { close(release) }) }
			var requests atomic.Int32
			sink := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				body, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
				detached := request.Clone(context.Background())
				detached.Body = nil
				observation := delivery{method: request.Method, path: request.URL.Path, headers: request.Header.Clone(), request: detached, body: body, err: err}
				select {
				case received <- observation:
				default:
				}
				select {
				case <-release:
					writer.WriteHeader(http.StatusOK)
				case <-request.Context().Done():
					writer.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			t.Cleanup(sink.Close)
			port := cliStartupPort(t)
			endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
			args, paths := cliStartupArguments(directory, port)
			// Override only this owned backend; credentials remain independent of
			// endpoint selection and the suite's native Firehose client identity.
			args = append(args, "--s3-endpoint", sink.URL+"/backend", "--issuer-base", endpoint)
			args = append(args, scenario.flags...)
			executable, err := os.Executable()
			require.NoError(t, err)
			command := exec.Command(executable, append([]string{"-test.run=^TestCLIStartupProcess$", "--"}, args...)...)
			if binary := os.Getenv("EVENTBUS_SMOKE_BINARY"); binary != "" {
				require.True(t, filepath.IsAbs(binary), "EVENTBUS_SMOKE_BINARY must select an absolute owned executable")
				info, err := os.Stat(binary)
				require.NoError(t, err)
				require.False(t, info.IsDir())
				command = exec.Command(binary, args...)
				t.Logf("packaged EventBus Firehose credential proof: %s", binary)
			}
			command.Dir = directory
			command.Env = append(os.Environ(), "EVENTBUS_CLI_STARTUP_CHILD=1")
			command.WaitDelay = time.Second
			logPath := filepath.Join(directory, "cli.log")
			log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err)
			t.Cleanup(func() { _ = log.Close() })
			command.Stdout, command.Stderr = log, log
			require.NoError(t, command.Start())
			done := make(chan struct{})
			var waitErr error
			go func() { waitErr = command.Wait(); close(done) }()
			client := &http.Client{Timeout: time.Second}
			t.Cleanup(func() {
				// Keep the destination alive through the actual CLI join, including
				// a failed assertion while its response is deliberately held.
				releaseDelivery()
				_ = command.Process.Signal(syscall.SIGTERM)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					_ = command.Process.Kill()
					<-done
				}
				client.CloseIdleConnections()
				if t.Failed() {
					data, _ := os.ReadFile(logPath)
					t.Logf("owned CLI output:\n%s", data)
				}
			})
			var health []byte
			require.Eventually(t, func() bool {
				response, err := client.Get(endpoint + "/health")
				if err != nil {
					return false
				}
				defer response.Body.Close()
				health, err = io.ReadAll(io.LimitReader(response.Body, 64<<10))
				var body struct {
					Service string `json:"service"`
				}
				return err == nil && response.StatusCode == http.StatusOK && json.Unmarshal(health, &body) == nil && body.Service == "eventbus"
			}, 5*time.Second, 10*time.Millisecond, "owned CLI never reached readiness")
			for _, path := range paths {
				_, err := os.Stat(path)
				require.NoError(t, err, "the CLI must use its owned database and captures")
			}
			sdk := firehose.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("native-producer", "native-producer-secret", ""), HTTPClient: client, RetryMaxAttempts: 1}, func(options *firehose.Options) {
				options.BaseEndpoint = aws.String(endpoint)
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			created, err := sdk.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
				DeliveryStreamName: aws.String("cli-credentials"), DeliveryStreamType: firehosetypes.DeliveryStreamTypeDirectPut,
				ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
					RoleARN: aws.String("arn:aws:iam::000000000000:role/cli-firehose"), BucketARN: aws.String("arn:aws:s3:::cli-owned-bucket"), Prefix: aws.String("owned/"),
					BufferingHints: &firehosetypes.BufferingHints{SizeInMBs: aws.Int32(1), IntervalInSeconds: aws.Int32(60)},
				},
			})
			require.NoError(t, err)
			require.Equal(t, "arn:aws:firehose:us-east-1:000000000000:deliverystream/cli-credentials", aws.ToString(created.DeliveryStreamARN))
			payload := []byte("{\"source\":\"owned-cli-credentials\"}\n")
			put, err := sdk.PutRecord(ctx, &firehose.PutRecordInput{DeliveryStreamName: aws.String("cli-credentials"), Record: &firehosetypes.Record{Data: payload}})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(put.RecordId))
			described, err := sdk.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String("cli-credentials")})
			require.NoError(t, err)
			readback, err := json.Marshal(described)
			require.NoError(t, err)
			require.Zero(t, requests.Load(), "a below-threshold record must remain buffered until graceful shutdown")
			require.NoError(t, command.Process.Signal(syscall.SIGTERM))
			var object delivery
			select {
			case object = <-received:
			case <-time.After(5 * time.Second):
				t.Fatal("SIGTERM did not flush the accepted Firehose record to its configured S3 endpoint")
			}
			require.NoError(t, object.err)
			require.Equal(t, http.MethodPut, object.method)
			require.True(t, strings.HasPrefix(object.path, "/backend/cli-owned-bucket/owned/"), "delivery must preserve the selected endpoint base path and native destination")
			require.Equal(t, payload, object.body)
			authorization := object.headers.Get("Authorization")
			require.Contains(t, authorization, "AWS4-HMAC-SHA256 Credential="+scenario.accessKey+"/")
			require.Contains(t, authorization, "/us-east-1/s3/aws4_request")
			require.Equal(t, scenario.sessionToken, object.headers.Get("X-Amz-Security-Token"))
			if scenario.sessionToken == "" {
				require.NotContains(t, object.headers, "X-Amz-Security-Token")
				require.NotContains(t, authorization, "x-amz-security-token")
			} else {
				require.Contains(t, authorization, "x-amz-security-token")
			}
			// Authenticate CLI composition with the expected secret. The core
			// tests separately verify the signer using independent canonical HMAC.
			stamp, err := time.Parse("20060102T150405Z", object.headers.Get("X-Amz-Date"))
			require.NoError(t, err)
			digest := sha256.Sum256(object.body)
			payloadHash := hex.EncodeToString(digest[:])
			require.Equal(t, payloadHash, object.headers.Get("X-Amz-Content-Sha256"))
			expected := object.request.Clone(t.Context())
			expected.Body = nil // Signing uses the actual received body hash.
			expected.Header.Del("Authorization")
			// net/http adds this transport header after the original signing.
			expected.Header.Del("Accept-Encoding")
			err = v4.NewSigner().SignHTTP(t.Context(), aws.Credentials{AccessKeyID: scenario.accessKey, SecretAccessKey: scenario.secretKey, SessionToken: scenario.sessionToken}, expected, payloadHash, "s3", "us-east-1", stamp, func(options *v4.SignerOptions) {
				options.DisableURIPathEscaping = true
			})
			require.NoError(t, err)
			require.Equal(t, expected.Header.Get("Authorization"), authorization, "the actual CLI delivery must authenticate with its selected secret")
			select {
			case <-done:
				t.Fatalf("CLI returned before its actual S3 response completed: %v", waitErr)
			default:
			}
			releaseDelivery()
			select {
			case <-done:
				require.NoError(t, waitErr, "graceful CLI shutdown must complete the actual S3 write")
			case <-time.After(5 * time.Second):
				t.Fatal("CLI did not join Firehose delivery after the S3 response completed")
			}
			require.EqualValues(t, 1, requests.Load(), "the one accepted record must be flushed exactly once")
			require.NoError(t, log.Close())
			logs, err := os.ReadFile(logPath)
			require.NoError(t, err)
			for _, surface := range []struct {
				name string
				data []byte
			}{{"health", health}, {"native readback", readback}, {"CLI log", logs}} {
				for _, private := range []string{scenario.secretKey, scenario.sessionToken} {
					if private != "" {
						require.NotContains(t, string(surface.data), private, "S3 credentials must stay out of %s", surface.name)
					}
				}
			}
		})
	}
}
