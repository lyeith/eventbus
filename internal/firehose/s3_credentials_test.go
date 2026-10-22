package firehose

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Verify the receiver's actual bytes and SigV4 headers independently of the
// production AWS signer. A shared signer on both sides would miss its misuse.
func verifyS3DeliverySignature(request *http.Request, body []byte, credentials S3Credentials, region string) error {
	const algorithm = "AWS4-HMAC-SHA256"
	authorization, ok := strings.CutPrefix(request.Header.Get("Authorization"), algorithm+" ")
	if !ok {
		return errors.New("missing SigV4 authorization")
	}
	attributes := map[string]string{}
	for _, item := range strings.Split(authorization, ", ") {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			return errors.New("invalid SigV4 authorization attribute")
		}
		attributes[key] = value
	}
	if len(attributes) != 3 {
		return errors.New("unexpected SigV4 authorization attributes")
	}
	scope := strings.Split(attributes["Credential"], "/")
	if len(scope) != 5 || scope[0] != credentials.AccessKeyID || scope[2] != region || scope[3] != "s3" || scope[4] != "aws4_request" {
		return errors.New("unexpected SigV4 credential scope")
	}
	stamp := request.Header.Get("X-Amz-Date")
	instant, err := time.Parse("20060102T150405Z", stamp)
	if err != nil || scope[1] != instant.Format("20060102") {
		return errors.New("invalid SigV4 signing date")
	}
	signedHeaders := "content-length;content-type;host;x-amz-content-sha256;x-amz-date"
	if credentials.SessionToken != "" {
		signedHeaders += ";x-amz-security-token"
	}
	if attributes["SignedHeaders"] != signedHeaders {
		return errors.New("unexpected SigV4 canonical signed headers")
	}
	token, tokenPresent := request.Header["X-Amz-Security-Token"]
	if credentials.SessionToken == "" {
		if tokenPresent {
			return errors.New("unexpected session-token header")
		}
	} else if len(token) != 1 || token[0] != credentials.SessionToken {
		return errors.New("incorrect session-token header")
	}
	if request.ContentLength != int64(len(body)) {
		return errors.New("incorrect signed content length")
	}
	payloadHash := sha256.Sum256(body)
	hashText := hex.EncodeToString(payloadHash[:])
	if request.Header.Get("X-Amz-Content-Sha256") != hashText {
		return errors.New("signed payload hash does not match actual body")
	}
	var headers strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		value := ""
		switch name {
		case "host":
			value = request.Host
		case "content-length":
			value = strconv.Itoa(len(body))
		default:
			var values []string
			for _, v := range request.Header.Values(name) {
				values = append(values, strings.Join(strings.Fields(v), " "))
			}
			value = strings.Join(values, ",")
		}
		fmt.Fprintf(&headers, "%s:%s\n", name, value)
	}
	query := request.URL.Query()
	for key := range query {
		sort.Strings(query[key])
	}
	canonicalQuery := strings.ReplaceAll(query.Encode(), "+", "%20")
	canonicalPath, err := canonicalS3TestPath(strings.SplitN(request.RequestURI, "?", 2)[0])
	if err != nil {
		return err
	}
	canonical := strings.Join([]string{request.Method, canonicalPath, canonicalQuery, headers.String(), signedHeaders, hashText}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonical))
	toSign := strings.Join([]string{algorithm, stamp, strings.Join(scope[1:], "/"), hex.EncodeToString(canonicalHash[:])}, "\n")
	key := []byte("AWS4" + credentials.SecretAccessKey)
	for _, component := range scope[1:] {
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write([]byte(component))
		key = mac.Sum(nil)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(toSign))
	received, err := hex.DecodeString(attributes["Signature"])
	if err != nil || !hmac.Equal(received, mac.Sum(nil)) {
		return errors.New("SigV4 signature did not authenticate actual request")
	}
	return nil
}

func TestFirehoseS3StaticCredentialsSignDeliveredObjects(t *testing.T) {
	// Explicit configuration must not be overridden by an ambient AWS profile or
	// session token. The manager never invokes a provider chain.
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	t.Setenv("AWS_SESSION_TOKEN", "ambient-session")
	const region = "eu-west-2"
	for _, test := range []struct {
		name        string
		credentials S3Credentials
		legacy      bool
	}{
		{"custom-static", S3Credentials{AccessKeyID: "selected-access", SecretAccessKey: "selected-secret"}, false},
		{"custom-session", S3Credentials{AccessKeyID: "selected-temp-access", SecretAccessKey: "selected-temp-secret", SessionToken: "selected/session+token=="}, false},
		{"legacy-constructor", S3Credentials{AccessKeyID: "test", SecretAccessKey: "testtest123"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			type received struct {
				body    []byte
				path    string
				rawPath string
				failure error
			}
			var mu sync.Mutex
			var objects []received
			sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if r.Method != http.MethodPut {
					err = errors.Join(err, errors.New("expected S3 PUT"))
				}
				err = errors.Join(err, verifyS3DeliverySignature(r, body, test.credentials, region))
				mu.Lock()
				objects = append(objects, received{body, r.URL.Path, strings.SplitN(r.RequestURI, "?", 2)[0], err})
				mu.Unlock()
				if err != nil {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(sink.Close)
			endpoint := sink.URL + "/s3%20base/encoded%2Fsegment/%25tenant%3Dfoo%2B?z=last&trace=b+space&trace=a"
			credentials := test.credentials
			var manager *FirehoseManager
			if test.legacy {
				manager = NewFirehoseManager(region, "123456789012", endpoint, credentials.AccessKeyID, credentials.SecretAccessKey)
			} else {
				manager = NewFirehoseManagerWithS3Credentials(region, "123456789012", endpoint, credentials)
			}
			// Passing a value fixes all three selections before background work starts.
			credentials = S3Credentials{AccessKeyID: "changed", SecretAccessKey: "changed", SessionToken: "changed"}
			t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
			stream, err := manager.CreateStream("signed", "selected-bucket", "topic=example/year=2026/space prefix/plus+unicode-é/percent%2F%-/", "", 100, 3600)
			require.NoError(t, err)
			payload := []byte{'e', 'x', 'a', 'c', 't', '\n', 0, 0xff, '\n'}
			_, err = manager.PutRecord(stream, payload)
			require.NoError(t, err)
			err = manager.flushBuffer(t.Context(), stream)
			mu.Lock()
			actual := append([]received(nil), objects...)
			mu.Unlock()
			require.Len(t, actual, 1)
			require.NoError(t, actual[0].failure, "local S3 receiver must authenticate actual signed request")
			require.NoError(t, err)
			require.Equal(t, payload, actual[0].body)
			require.True(t, strings.HasPrefix(actual[0].path, "/s3 base/encoded/segment/%tenant=foo+/selected-bucket/topic=example/year=2026/space prefix/plus+unicode-é/percent%2F%-/"))
			require.True(t, strings.HasPrefix(actual[0].rawPath, "/s3%20base/encoded%2Fsegment/%25tenant%3Dfoo%2B/selected-bucket/topic%3Dexample/year%3D2026/space%20prefix/plus%2Bunicode-%C3%A9/percent%252F%25-/"))
			snapshot, ok := manager.Snapshot(stream.Name)
			require.True(t, ok)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.PendingObjects)
		})
	}
}
func TestFirehoseS3RedirectsRetainRecordsWithoutForwardingCredentials(t *testing.T) {
	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			credentials := S3Credentials{AccessKeyID: "selected-access", SecretAccessKey: "selected-secret", SessionToken: "selected-session"}
			var unexpectedCalls atomic.Int32
			destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				unexpectedCalls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(destination.Close)
			var redirect atomic.Bool
			redirect.Store(true)
			var mu sync.Mutex
			var paths []string
			var failures []error
			sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				err = errors.Join(err, verifyS3DeliverySignature(r, body, credentials, "us-east-1"))
				mu.Lock()
				paths = append(paths, r.URL.Path)
				failures = append(failures, err)
				mu.Unlock()
				if err != nil {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				if redirect.Load() {
					http.Redirect(w, r, destination.URL+"/must-not-receive", status)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(sink.Close)
			manager := NewFirehoseManagerWithS3Credentials("us-east-1", "000000000000", sink.URL, credentials)
			t.Cleanup(func() { redirect.Store(false); require.NoError(t, manager.Shutdown()) })
			stream, err := manager.CreateStream("redirect", "bucket", "private/", "", 100, 3600)
			require.NoError(t, err)
			_, err = manager.PutRecord(stream, []byte("must-remain-at-configured-endpoint"))
			require.NoError(t, err)
			err = manager.flushBuffer(t.Context(), stream)
			require.Error(t, err)
			require.ErrorContains(t, err, "status "+strconv.Itoa(status))
			snapshot, ok := manager.Snapshot(stream.Name)
			require.True(t, ok)
			require.Equal(t, 1, snapshot.BufferedRecords)
			require.Equal(t, 1, snapshot.PendingObjects)
			require.NotEmpty(t, snapshot.LastDeliveryError)
			require.NotContains(t, snapshot.LastDeliveryError, credentials.SecretAccessKey)
			require.NotContains(t, snapshot.LastDeliveryError, credentials.SessionToken)
			require.Zero(t, unexpectedCalls.Load(), "redirect must not forward credential headers or payload")
			redirect.Store(false)
			require.NoError(t, manager.flushBuffer(t.Context(), stream))
			snapshot, ok = manager.Snapshot(stream.Name)
			require.True(t, ok)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.PendingObjects)
			mu.Lock()
			actualPaths := append([]string(nil), paths...)
			actualFailures := append([]error(nil), failures...)
			mu.Unlock()
			require.Len(t, actualPaths, 2)
			require.Equal(t, actualPaths[0], actualPaths[1], "retry must retain its prepared object key")
			for _, failure := range actualFailures {
				require.NoError(t, failure, "only original endpoint sees signed requests")
			}
			require.Zero(t, unexpectedCalls.Load())
		})
	}
}

// Canonicalize the receiver's wire path independently of Go's permissive
// EscapedPath and the production Smithy encoder. Decode each wire segment once,
// then encode every non-unreserved byte; encoded endpoint slashes stay encoded.
func canonicalS3TestPath(raw string) (string, error) {
	segments := strings.Split(raw, "/")
	for index, segment := range segments {
		decoded, err := url.PathUnescape(segment)
		if err != nil {
			return "", err
		}
		var out strings.Builder
		for _, value := range []byte(decoded) {
			if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || strings.ContainsRune("-._~", rune(value)) {
				out.WriteByte(value)
			} else {
				fmt.Fprintf(&out, "%%%02X", value)
			}
		}
		segments[index] = out.String()
	}
	return strings.Join(segments, "/"), nil
}
