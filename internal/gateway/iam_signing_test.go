package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/require"
)

func iamSDKQueryRequest(t *testing.T, body []byte, token, expires string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, "http://local.example.test:4180/edge/v1/items/a%2Fb?repeat=two&repeat=one", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("X-Proof", "signed-proof")
	if expires != "" {
		query := request.URL.Query()
		query.Set("X-Amz-Expires", expires)
		request.URL.RawQuery = query.Encode()
	}
	hash := sha256.Sum256(body)
	signed, _, err := v4.NewSigner().PresignHTTP(t.Context(), aws.Credentials{
		AccessKeyID: "local-example-key", SecretAccessKey: "local-example-secret", SessionToken: token,
	}, request, hex.EncodeToString(hash[:]), "execute-api", "us-east-1", iamTestTime)
	require.NoError(t, err)
	incoming := httptest.NewRequest(request.Method, signed, bytes.NewReader(body))
	incoming.Header = request.Header.Clone()
	incoming.Host, incoming.ContentLength = request.Host, request.ContentLength
	return incoming
}

func TestIAMVerifierFrozenBotocoreDateAndQueryControls(t *testing.T) {
	// Unchanged botocore 1.40.76 SigV4Auth Date-only and SigV4QueryAuth
	// execute-api controls, generated with explicit local example credentials.
	dateRequest := httptest.NewRequest(http.MethodPost,
		"http://127.0.0.1:14180/edge/v1/items/example%2Fhistory?literal=%252F", bytes.NewReader([]byte{0, 'd', 'a', 't', 'e', 255}))
	dateRequest.Header = http.Header{
		"Content-Type":         {"application/octet-stream"},
		"Date":                 {"Wed, 28 Oct 2026 01:02:03 -0000"},
		"X-Amz-Security-Token": {"local-example-token"},
		"Authorization":        {"AWS4-HMAC-SHA256 Credential=local-example-key/20261028/us-east-1/execute-api/aws4_request, SignedHeaders=content-type;date;host;x-amz-security-token, Signature=ad844310b449a14d9756a3461100e5c5e0445c2da16ff3670d8e45db267ad2fa"},
	}
	queryRequest := httptest.NewRequest(http.MethodGet,
		"http://127.0.0.1:14180/edge/v1/items/example%2Fhistory?literal=%252F&space=a%20b&plus=a%2Bb&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=local-example-key%2F20261028%2Fus-east-1%2Fexecute-api%2Faws4_request&X-Amz-Date=20261028T010203Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Security-Token=local-example-token&X-Amz-Signature=d1ef73c1a12e162cb310151ecd9dff19e77b4bb076d3e4a595a1983213aa31bc", nil)
	for name, request := range map[string]*http.Request{"Date": dateRequest, "query": queryRequest} {
		t.Run(name, func(t *testing.T) {
			originalURL, originalHeaders := *request.URL, request.Header.Clone()
			authenticated, status := iamTestVerifierWithToken("local-example-token").authenticate(request)
			require.Zero(t, status)
			require.Equal(t, "local-example-key", authenticated.identity.AccessKey)
			require.NotNil(t, authenticated.credential)
			require.Equal(t, originalURL, *request.URL)
			require.Equal(t, originalHeaders, request.Header)
			if name == "Date" {
				require.Equal(t, []byte{0, 'd', 'a', 't', 'e', 255}, authenticated.body)
			} else {
				require.Empty(t, authenticated.body)
			}
		})
	}
}

func TestIAMVerifierFrozenSmithyRFC3986QueryControl(t *testing.T) {
	// Maintained @smithy/signature-v4, using Node's native crypto through its
	// hash interface. The logical query bag is {q:"+", lower:"/", tilde:"~",
	// literal:"%2F", semicolon:"a;admin=true"}. AWS CRT's URI percent-decode/re-encode model maps these
	// original wire spellings to the same single canonical query.
	request := httptest.NewRequest(http.MethodGet,
		"http://127.0.0.1:14180/edge/v1/items/example%2Fhistory?q=+&lower=%2f&tilde=%7E&literal=%252F&semicolon=a;admin=true", nil)
	request.Header = http.Header{
		"X-Amz-Date":           {"20261028T010203Z"},
		"X-Amz-Security-Token": {"local-example-token"},
		"X-Amz-Content-Sha256": {"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		"Authorization":        {"AWS4-HMAC-SHA256 Credential=local-example-key/20261028/us-east-1/execute-api/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=c004ebd44caa9d7fc2e05a50a9b3a58a3732985c39ef89141efaf1e54dc7ec05"},
	}
	original := request.URL.RawQuery
	authenticated, status := iamTestVerifierWithToken("local-example-token").authenticate(request)
	require.Zero(t, status)
	require.Equal(t, "local-example-key", authenticated.identity.AccessKey)
	require.Equal(t, original, request.URL.RawQuery)
}

func TestIAMSigningDateFormsAndPrecedence(t *testing.T) {
	for _, value := range []string{
		"20261028T010203Z", "Wed, 28 Oct 2026 01:02:03 GMT", "Wed, 28 Oct 2026 01:02:03 -0000",
	} {
		request := iamSDKRequest(t, http.MethodGet, "/items/one", nil, iamTestTime, "")
		request.Header.Del("X-Amz-Date")
		request.Header.Set("Date", value)
		headers, ok := iamRequestHeaders(request.Header)
		require.True(t, ok)
		parsed, name, ok := iamHeaderSigningDate(headers)
		require.True(t, ok, value)
		require.Equal(t, "date", name)
		require.Equal(t, iamTestTime, parsed)
	}
	// The official Go SDK signs this ordinary Date field as well as x-amz-date.
	// A conflicting old Date must not override the authoritative AWS date.
	request := iamSDKRequest(t, http.MethodGet, "/items/one", nil, iamTestTime, "")
	request.Header.Set("Date", iamTestTime.Add(-24*time.Hour).Format(http.TimeFormat))
	hash := sha256.Sum256(nil)
	require.NoError(t, v4.NewSigner().SignHTTP(t.Context(), aws.Credentials{
		AccessKeyID: "local-example-key", SecretAccessKey: "local-example-secret",
	}, request, hex.EncodeToString(hash[:]), "execute-api", "us-east-1", iamTestTime))
	_, status := iamTestVerifier().authenticate(request)
	require.Zero(t, status)

	request = iamSDKRequest(t, http.MethodGet, "/items/one", nil, iamTestTime, "")
	request.Header.Set("Date", iamTestTime.Format(http.TimeFormat))
	request.Header.Set("X-Amz-Date", "invalid")
	tracker := &iamReadTracker{reader: strings.NewReader("")}
	request.Body = tracker
	_, status = iamTestVerifier().authenticate(request)
	require.Equal(t, http.StatusForbidden, status, "present invalid AWS date cannot fall back to Date")
	require.Zero(t, tracker.reads)
}

func TestIAMVerifierSDKQueryAuthenticatesExactBodyAndRequest(t *testing.T) {
	for _, token := range []string{"", "local-example-token"} {
		for _, body := range [][]byte{nil, []byte{0, 'q', 'u', 'e', 'r', 'y', 255}} {
			t.Run(token+"/"+iamBodyName(body), func(t *testing.T) {
				request := iamSDKQueryRequest(t, body, token, "")
				originalURL, originalHeaders := *request.URL, request.Header.Clone()
				authenticated, status := iamTestVerifierWithToken(token).authenticate(request)
				require.Zero(t, status)
				require.True(t, bytes.Equal(body, authenticated.body))
				require.Equal(t, "local-example-key", authenticated.identity.AccessKey)
				require.Equal(t, originalURL, *request.URL)
				require.Equal(t, originalHeaders, request.Header)
			})
		}
	}
}

func TestIAMVerifierQueryMetadataRefusesBeforeBody(t *testing.T) {
	tests := []struct {
		name   string
		change func(*http.Request)
	}{
		{"mixed authorization", func(r *http.Request) { r.Header.Set("Authorization", "AWS4-HMAC-SHA256 invalid") }},
		{"mixed header date", func(r *http.Request) { r.Header.Set("X-Amz-Date", iamTestTime.Format(iamTimeFormat)) }},
		{"mixed token", func(r *http.Request) { r.Header.Set("X-Amz-Security-Token", "local-example-token") }},
		{"duplicate control", func(r *http.Request) { r.URL.RawQuery += "&X-Amz-Date=" + iamTestTime.Format(iamTimeFormat) }},
		{"encoded duplicate control", func(r *http.Request) { r.URL.RawQuery += "&%58-Amz-Date=" + iamTestTime.Format(iamTimeFormat) }},
		{"case ambiguous control", func(r *http.Request) { r.URL.RawQuery += "&x-amz-date=" + iamTestTime.Format(iamTimeFormat) }},
		{"malformed query", func(r *http.Request) { r.URL.RawQuery += "&bad=%GG" }},
		{"missing algorithm", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Algorithm", "") }},
		{"wrong algorithm", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Algorithm", "AWS4-ECDSA-P256-SHA256") }},
		{"missing signature", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Signature", "") }},
		{"bad signature", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Signature", "not-hex") }},
		{"missing date", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Date", "") }},
		{"malformed date", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Date", "2026-10-28") }},
		{"wrong scope", func(r *http.Request) {
			iamMutateQuery(r, "X-Amz-Credential", "local-example-key/20261028/us-east-1/s3/aws4_request")
		}},
		{"unknown key", func(r *http.Request) {
			iamMutateQuery(r, "X-Amz-Credential", "unknown/20261028/us-east-1/execute-api/aws4_request")
		}},
		{"missing token", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Security-Token", "") }},
		{"wrong token", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Security-Token", "wrong-token") }},
		{"missing signed host", func(r *http.Request) { iamMutateQuery(r, "X-Amz-SignedHeaders", "x-proof") }},
		{"zero expiry", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Expires", "0") }},
		{"negative expiry", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Expires", "-1") }},
		{"fractional expiry", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Expires", "1.5") }},
		{"overflow expiry", func(r *http.Request) { iamMutateQuery(r, "X-Amz-Expires", strings.Repeat("9", 30)) }},
		{"unsigned payload", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD") }},
		{"aws chunk signatures", func(r *http.Request) { r.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := iamSDKQueryRequest(t, []byte("payload"), "local-example-token", "60")
			test.change(request)
			tracker := &iamReadTracker{reader: strings.NewReader("payload")}
			request.Body = tracker
			result, status := iamTestVerifierWithToken("local-example-token").authenticate(request)
			require.Equal(t, http.StatusForbidden, status)
			require.Zero(t, result)
			require.Zero(t, tracker.reads)
		})
	}
}

func iamMutateQuery(request *http.Request, name, value string) {
	query := request.URL.Query()
	if value == "" {
		query.Del(name)
	} else {
		query.Set(name, value)
	}
	request.URL.RawQuery = query.Encode()
}

func TestIAMVerifierQueryTamperingAndConservativeExpiry(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*http.Request)
	}{
		{"method", func(r *http.Request) { r.Method = http.MethodDelete }},
		{"path", func(r *http.Request) { r.URL.Path = "/edge/v1/items/other"; r.URL.RawPath = "" }},
		{"query", func(r *http.Request) { r.URL.RawQuery += "&changed=true" }},
		{"header", func(r *http.Request) { r.Header.Set("X-Proof", "changed") }},
		{"body", func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader("changed")) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := iamSDKQueryRequest(t, []byte("payload"), "", "60")
			test.change(request)
			_, status := iamTestVerifier().authenticate(request)
			require.Equal(t, http.StatusForbidden, status)
		})
	}
	for _, expires := range []int{1, 60, 300, 3600} {
		t.Run("expiry-"+strconv.Itoa(expires), func(t *testing.T) {
			verifier := iamTestVerifier()
			bound := time.Duration(min(expires, 300)) * time.Second
			for _, elapsed := range []time.Duration{bound, bound + time.Nanosecond} {
				verifier.now = func() time.Time { return iamTestTime.Add(elapsed) }
				request := iamSDKQueryRequest(t, nil, "", strconv.Itoa(expires))
				_, status := verifier.authenticate(request)
				if elapsed == bound {
					require.Zero(t, status)
				} else {
					require.Equal(t, http.StatusForbidden, status)
				}
			}
		})
	}
}

type iamAdvanceClockReader struct {
	reader  io.Reader
	advance func()
}

func (reader *iamAdvanceClockReader) Read(data []byte) (int, error) {
	reader.advance()
	return reader.reader.Read(data)
}

func TestIAMVerifierFencesCredentialExpiryAfterBodyRead(t *testing.T) {
	for _, query := range []bool{false, true} {
		t.Run(strconv.FormatBool(query), func(t *testing.T) {
			credential := iamCredential{secretAccessKey: "local-example-secret", expiresAt: iamTestTime.Add(time.Second),
				identity: iamIdentity{AccountID: "111122223333", UserARN: "arn:aws:iam::111122223333:user/worker"}}
			verifier := newIAMVerifier("us-east-1", map[string]iamCredential{"local-example-key": credential})
			now := iamTestTime
			verifier.now = func() time.Time { return now }
			request := iamSDKRequest(t, http.MethodPost, "/items/one", []byte("payload"), iamTestTime, "")
			if query {
				request = iamSDKQueryRequest(t, []byte("payload"), "", "")
			}
			tracker := &iamReadTracker{reader: &iamAdvanceClockReader{reader: strings.NewReader("payload"), advance: func() { now = iamTestTime.Add(2 * time.Second) }}}
			request.Body = tracker
			result, status := verifier.authenticate(request)
			require.Equal(t, http.StatusForbidden, status)
			require.Zero(t, result)
			require.Positive(t, tracker.reads, "credential was valid before hashing and expired during owned body read")

			now = iamTestTime.Add(2 * time.Second)
			request = iamSDKQueryRequest(t, []byte("payload"), "", "")
			tracker = &iamReadTracker{reader: strings.NewReader("payload")}
			request.Body = tracker
			_, status = verifier.authenticate(request)
			require.Equal(t, http.StatusForbidden, status)
			require.Zero(t, tracker.reads, "already expired credentials refuse before body read")
		})
	}
}
