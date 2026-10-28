package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/smithy-go/encoding/httpbinding"
)

const maxIAMRequestBody = 10 * 1024 * 1024
const iamSigningAlgorithm = "AWS4-HMAC-SHA256"
const iamTimeFormat = "20060102T150405Z"

// Only a successfully verified credential lookup can supply this identity.
// Gateway account metadata and REQUEST-authorizer context have separate owners.
type iamIdentity struct {
	AccessKey string `json:"accessKey"`
	AccountID string `json:"accountId"`
	CallerID  string `json:"callerId,omitempty"`
	UserARN   string `json:"userArn"`
	UserID    string `json:"userId,omitempty"`
}

type iamCredential struct {
	secretAccessKey string
	sessionToken    string
	identity        iamIdentity
}

type iamVerifier struct {
	region      string
	credentials map[string]iamCredential
	now         func() time.Time
}

func newIAMVerifier(region string, credentials map[string]iamCredential) *iamVerifier {
	owned := make(map[string]iamCredential, len(credentials))
	for accessKey, credential := range credentials {
		credential.identity.AccessKey = accessKey
		owned[accessKey] = credential
	}
	return &iamVerifier{region: region, credentials: owned, now: time.Now}
}

type iamAuthorization struct {
	accessKey, date, region string
	signedHeaders           []string
	signature               []byte
}

// authenticate consumes the original request body only after validating its
// authentication metadata. The caller owns body closure and reuses the returned
// bytes for integration. Headers, URL and native request metadata are untouched.
func (verifier *iamVerifier) authenticate(request *http.Request) (iamIdentity, []byte, int) {
	denied := func() (iamIdentity, []byte, int) { return iamIdentity{}, nil, http.StatusForbidden }
	if verifier == nil || request == nil || request.URL == nil || request.URL.Opaque != "" {
		return denied()
	}
	headers, ok := iamRequestHeaders(request.Header)
	if !ok {
		return denied()
	}
	authorization, ok := iamSingleHeader(headers, "authorization", true)
	if !ok {
		return denied()
	}
	parsed, ok := parseIAMAuthorization(authorization)
	if !ok || parsed.region != verifier.region {
		return denied()
	}
	credential, ok := verifier.credentials[parsed.accessKey]
	if !ok || credential.secretAccessKey == "" {
		return denied()
	}
	date, ok := iamSingleHeader(headers, "x-amz-date", true)
	if !ok {
		return denied()
	}
	signedAt, err := time.Parse(iamTimeFormat, date)
	if err != nil || parsed.date != signedAt.UTC().Format("20060102") {
		return denied()
	}
	now := verifier.now()
	if signedAt.Before(now.Add(-5*time.Minute)) || signedAt.After(now.Add(5*time.Minute)) {
		return denied()
	}
	token, ok := iamSingleHeader(headers, "x-amz-security-token", false)
	if !ok || !hmac.Equal([]byte(token), []byte(credential.sessionToken)) {
		return denied()
	}
	payloadHash, ok := iamSingleHeader(headers, "x-amz-content-sha256", false)
	if !ok || payloadHash != "" && !iamSHA256Text(payloadHash) {
		return denied()
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return denied()
	}
	// Header authentication is the supported contract. Mixed/presigned control
	// parameters must never be interpreted as application credential overrides.
	for name := range query {
		switch strings.ToLower(name) {
		case "x-amz-algorithm", "x-amz-credential", "x-amz-date", "x-amz-expires",
			"x-amz-signedheaders", "x-amz-signature", "x-amz-security-token":
			return denied()
		}
	}
	path := request.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") || strings.ContainsAny(path, "\x00\r\n") {
		return denied()
	}
	if request.URL.RawPath != "" {
		decoded, err := url.PathUnescape(request.URL.RawPath)
		if err != nil || decoded != request.URL.Path {
			return denied()
		}
	}
	canonicalHeaders, ok := iamCanonicalHeaders(request, headers, parsed.signedHeaders, credential.sessionToken != "")
	if !ok {
		return denied()
	}
	if request.ContentLength > maxIAMRequestBody {
		return iamIdentity{}, nil, http.StatusRequestEntityTooLarge
	}
	var body []byte
	if request.Body != nil {
		body, err = io.ReadAll(io.LimitReader(request.Body, maxIAMRequestBody+1))
		if err != nil {
			return iamIdentity{}, nil, http.StatusBadRequest
		}
	}
	if len(body) > maxIAMRequestBody {
		return iamIdentity{}, nil, http.StatusRequestEntityTooLarge
	}
	if request.ContentLength >= 0 && int64(len(body)) != request.ContentLength {
		return iamIdentity{}, nil, http.StatusBadRequest
	}
	digest := sha256.Sum256(body)
	actualHash := hex.EncodeToString(digest[:])
	if payloadHash != "" && !hmac.Equal([]byte(payloadHash), []byte(actualHash)) {
		return denied()
	}
	canonicalRequest := strings.Join([]string{
		request.Method,
		httpbinding.EscapePath(path, false),
		iamCanonicalQuery(query),
		canonicalHeaders,
		strings.Join(parsed.signedHeaders, ";"),
		actualHash,
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	scope := parsed.date + "/" + parsed.region + "/execute-api/aws4_request"
	stringToSign := iamSigningAlgorithm + "\n" + date + "\n" + scope + "\n" + hex.EncodeToString(canonicalHash[:])
	key := iamHMAC([]byte("AWS4"+credential.secretAccessKey), parsed.date)
	key = iamHMAC(key, parsed.region)
	key = iamHMAC(key, "execute-api")
	key = iamHMAC(key, "aws4_request")
	if !hmac.Equal(parsed.signature, iamHMAC(key, stringToSign)) {
		return denied()
	}
	return credential.identity, body, 0
}

func parseIAMAuthorization(value string) (iamAuthorization, bool) {
	var result iamAuthorization
	algorithm, fields, found := strings.Cut(value, " ")
	if !found || algorithm != iamSigningAlgorithm {
		return result, false
	}
	parts := strings.Split(fields, ",")
	if len(parts) != 3 {
		return result, false
	}
	values := make(map[string]string, 3)
	for _, part := range parts {
		name, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found || value == "" || values[name] != "" {
			return result, false
		}
		switch name {
		case "Credential", "SignedHeaders", "Signature":
			values[name] = value
		default:
			return result, false
		}
	}
	scope := strings.Split(values["Credential"], "/")
	if len(scope) != 5 || scope[0] == "" || len(scope[1]) != 8 || scope[3] != "execute-api" || scope[4] != "aws4_request" {
		return result, false
	}
	result.accessKey, result.date, result.region = scope[0], scope[1], scope[2]
	for index, name := range strings.Split(values["SignedHeaders"], ";") {
		if !validHeaderName(name) || strings.ToLower(name) != name || name == "authorization" ||
			index > 0 && name <= result.signedHeaders[index-1] {
			return iamAuthorization{}, false
		}
		result.signedHeaders = append(result.signedHeaders, name)
	}
	if !iamSHA256Text(values["Signature"]) {
		return iamAuthorization{}, false
	}
	result.signature, _ = hex.DecodeString(values["Signature"])
	return result, true
}

// Parse incoming header names once without copying or changing their values.
// net/http preserves repeated values under one canonical key. Case-variant map
// keys are ambiguous synthetic input and must not acquire an identity.
func iamRequestHeaders(header http.Header) (map[string][]string, bool) {
	normalized := make(map[string][]string, len(header))
	for name, values := range header {
		lower := strings.ToLower(name)
		if _, present := normalized[lower]; !validHeaderName(name) || present {
			return nil, false
		}
		if lower == "content-length" && len(values) != 1 {
			return nil, false
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\x00\r\n") {
				return nil, false
			}
		}
		normalized[lower] = values
	}
	return normalized, true
}

func iamSingleHeader(headers map[string][]string, name string, required bool) (string, bool) {
	values, present := headers[name]
	if !present {
		return "", !required
	}
	if len(values) != 1 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func iamCanonicalHeaders(request *http.Request, headers map[string][]string, names []string, session bool) (string, bool) {
	var result strings.Builder
	host, date, token := false, false, false
	for _, name := range names {
		var values []string
		switch name {
		case "host":
			host = true
			if request.Host == "" || strings.ContainsAny(request.Host, " \t\x00\r\n") {
				return "", false
			}
			if _, present := headers[name]; present {
				return "", false // Host belongs to Request.Host on incoming HTTP.
			}
			values = []string{request.Host}
		case "content-length":
			var present bool
			values, present = headers[name]
			if !present {
				if request.ContentLength < 0 {
					return "", false
				}
				values = []string{strconv.FormatInt(request.ContentLength, 10)}
			}
			if len(values) != 1 {
				return "", false
			}
			length, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil || length < 0 || request.ContentLength >= 0 && length != request.ContentLength {
				return "", false
			}
		default:
			var present bool
			values, present = headers[name]
			if !present || len(values) == 0 {
				return "", false
			}
		}
		date = date || name == "x-amz-date"
		token = token || name == "x-amz-security-token"
		result.WriteString(name)
		result.WriteByte(':')
		for index, value := range values {
			for _, character := range value {
				if character < ' ' && character != '\t' || character == 127 {
					return "", false
				}
			}
			if index != 0 {
				result.WriteByte(',')
			}
			result.WriteString(strings.Join(strings.FieldsFunc(value, func(character rune) bool {
				return character == ' ' || character == '\t'
			}), " "))
		}
		result.WriteByte('\n')
	}
	return result.String(), host && date && (!session || token)
}

func iamCanonicalQuery(query url.Values) string {
	type pair struct{ name, value string }
	var pairs []pair
	for name, values := range query {
		name = httpbinding.EscapePath(name, true)
		for _, value := range values {
			pairs = append(pairs, pair{name, httpbinding.EscapePath(value, true)})
		}
	}
	sort.Slice(pairs, func(left, right int) bool {
		if pairs[left].name != pairs[right].name {
			return pairs[left].name < pairs[right].name
		}
		return pairs[left].value < pairs[right].value
	})
	var result strings.Builder
	for index, pair := range pairs {
		if index != 0 {
			result.WriteByte('&')
		}
		result.WriteString(pair.name)
		result.WriteByte('=')
		result.WriteString(pair.value)
	}
	return result.String()
}

func iamSHA256Text(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func iamHMAC(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write([]byte(value))
	return hash.Sum(nil)
}
