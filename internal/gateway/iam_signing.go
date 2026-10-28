package gateway

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type iamSigningRequest struct {
	authorization iamAuthorization
	signedAt      time.Time
	dateHeader    string
	token         string
	query         url.Values
	validity      time.Duration
	presigned     bool
}

// Header and query authentication share one canonical request and signature
// verifier. Only their authentication metadata location and signed date differ.
func parseIAMSigningRequest(headers map[string][]string, rawQuery string) (iamSigningRequest, bool) {
	var result iamSigningRequest
	query, ok := parseIAMQuery(rawQuery)
	if !ok {
		return result, false
	}
	result.query, result.validity = query, 5*time.Minute
	hasQueryAuth := false
	for name, values := range query {
		if canonical := iamQueryControlName(name); canonical != "" {
			if name != canonical || len(values) != 1 || values[0] == "" {
				return iamSigningRequest{}, false
			}
			hasQueryAuth = true
		}
	}
	if _, present := headers["authorization"]; present {
		if hasQueryAuth {
			return iamSigningRequest{}, false
		}
		authorization, ok := iamSingleHeader(headers, "authorization", true)
		if !ok {
			return iamSigningRequest{}, false
		}
		result.authorization, ok = parseIAMAuthorization(authorization)
		if !ok {
			return iamSigningRequest{}, false
		}
		result.signedAt, result.dateHeader, ok = iamHeaderSigningDate(headers)
		if !ok {
			return iamSigningRequest{}, false
		}
		result.token, ok = iamSingleHeader(headers, "x-amz-security-token", false)
		return result, ok
	}
	// A second header source for query credentials/date is ambiguous. The Date
	// header can still be ordinary signed request metadata, not the query date.
	if _, present := headers["x-amz-date"]; present {
		return iamSigningRequest{}, false
	}
	if _, present := headers["x-amz-security-token"]; present {
		return iamSigningRequest{}, false
	}
	if iamQueryValue(query, "X-Amz-Algorithm") != iamSigningAlgorithm {
		return iamSigningRequest{}, false
	}
	result.authorization, ok = parseIAMCredentialFields(
		iamQueryValue(query, "X-Amz-Credential"), iamQueryValue(query, "X-Amz-SignedHeaders"), iamQueryValue(query, "X-Amz-Signature"))
	if !ok {
		return iamSigningRequest{}, false
	}
	var err error
	result.signedAt, err = time.Parse(iamTimeFormat, iamQueryValue(query, "X-Amz-Date"))
	if err != nil {
		return iamSigningRequest{}, false
	}
	if expires, present := query["X-Amz-Expires"]; present {
		// A local bound: an explicit expiry may shorten, never extend, the
		// existing execute-api freshness window. S3's longer lifetime is not
		// assumed to be an API Gateway service contract.
		seconds, err := strconv.ParseUint(expires[0], 10, 64)
		if err != nil || seconds == 0 {
			return iamSigningRequest{}, false
		}
		for _, character := range expires[0] {
			if character < '0' || character > '9' {
				return iamSigningRequest{}, false
			}
		}
		result.validity = time.Duration(min(seconds, 300)) * time.Second
	}
	result.token, result.presigned = iamQueryValue(query, "X-Amz-Security-Token"), true
	// The signature is the only authentication query parameter excluded from
	// its own canonical request. This owned map never rewrites native RawQuery.
	delete(query, "X-Amz-Signature")
	return result, true
}

func iamQueryControlName(name string) string {
	switch strings.ToLower(name) {
	case "x-amz-algorithm":
		return "X-Amz-Algorithm"
	case "x-amz-credential":
		return "X-Amz-Credential"
	case "x-amz-date":
		return "X-Amz-Date"
	case "x-amz-expires":
		return "X-Amz-Expires"
	case "x-amz-signedheaders":
		return "X-Amz-SignedHeaders"
	case "x-amz-signature":
		return "X-Amz-Signature"
	case "x-amz-security-token":
		return "X-Amz-Security-Token"
	}
	return ""
}
func iamQueryValue(query url.Values, name string) string {
	if values := query[name]; len(values) == 1 {
		return values[0]
	}
	return ""
}

// Unlike form decoding, RFC3986 percent decoding preserves literal '+' bytes.
// AWS CRT canonical query construction applies this decode/re-encode model.
func parseIAMQuery(raw string) (url.Values, bool) {
	query := make(url.Values)
	for _, pair := range strings.Split(raw, "&") {
		if pair == "" {
			continue
		}
		if strings.ContainsAny(pair, "\x00\r\n") {
			return nil, false
		}
		name, value, _ := strings.Cut(pair, "=")
		name, nameErr := url.PathUnescape(name)
		value, valueErr := url.PathUnescape(value)
		if nameErr != nil || valueErr != nil {
			return nil, false
		}
		query[name] = append(query[name], value)
	}
	return query, true
}

func iamHeaderSigningDate(headers map[string][]string) (time.Time, string, bool) {
	if _, present := headers["x-amz-date"]; present {
		value, ok := iamSingleHeader(headers, "x-amz-date", true)
		if !ok {
			return time.Time{}, "", false
		}
		parsed, err := time.Parse(iamTimeFormat, value)
		return parsed, "x-amz-date", err == nil
	}
	value, ok := iamSingleHeader(headers, "date", true)
	if !ok {
		return time.Time{}, "", false
	}
	for _, format := range []string{iamTimeFormat, time.RFC1123Z, http.TimeFormat, time.RFC850, time.ANSIC} {
		parsed, err := time.Parse(format, value)
		if err == nil {
			_, offset := parsed.Zone()
			if offset == 0 {
				return parsed.UTC(), "date", true
			}
		}
	}
	return time.Time{}, "", false
}
