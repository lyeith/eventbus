package gateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func testLogger(buffer *bytes.Buffer) zerolog.Logger { return zerolog.New(buffer) }
func TestPolicyStringArraysAndDenyPrecedence(t *testing.T) {
	arn := "arn:aws:execute-api:us-east-1:123456789012:api/dev/GET/a/b"
	for _, tc := range []struct {
		name       string
		statements any
		want       int
	}{
		{"glob spans slashes", map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": "arn:aws:execute-api:*:*:api/dev/GET/*"}, 0},
		{"question wildcard", map[string]any{"Effect": "Allow", "Action": []string{"execute-api:*"}, "Resource": []string{strings.TrimSuffix(arn, "b") + "?"}}, 0},
		{"deny last", []any{map[string]any{"Effect": "Allow", "Action": "*", "Resource": "*"}, map[string]any{"Effect": "Deny", "Action": "execute-api:Invoke", "Resource": arn}}, 403},
		{"deny first", []any{map[string]any{"Effect": "Deny", "Action": "*", "Resource": arn}, map[string]any{"Effect": "Allow", "Action": "*", "Resource": "*"}}, 403},
		{"wrong method", map[string]any{"Effect": "Allow", "Action": "execute-api:Invoke", "Resource": strings.Replace(arn, "GET", "POST", 1)}, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, _ := json.Marshal(map[string]any{"principalId": "p", "policyDocument": map[string]any{"Version": "2012-10-17", "Statement": tc.statements}})
			response, err := parseAuthorizerResponse(data)
			if err != nil {
				t.Fatal(err)
			}
			if status := policyStatus(response, arn); status != tc.want {
				t.Fatalf("status=%d", status)
			}
		})
	}
}
func TestUnsupportedPolicySemanticsAndLimits(t *testing.T) {
	for _, extra := range []string{`"Condition":{}`, `"NotAction":"execute-api:Invoke"`, `"NotResource":"*"`, `"Principal":"*"`} {
		t.Run(extra, func(t *testing.T) {
			data := []byte(`{"principalId":"p","policyDocument":{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*",` + extra + `}]}}`)
			if _, err := parseAuthorizerResponse(data); err == nil {
				t.Fatal("unsupported authorization semantics accepted")
			}
		})
	}
	data, _ := json.Marshal(allowResponse(strings.Repeat("a", 513), nil))
	if _, err := parseAuthorizerResponse(data); err == nil {
		t.Fatal("oversized policy resource accepted")
	}
	data, _ = json.Marshal(allowResponse("${aws:username}", nil))
	if _, err := parseAuthorizerResponse(data); err == nil {
		t.Fatal("unsupported policy variables accepted")
	}
	authorizer := lambdaAuthorizer{}
	if _, status := authorizer.authorize(t.Context(), requestEvent{MethodARN: strings.Repeat("a", 1601)}); status != http.StatusRequestURITooLong {
		t.Fatalf("long ARN status=%d", status)
	}
}
func TestConfigurationRejectsUnsafeOrUnsupportedFixtures(t *testing.T) {
	for _, mutation := range []func(*Config){
		func(cfg *Config) {
			auth := cfg.Authorizers["auth"]
			auth.TTL = intPointer(3601)
			cfg.Authorizers["auth"] = auth
		},
		func(cfg *Config) { auth := cfg.Authorizers["auth"]; auth.TTL = nil; cfg.Authorizers["auth"] = auth },
		func(cfg *Config) {
			auth := cfg.Authorizers["auth"]
			auth.Type = "TOKEN"
			cfg.Authorizers["auth"] = auth
		},
		func(cfg *Config) { cfg.Routes[0].Integration.URI = "http://{host}/" },
		func(cfg *Config) {
			cfg.Routes[0].Integration.RequestParameters = map[string]string{"integration.request.header.Host": "'evil'"}
		},
		func(cfg *Config) {
			cfg.Routes[0].Integration.RequestParameters = map[string]string{"integration.request.path.proxy": "method.request.path.absent"}
		},
		func(cfg *Config) { cfg.Routes[0].Path = "/api/{proxy+}/bad" },
	} {
		cfg := fixture("http://lambda/2015-03-31/functions/auth/invocations", "http://backend")
		mutation(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Fatal("invalid fixture accepted")
		}
	}
}
