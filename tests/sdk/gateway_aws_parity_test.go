//go:build awsparity

package sdk

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/smithy-go/logging"
	"github.com/lyeith/eventbus/internal/localexec"
)

const awsParityPrefix = "/__eventbus_aws_parity__/"
const awsParityMarker = "echo-v1"

type awsParityConfig struct {
	profile, region, output, python string
	endpoints                       []awsParityEndpoint
}

type awsParityEndpoint struct {
	name string
	url  *url.URL
}

func readAWSParityConfig(getenv func(string) string) (awsParityConfig, bool, error) {
	var cfg awsParityConfig
	if getenv("EVENTBUS_AWS_PARITY") == "" {
		return cfg, false, nil
	}
	if getenv("EVENTBUS_AWS_PARITY") != "1" {
		return cfg, false, errors.New("EVENTBUS_AWS_PARITY must be 1 or absent")
	}
	cfg.profile = getenv("EVENTBUS_AWS_PARITY_PROFILE")
	cfg.region = getenv("EVENTBUS_AWS_PARITY_REGION")
	cfg.output = getenv("EVENTBUS_AWS_PARITY_OUTPUT")
	cfg.python = getenv("EVENTBUS_AWS_PARITY_PYTHON")
	if err := awsParityValidatePython(cfg.python); err != nil {
		return cfg, true, err
	}
	if cfg.profile == "" || strings.TrimSpace(cfg.profile) != cfg.profile {
		return cfg, true, errors.New("set an explicit EVENTBUS_AWS_PARITY_PROFILE")
	}
	if !regexp.MustCompile("^[a-z0-9][a-z0-9-]*$").MatchString(cfg.region) {
		return cfg, true, errors.New("set an explicit EVENTBUS_AWS_PARITY_REGION")
	}
	if !filepath.IsAbs(cfg.output) || filepath.Clean(cfg.output) != cfg.output {
		return cfg, true, errors.New("EVENTBUS_AWS_PARITY_OUTPUT must be a new absolute file path")
	}
	for _, kind := range []struct{ name, variable string }{
		{"http-v2", "EVENTBUS_AWS_PARITY_HTTP_V2_URL"},
		{"rest-v1", "EVENTBUS_AWS_PARITY_REST_URL"},
	} {
		raw := getenv(kind.variable)
		if raw == "" {
			continue
		}
		endpoint, err := url.Parse(raw)
		if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" ||
			endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery ||
			endpoint.Fragment != "" || endpoint.Opaque != "" ||
			endpoint.Path != endpoint.EscapedPath() ||
			!strings.HasSuffix(endpoint.Path, awsParityPrefix) {
			return cfg, true, fmt.Errorf("%s must be an HTTPS echo prefix ending %s without escapes, credentials, query or fragment", kind.variable, awsParityPrefix)
		}
		for _, part := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(endpoint.Path, "/"), "/"), "/") {
			if part == "" || part == "." || part == ".." {
				return cfg, true, errors.New("echo prefix must have literal nonempty path components")
			}
		}
		cfg.endpoints = append(cfg.endpoints, awsParityEndpoint{kind.name, endpoint})
	}
	if len(cfg.endpoints) == 0 {
		return cfg, true, errors.New("set EVENTBUS_AWS_PARITY_HTTP_V2_URL and/or EVENTBUS_AWS_PARITY_REST_URL")
	}
	return cfg, true, nil
}

func awsParityValidatePython(python string) error {
	if python == "" {
		return nil
	}
	info, err := os.Stat(python)
	if !filepath.IsAbs(python) || err != nil || !info.Mode().IsRegular() {
		return errors.New("EVENTBUS_AWS_PARITY_PYTHON must select an absolute existing Python interpreter")
	}
	return nil
}

// Explicit profile selection takes precedence over environment keys in the pinned
// SDK resolver. Reject provider-less/source-environment profiles before the SDK's
// credential chain can fall through to ECS or IMDS. SDK owns SSO/role mechanics.
func awsParityProfileHasProvider(profile *config.SharedConfig) bool {
	for depth := 0; profile != nil && depth < 20; depth++ {
		if profile.CredentialSource != "" {
			return false
		}
		if profile.Source != nil {
			profile = profile.Source
			continue
		}
		return profile.Credentials.HasKeys() || profile.CredentialProcess != "" ||
			profile.WebIdentityTokenFile != "" || profile.SSOSession != nil ||
			(profile.SSOStartURL != "" && profile.SSOAccountID != "" && profile.SSORoleName != "") ||
			profile.LoginSession != ""
	}
	return false
}

func awsParityCredentials(ctx context.Context, cfg awsParityConfig, client *http.Client, configFiles, credentialFiles []string) (aws.Credentials, error) {
	shared, err := config.LoadSharedConfigProfile(ctx, cfg.profile, func(options *config.LoadSharedConfigOptions) {
		options.ConfigFiles, options.CredentialsFiles = configFiles, credentialFiles
	})
	if err != nil || !awsParityProfileHasProvider(&shared) {
		return aws.Credentials{}, errors.New("explicit profile must declare credentials, SSO, login, process or a source-profile chain; ambient credential_source is unsupported")
	}
	loaded, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(cfg.profile), config.WithRegion(cfg.region),
		config.WithSharedConfigFiles(configFiles), config.WithSharedCredentialsFiles(credentialFiles),
		config.WithEC2IMDSClientEnableState(imds.ClientDisabled),
		config.WithHTTPClient(client), config.WithRetryMaxAttempts(1),
		config.WithLogger(logging.NewStandardLogger(io.Discard)),
		config.WithLogConfigurationWarnings(false),
	)
	if err != nil {
		return aws.Credentials{}, errors.New("explicit AWS profile configuration failed")
	}
	value, err := loaded.Credentials.Retrieve(ctx)
	if err != nil || !value.HasKeys() {
		// Profile/process/provider errors can contain private output. Never print them.
		return aws.Credentials{}, errors.New("explicit AWS profile credential retrieval failed")
	}
	return value, nil
}

type awsParityCase struct{ name, suffix string }

var awsParityCases = []awsParityCase{
	{"structural-slash", "items/Type/example/history"},
	{"encoded-slash", "items/Type/example%2Fhistory"},
	{"lowercase-slash", "items/Type/example%2fhistory"},
	{"encoded-backslash", "items/Type/example%5Chistory"},
	{"double-escape", "items/Type/example%252Fhistory"},
	{"literal-percent", "items/Type/percent%25value"},
	{"space", "items/Type/example%20history"},
	{"unicode", "items/Type/%E9%9B%AA"},
	{"dot-segment", "items/a/../history"},
	{"encoded-dot", "items/a/%2e%2e/history"},
	{"repeated-slash", "items/Type//history"},
	{"query-order", "items/query?z=last&a=two&a=one&empty=&bare"},
	{"query-plus", "items/query?value=a+b&value=a%20b&literal=a%2Bb"},
	{"query-escapes", "items/query?slash=%2f&slash=%2F&letter=%41&unicode=%E9%9B%AA"},
}

// Decode only fields eligible for the observation. The native echo includes
// credentials in headers; unknown JSON fields (including headers/body) are ignored.
type awsParityEvent struct {
	Version        string            `json:"version"`
	RouteKey       string            `json:"routeKey"`
	Resource       string            `json:"resource"`
	Path           string            `json:"path"`
	RawPath        string            `json:"rawPath"`
	RawQueryString string            `json:"rawQueryString"`
	HTTPMethod     string            `json:"httpMethod"`
	PathParameters map[string]string `json:"pathParameters"`
	RequestContext struct {
		RequestID string `json:"requestId"`
		Path      string `json:"path"`
		HTTP      struct {
			Method string `json:"method"`
			Path   string `json:"path"`
		} `json:"http"`
		Identity struct {
			UserARN string `json:"userArn"`
		} `json:"identity"`
		Authorizer struct {
			IAM struct {
				UserARN string `json:"userArn"`
			} `json:"iam"`
		} `json:"authorizer"`
	} `json:"requestContext"`
}

type awsParityObservation struct {
	Endpoint        string            `json:"endpoint"`
	Case            string            `json:"case"`
	Mode            string            `json:"mode"`
	Signer          string            `json:"signer"`
	Signature       string            `json:"signature"`
	RequestedPath   string            `json:"requestedPath"`
	WirePath        string            `json:"wirePath"`
	RequestedQuery  string            `json:"requestedQuery,omitempty"`
	WireQuery       string            `json:"wireQuery,omitempty"`
	Status          int               `json:"status"`
	Result          string            `json:"result"`
	ErrorClass      string            `json:"errorClass,omitempty"`
	RawPath         string            `json:"rawPath,omitempty"`
	Path            string            `json:"path,omitempty"`
	ContextPath     string            `json:"contextPath,omitempty"`
	ContextHTTPPath string            `json:"contextHTTPPath,omitempty"`
	Route           string            `json:"route,omitempty"`
	Captures        map[string]string `json:"captures,omitempty"`
	EventQuery      string            `json:"eventQuery,omitempty"`
	IdentityARN     string            `json:"identityArn,omitempty"`
}

// Returned values must originate from this runner's synthetic input. Unknown
// fixture output is represented by a constant rather than persisted verbatim.
func awsParitySafeCapture(value, suffix string) string {
	requestPath, _, _ := strings.Cut(suffix, "?")
	if value == "" {
		return ""
	}
	for decode := 0; decode < 3; decode++ {
		if strings.Contains(requestPath, value) || strings.Contains(path.Clean("/"+requestPath), value) {
			return value
		}
		next, err := url.PathUnescape(requestPath)
		if err != nil || next == requestPath {
			break
		}
		requestPath = next
	}
	return "<unexpected>"
}

func awsParitySafePath(value, suffix string) string {
	if value == "" {
		return ""
	}
	if start := strings.Index(value, awsParityPrefix); start >= 0 {
		tail := value[start+len(awsParityPrefix):]
		if safe := awsParitySafeCapture(tail, suffix); safe != "<unexpected>" {
			return "<fixture>/" + safe
		}
	}
	if strings.HasPrefix(value, "/") {
		if safe := awsParitySafeCapture(strings.TrimPrefix(value, "/"), suffix); safe != "<unexpected>" {
			return "/" + safe
		}
	}
	return "<unexpected>"
}

func awsParitySafeRoute(value string) string {
	if value == "" {
		return ""
	}
	// Only fixture routing templates are useful here, never arbitrary app routes.
	for _, suffix := range []string{"{proxy+}", "{id}", "control", "items/Type/{id}", "items/Type/example/history"} {
		if value == awsParityPrefix+suffix || value == "GET "+awsParityPrefix+suffix ||
			value == "ANY "+awsParityPrefix+suffix {
			return strings.Replace(value, awsParityPrefix, "<fixture>/", 1)
		}
	}
	if value == "$default" {
		return value
	}
	return "<unexpected>"
}

func awsParitySafeARN(value string) string {
	if value == "" {
		return ""
	}
	parts := strings.SplitN(value, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[3] != "" ||
		!regexp.MustCompile("^aws(-cn|-us-gov)?$").MatchString(parts[1]) ||
		!regexp.MustCompile("^[0-9]{12}$").MatchString(parts[4]) {
		return "<unexpected>"
	}
	if parts[2] == "iam" && parts[5] == "root" {
		return "arn:" + parts[1] + ":iam::<account>:root"
	}
	kind, _, _ := strings.Cut(parts[5], "/")
	if (parts[2] == "iam" && (kind == "user" || kind == "role")) ||
		(parts[2] == "sts" && (kind == "assumed-role" || kind == "federated-user")) {
		return "arn:" + parts[1] + ":" + parts[2] + "::<account>:" + kind + "/<principal>"
	}
	return "<unexpected>"
}

func awsParitySanitize(event awsParityEvent, suffix string, observation *awsParityObservation) {
	observation.RawPath = awsParitySafePath(event.RawPath, suffix)
	observation.Path = awsParitySafePath(event.Path, suffix)
	observation.ContextPath = awsParitySafePath(event.RequestContext.Path, suffix)
	observation.ContextHTTPPath = awsParitySafePath(event.RequestContext.HTTP.Path, suffix)
	observation.Route = awsParitySafeRoute(event.RouteKey)
	if event.RouteKey == "" {
		observation.Route = awsParitySafeRoute(event.Resource)
	}
	for key, value := range event.PathParameters {
		// Expected echo routes have these names. Do not persist arbitrary keys.
		if key == "proxy" || key == "id" {
			if observation.Captures == nil {
				observation.Captures = make(map[string]string)
			}
			observation.Captures[key] = awsParitySafeCapture(value, suffix)
		}
	}
	_, query, _ := strings.Cut(suffix, "?")
	if event.RawQueryString == observation.WireQuery || event.RawQueryString == query {
		observation.EventQuery = event.RawQueryString
	} else if event.RawQueryString != "" {
		observation.EventQuery = "<unexpected>"
	}
	identity := event.RequestContext.Authorizer.IAM.UserARN
	if identity == "" {
		identity = event.RequestContext.Identity.UserARN
	}
	observation.IdentityARN = awsParitySafeARN(identity)
}

func awsParityOpenOutput(path string) (*os.File, error) {
	// O_EXCL also refuses symlinks and existing files; never truncate evidence.
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

// Fixed observer modes differ only in signing/query serialization. They do not
// select credentials or targets; both are supplied by the explicit live owner.
type awsParitySigner struct {
	name, mode string
	sign       func(context.Context, *http.Request) error
}

func awsParityGoSigner(credentials aws.Credentials, region string, originalQuery bool) awsParitySigner {
	mode := "serialized-query"
	if originalQuery {
		mode = "original-query"
	}
	return awsParitySigner{"aws-sdk-go-v2/v4", mode, func(ctx context.Context, request *http.Request) error {
		original := request.URL.RawQuery
		hash := sha256.Sum256(nil)
		err := v4.NewSigner().SignHTTP(ctx, credentials, request, hex.EncodeToString(hash[:]), "execute-api", region, time.Now())
		if originalQuery {
			request.URL.RawQuery = original
		}
		return err
	}}
}

// The explicitly selected interpreter never loads an AWS session/client/provider.
// Only these headers return through bounded memory; no child output is logged.
const awsParityBotocoreScript = `
import json, sys
import botocore
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
if botocore.__version__ != "1.40.76":
    raise RuntimeError("pinned botocore 1.40.76 required")
data = json.load(sys.stdin)
request = AWSRequest(method="GET", url=data["url"])
SigV4Auth(Credentials(data["key"], data["secret"], data["token"] or None), "execute-api", data["region"]).add_auth(request)
json.dump({"authorization": request.headers["Authorization"], "date": request.headers["X-Amz-Date"], "token": request.headers.get("X-Amz-Security-Token", "")}, sys.stdout)
`

func awsParityBotocoreSigner(python string, credentials aws.Credentials, region string) awsParitySigner {
	return awsParitySigner{"botocore/1.40.76/SigV4Auth", "original-query", func(ctx context.Context, request *http.Request) error {
		input, err := json.Marshal(map[string]string{"url": request.URL.String(), "region": region, "key": credentials.AccessKeyID, "secret": credentials.SecretAccessKey, "token": credentials.SessionToken})
		if err != nil {
			return errors.New("could not encode private botocore signer input")
		}
		childCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		command := exec.CommandContext(childCtx, python, "-E", "-s", "-c", awsParityBotocoreScript)
		command.Env = []string{"AWS_EC2_METADATA_DISABLED=true", "NO_PROXY=*"}
		for _, key := range []string{"PATH", "SystemRoot", "LD_LIBRARY_PATH", "TMPDIR", "TMP", "TEMP"} {
			if value, exists := os.LookupEnv(key); exists {
				command.Env = append(command.Env, key+"="+value)
			}
		}
		command.Stdin = bytes.NewReader(input)
		output := localexec.NewBoundedOutput(16 << 10)
		stdout, stderr := localexec.NewTrackedOutputs(output, io.Discard)
		command.Stdout, command.Stderr = stdout, stderr
		if localexec.Configure(command) != nil || command.Start() != nil {
			return errors.New("could not start explicitly selected botocore signer")
		}
		defer localexec.Cleanup(command)
		if command.Wait() != nil || stdout.Err() != nil || stderr.Err() != nil || output.Overflowed() {
			return errors.New("explicit botocore signer failed; requires botocore 1.40.76")
		}
		var headers struct{ Authorization, Date, Token string }
		if json.Unmarshal(output.Bytes(), &headers) != nil ||
			!strings.HasPrefix(headers.Authorization, "AWS4-HMAC-SHA256 ") ||
			!regexp.MustCompile("^[0-9]{8}T[0-9]{6}Z$").MatchString(headers.Date) || headers.Token != credentials.SessionToken {
			return errors.New("explicit botocore signer returned invalid private headers")
		}
		request.Header.Set("Authorization", headers.Authorization)
		request.Header.Set("X-Amz-Date", headers.Date)
		if headers.Token != "" {
			request.Header.Set("X-Amz-Security-Token", headers.Token)
		}
		return nil
	}}
}

func awsParityObserve(ctx context.Context, client *http.Client, signer awsParitySigner, endpoint awsParityEndpoint, scenario awsParityCase, tamper bool) (awsParityObservation, bool) {
	observation := awsParityObservation{Endpoint: endpoint.name, Case: scenario.name, Signer: signer.name, Mode: signer.mode, Signature: "valid", Result: "transport-error"}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.url.String()+scenario.suffix, nil)
	if err != nil {
		return observation, false
	}
	observation.RequestedPath = awsParitySafePath(request.URL.EscapedPath(), scenario.suffix)
	observation.RequestedQuery = request.URL.RawQuery
	if err := signer.sign(ctx, request); err != nil {
		observation.Result = "signing-error"
		return observation, false
	}
	// Record the selected mode's actual wire bytes after signing/restoration.
	observation.WirePath = awsParitySafePath(request.URL.EscapedPath(), scenario.suffix)
	observation.WireQuery = request.URL.RawQuery
	if tamper {
		observation.Signature = "tampered"
		authorization := request.Header.Get("Authorization")
		last := byte('0')
		if strings.HasSuffix(authorization, "0") {
			last = '1'
		}
		request.Header.Set("Authorization", authorization[:len(authorization)-1]+string(last))
	}
	response, err := client.Do(request)
	if err != nil {
		// Transport errors include URLs; do not persist or print them.
		return observation, false
	}
	defer response.Body.Close()
	observation.Status = response.StatusCode
	observation.Result = "rejected"
	for _, allowed := range []string{"ForbiddenException", "AccessDeniedException", "InvalidSignatureException", "SignatureDoesNotMatch", "IncompleteSignatureException", "MissingAuthenticationTokenException", "ExpiredTokenException"} {
		if strings.Split(response.Header.Get("X-Amzn-Errortype"), ":")[0] == allowed {
			observation.ErrorClass = allowed
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		return observation, false
	}
	observation.Result = "unexpected-response"
	if response.Header.Get("X-EventBus-AWS-Parity") != awsParityMarker {
		return observation, false
	}
	var event awsParityEvent
	body := io.LimitReader(response.Body, (1<<20)+1)
	data, err := io.ReadAll(body)
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, &event) != nil {
		return observation, false
	}
	native := event.RequestContext.RequestID != "" &&
		((endpoint.name == "http-v2" && event.Version == "2.0" && event.RequestContext.HTTP.Method == http.MethodGet) ||
			(endpoint.name == "rest-v1" && event.HTTPMethod == http.MethodGet && event.Version == ""))
	if !native {
		return observation, false
	}
	observation.Result = "integration-reached"
	awsParitySanitize(event, scenario.suffix, &observation)
	return observation, true
}

// This is an observer, not a provisioning fixture or an assertion of AWS parity.
// It is never called by offline tests or normal SDK lanes.
func TestAWSGatewayParity(t *testing.T) {
	cfg, enabled, err := readAWSParityConfig(os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled {
		t.Skip("set EVENTBUS_AWS_PARITY=1 only for explicitly selected existing AWS echo fixtures")
	}
	transport := &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	output, err := awsParityOpenOutput(cfg.output)
	if err != nil {
		t.Fatal("could not create new private parity output file")
	}
	defer output.Close()
	credentialCtx, credentialCancel := context.WithTimeout(ctx, 15*time.Second)
	credentials, err := awsParityCredentials(credentialCtx, cfg, client,
		[]string{config.DefaultSharedConfigFilename()}, []string{config.DefaultSharedCredentialsFilename()})
	credentialCancel()
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(output)
	record := func(observation awsParityObservation) {
		t.Helper()
		if encoder.Encode(observation) != nil || output.Sync() != nil {
			t.Fatal("could not persist private parity observation")
		}
	}
	signers := []awsParitySigner{awsParityGoSigner(credentials, cfg.region, false), awsParityGoSigner(credentials, cfg.region, true)}
	if cfg.python != "" {
		signers = append(signers, awsParityBotocoreSigner(cfg.python, credentials, cfg.region))
	}
	for _, endpoint := range cfg.endpoints {
		for signerIndex, signer := range signers {
			control := awsParityCase{"control", "control"}
			baseline, echo := awsParityObserve(ctx, client, signer, endpoint, control, false)
			record(baseline)
			if !echo {
				t.Fatalf("%s/%s/%s control did not return the marked native echo; remaining requests refused", endpoint.name, signer.name, signer.mode)
			}
			tampered, echo := awsParityObserve(ctx, client, signer, endpoint, control, true)
			record(tampered)
			if echo || tampered.Status != http.StatusForbidden {
				t.Fatalf("%s/%s/%s control did not return 403 for a tampered signature; AWS_IAM fixture required", endpoint.name, signer.name, signer.mode)
			}
			for _, scenario := range awsParityCases {
				if signerIndex > 0 && !strings.Contains(scenario.suffix, "?") {
					continue
				}
				observation, _ := awsParityObserve(ctx, client, signer, endpoint, scenario, false)
				record(observation)
				if observation.Status == 0 {
					t.Errorf("%s/%s/%s/%s did not receive an HTTP response", endpoint.name, signer.name, signer.mode, scenario.name)
				}
			}
		}
	}
	t.Log("Private AWS gateway observations recorded with explicit signer/mode and actual wire-query provenance")
}

// Offline tests inject a transport so inherited host configuration cannot reach AWS.
type awsParityRoundTrip func(*http.Request) (*http.Response, error)

func (roundTrip awsParityRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestAWSParityConfig(t *testing.T) {
	base := map[string]string{
		"EVENTBUS_AWS_PARITY": "1", "EVENTBUS_AWS_PARITY_PROFILE": "explicit-parity",
		"EVENTBUS_AWS_PARITY_REGION": "eu-west-1", "EVENTBUS_AWS_PARITY_OUTPUT": "/tmp/private-parity.jsonl",
		"EVENTBUS_AWS_PARITY_HTTP_V2_URL": "https://example.execute-api.eu-west-1.amazonaws.com/stage" + awsParityPrefix,
	}
	read := func(values map[string]string) (awsParityConfig, bool, error) {
		return readAWSParityConfig(func(key string) string { return values[key] })
	}
	if cfg, enabled, err := read(base); err != nil || !enabled || len(cfg.endpoints) != 1 {
		t.Fatalf("explicit configuration failed: %v", err)
	}
	sovereign := make(map[string]string)
	for key, value := range base {
		sovereign[key] = value
	}
	sovereign["EVENTBUS_AWS_PARITY_REGION"] = "eusc-de-east-1"
	if _, _, err := read(sovereign); err != nil {
		t.Fatal("explicit sovereign region was rejected")
	}
	for _, scenario := range []struct{ name, key, value string }{
		{"flag", "EVENTBUS_AWS_PARITY", "yes"},
		{"profile", "EVENTBUS_AWS_PARITY_PROFILE", ""},
		{"region", "EVENTBUS_AWS_PARITY_REGION", ""},
		{"unsafe-region", "EVENTBUS_AWS_PARITY_REGION", "eu west 1"},
		{"relative-python", "EVENTBUS_AWS_PARITY_PYTHON", "python"},
		{"relative-output", "EVENTBUS_AWS_PARITY_OUTPUT", "evidence.jsonl"},
		{"http", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "http://example.com" + awsParityPrefix},
		{"application-route", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "https://example.com/application/"},
		{"userinfo", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "https://user:secret@example.com" + awsParityPrefix},
		{"query", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "https://example.com" + awsParityPrefix + "?token=secret"},
		{"fragment", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "https://example.com" + awsParityPrefix + "#fragment"},
		{"escaped-prefix", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "https://example.com/%73tage" + awsParityPrefix},
		{"dot-prefix", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "https://example.com/../stage" + awsParityPrefix},
		{"empty-prefix", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", "https://example.com//stage" + awsParityPrefix},
		{"no-endpoint", "EVENTBUS_AWS_PARITY_HTTP_V2_URL", ""},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			values := make(map[string]string)
			for key, value := range base {
				values[key] = value
			}
			values[scenario.key] = scenario.value
			if _, _, err := read(values); err == nil {
				t.Fatal("unsafe/incomplete configuration accepted")
			}
		})
	}
	if _, enabled, err := read(map[string]string{"AWS_PROFILE": "ambient", "AWS_REGION": "eu-west-1"}); err != nil || enabled {
		t.Fatal("ambient AWS configuration enabled the observer")
	}
	t.Run("private-exclusive-output", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "observations.jsonl")
		file, err := awsParityOpenOutput(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm()&0o077 != 0 {
			t.Fatal("output permits access by group/others")
		}
		if duplicate, err := awsParityOpenOutput(path); err == nil {
			duplicate.Close()
			t.Fatal("existing output was overwritten")
		}
	})
	t.Run("explicit-profile-no-env-fallback", func(t *testing.T) {
		dir := t.TempDir()
		credentialsFile := filepath.Join(dir, "credentials")
		if err := os.WriteFile(credentialsFile, []byte("[selected]\naws_access_key_id = selected-key\naws_secret_access_key = selected-secret\n[default]\naws_access_key_id = default-key\naws_secret_access_key = default-secret\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("AWS_ACCESS_KEY_ID", "ambient-key")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
		t.Setenv("AWS_PROFILE", "default")
		t.Setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/ambient-provider")
		client := &http.Client{Transport: awsParityRoundTrip(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("offline test refuses network")
		})}
		selected, err := awsParityCredentials(t.Context(), awsParityConfig{profile: "selected", region: "eu-west-1"}, client, []string{}, []string{credentialsFile})
		if err != nil || selected.AccessKeyID != "selected-key" {
			t.Fatal("explicit profile did not override ambient credentials")
		}
		if _, err := awsParityCredentials(t.Context(), awsParityConfig{profile: "missing", region: "eu-west-1"}, client, []string{}, []string{credentialsFile}); err == nil {
			t.Fatal("missing profile fell back")
		}
		for _, profile := range []config.SharedConfig{
			{}, {CredentialSource: "Environment"}, {CredentialSource: "EcsContainer"},
			{CredentialSource: "Ec2InstanceMetadata"}, {Source: &config.SharedConfig{}},
		} {
			if awsParityProfileHasProvider(&profile) {
				t.Fatal("ambient/provider-less chain accepted")
			}
		}
	})
}

func TestAWSParitySanitize(t *testing.T) {
	suffix := "items/Type/example%2Fhistory?value=a+b"
	var event awsParityEvent
	// Unknown sensitive fields must never enter the sanitized observation.
	raw := `{"version":"2.0","rawPath":"/stage/__eventbus_aws_parity__/items/Type/example%2Fhistory","rawQueryString":"value=a+b","headers":{"Authorization":"PRIVATE_AUTH","X-Amz-Security-Token":"PRIVATE_TOKEN"},"body":"PRIVATE_BODY","pathParameters":{"proxy":"items/Type/example/history","id":"PRIVATE_SECRET","private-key":"PRIVATE_VALUE"},"requestContext":{"requestId":"PRIVATE_REQUEST_ID","path":"/stage/__eventbus_aws_parity__/items/Type/example%2Fhistory","http":{"method":"GET","path":"/stage/__eventbus_aws_parity__/items/Type/example/history"},"authorizer":{"iam":{"userArn":"arn:aws:sts::123456789012:assumed-role/private-role/private-session","accessKey":"PRIVATE_ACCESS_KEY"}}}}`
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		t.Fatal(err)
	}
	observation := awsParityObservation{WireQuery: "value=a+b"}
	awsParitySanitize(event, suffix, &observation)
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "PRIVATE") || strings.Contains(string(encoded), "123456789012") ||
		strings.Contains(string(encoded), "private-role") || strings.Contains(string(encoded), "private-session") {
		t.Fatal("private fixture output escaped sanitization")
	}
	if observation.RawPath != "<fixture>/items/Type/example%2Fhistory" ||
		observation.ContextPath != "<fixture>/items/Type/example%2Fhistory" ||
		observation.ContextHTTPPath != "<fixture>/items/Type/example/history" ||
		observation.Captures["proxy"] != "items/Type/example/history" ||
		observation.Captures["id"] != "<unexpected>" ||
		observation.IdentityARN != "arn:aws:sts::<account>:assumed-role/<principal>" {
		t.Fatal("expected native paths/captures or redacted ARN lost")
	}
	for _, value := range []string{"Authorization: secret", "arn:aws:iam::not-an-account:user/private", "arn:aws:iam::123456789012:secret/private"} {
		if awsParitySafeARN(value) != "<unexpected>" {
			t.Fatal("unexpected ARN accepted")
		}
	}
	t.Run("signer-mode-wire-provenance", func(t *testing.T) {
		endpoint, err := url.Parse("https://offline.invalid" + awsParityPrefix)
		if err != nil {
			t.Fatal(err)
		}
		credentials := aws.Credentials{AccessKeyID: "offline", SecretAccessKey: "offline-secret", SessionToken: "offline-token"}
		scenario := awsParityCase{"wire-provenance", "items/Type/example%2Fhistory?z=2&a=a+b&letter=%41"}
		for _, signer := range []awsParitySigner{awsParityGoSigner(credentials, "eusc-de-east-1", false), awsParityGoSigner(credentials, "eusc-de-east-1", true)} {
			t.Run(signer.mode, func(t *testing.T) {
				var wireQuery string
				client := &http.Client{Transport: awsParityRoundTrip(func(request *http.Request) (*http.Response, error) {
					wireQuery = request.URL.RawQuery
					if request.Header.Get("X-Amz-Security-Token") != credentials.SessionToken || request.Header.Get("Authorization") == "" {
						t.Fatal("signed request headers missing")
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Eventbus-Aws-Parity": {awsParityMarker}}, Body: io.NopCloser(strings.NewReader(raw))}, nil
				})}
				got, echo := awsParityObserve(t.Context(), client, signer, awsParityEndpoint{"http-v2", endpoint}, scenario, false)
				wantWire := "a=a%20b&letter=A&z=2"
				if signer.mode == "original-query" {
					wantWire = "z=2&a=a+b&letter=%41"
				}
				if !echo || got.RequestedQuery != "z=2&a=a+b&letter=%41" ||
					got.WireQuery != wantWire || got.WireQuery != wireQuery || got.Mode != signer.mode || got.Signer != signer.name ||
					got.RequestedPath != "<fixture>/items/Type/example%2Fhistory" || got.WirePath != got.RequestedPath {
					t.Fatal("requested and actual signed wire provenance was not retained")
				}
			})
		}
		t.Run("optional-botocore-original-query", func(t *testing.T) {
			python := os.Getenv("EVENTBUS_AWS_PARITY_PYTHON")
			if python == "" {
				t.Skip("explicit EVENTBUS_AWS_PARITY_PYTHON enables the offline pinned botocore proof")
			}
			if err := awsParityValidatePython(python); err != nil {
				t.Fatal(err)
			}
			client := &http.Client{Transport: awsParityRoundTrip(func(request *http.Request) (*http.Response, error) {
				if request.URL.RawQuery != "z=2&a=a+b&letter=%41" || request.Header.Get("X-Amz-Security-Token") != credentials.SessionToken {
					t.Fatal("botocore changed original URL bytes or lost explicit session credentials")
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Eventbus-Aws-Parity": {awsParityMarker}}, Body: io.NopCloser(strings.NewReader(raw))}, nil
			})}
			got, echo := awsParityObserve(t.Context(), client, awsParityBotocoreSigner(python, credentials, "eusc-de-east-1"), awsParityEndpoint{"http-v2", endpoint}, scenario, false)
			if !echo || got.Result != "integration-reached" || got.Signer != "botocore/1.40.76/SigV4Auth" ||
				got.Mode != "original-query" || got.WireQuery != got.RequestedQuery {
				t.Fatal("explicit pinned botocore signer failed offline original-wire proof")
			}
		})
	})
	if awsParitySafePath("/unrelated/private-secret", suffix) != "<unexpected>" ||
		awsParitySafeRoute("GET /private-application") != "<unexpected>" {
		t.Fatal("unexpected application output accepted")
	}
}
