package lambda

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func metadataRequest(service *Service, name, qualifier string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, invokePrefix+url.PathEscape(name)+qualifier, nil)
	response := httptest.NewRecorder()
	service.ServeHTTP(response, request)
	return response
}

func TestGetFunctionRegisteredMetadataSelectionAndPrivacy(t *testing.T) {
	directory := t.TempDir()
	// Metadata and DryRun must not execute a handler or a command.
	marker := filepath.Join(directory, "must-not-start")
	f := providedFunction(t, "echo")
	f.Timeout = 1500 * time.Millisecond
	f.Environment = maps.Clone(f.Environment)
	f.Environment["AWS_REGION"] = "eu-west-1"
	f.Environment["AWS_ACCOUNT_ID"] = "123456789012"
	f.Environment["PRIVATE_SECRET"] = "must-stay-private"
	f.Environment["EVENTBUS_LAMBDA_TEST_INIT_PID"] = marker
	service := newTestService(t, map[string]Function{"metadata": f, "metadata:live": f, "metadata:2": f}, directory)
	for _, test := range []struct{ name, qualifier, arn, version string }{
		{"metadata", "", "arn:aws:lambda:eu-west-1:123456789012:function:metadata", "$LATEST"},
		{"metadata", "?Qualifier=live", "arn:aws:lambda:eu-west-1:123456789012:function:metadata:live", "$LATEST"},
		{"metadata:2", "", "arn:aws:lambda:eu-west-1:123456789012:function:metadata:2", "2"},
		{"metadata", "?Qualifier=%24LATEST", "arn:aws:lambda:eu-west-1:123456789012:function:metadata:$LATEST", "$LATEST"},
		{"arn:aws-us-gov:lambda:us-gov-west-1:987654321098:function:metadata:live", "", "arn:aws-us-gov:lambda:us-gov-west-1:987654321098:function:metadata:live", "$LATEST"},
		{"987654321098:function:metadata", "?Qualifier=live", "arn:aws:lambda:eu-west-1:987654321098:function:metadata:live", "$LATEST"},
	} {
		t.Run(test.name+test.qualifier, func(t *testing.T) {
			response := metadataRequest(service, test.name, test.qualifier)
			if response.Code != 200 || response.Header().Get("X-Amzn-RequestId") == "" {
				t.Fatalf("metadata HTTP %d: %s", response.Code, response.Body.String())
			}
			var result struct{ Configuration FunctionConfiguration }
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			config := result.Configuration
			if config.FunctionName != "metadata" || config.FunctionARN != test.arn || config.Version != test.version || config.Runtime != "provided" || config.Handler != "" || config.Timeout != 2 || config.State != "Active" {
				t.Fatalf("metadata %+v", config)
			}
			for _, secret := range []string{directory, marker, "PRIVATE_SECRET", "must-stay-private", "Environment", "Command", "Code", "Location"} {
				if strings.Contains(response.Body.String(), secret) {
					t.Fatalf("metadata exposed %q: %s", secret, response.Body.String())
				}
			}
		})
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("metadata spawned command: %v", err)
	}
	for _, target := range []string{"metadata", "metadata:live"} {
		response := requestInvoke(service, target, "", map[string]string{"X-Amz-Invocation-Type": "DryRun"})
		if response.Code != 204 {
			t.Fatalf("DryRun changed: %d %s", response.Code, response.Body.String())
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("DryRun spawned command: %v", err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	response := metadataRequest(service, "metadata", "")
	if response.Code != 503 || response.Header().Get("X-Amzn-ErrorType") != "ServiceException" {
		t.Fatalf("closed metadata %d %s", response.Code, response.Body.String())
	}
}

func TestGetFunctionHandlerProjection(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			// Projection itself requires no interpreter or execution; constructor
			// resolution has already established this immutable entry.
			service := &Service{functions: map[string]executableFunction{"nested": {
				name: "nested", runtime: runtime, module: filepath.Join(t.TempDir(), "private-source", "handler.py"),
				exported: "process", timeout: 10 * time.Second,
			}}}
			result, err := service.GetFunction("nested", "")
			if err != nil || result.Handler != "handler.process" || result.Runtime != runtime {
				t.Fatalf("handler metadata %+v %v", result, err)
			}
		})
	}
}

func TestGetFunctionTypedTargetAndQualifierErrors(t *testing.T) {
	service := newTestService(t, map[string]Function{"metadata": providedFunction(t, "echo")}, t.TempDir())
	for _, test := range []struct {
		name, qualifier, code string
		status                int
	}{
		{"missing", "", "ResourceNotFoundException", 404},
		{"metadata", "?Qualifier=missing", "ResourceNotFoundException", 404},
		{"metadata:missing", "", "ResourceNotFoundException", 404},
		{"metadata:live", "?Qualifier=other", "InvalidParameterValueException", 400},
		{"metadata", "?Qualifier=bad%3Aalias", "InvalidParameterValueException", 400},
		{"metadata", "?Qualifier=", "InvalidParameterValueException", 400},
		{"metadata", "?Qualifier=one&Qualifier=two", "InvalidParameterValueException", 400},
		{"arn:aws:s3:us-east-1:123456789012:function:metadata", "", "InvalidParameterValueException", 400},
		{"arn:aws:lambda:us-east-1:bad-account:function:metadata", "", "InvalidParameterValueException", 400},
		{"", "", "InvalidParameterValueException", 400},
	} {
		response := metadataRequest(service, test.name, test.qualifier)
		if response.Code != test.status || response.Header().Get("X-Amzn-ErrorType") != test.code {
			t.Fatalf("%s%s: %d %s %s", test.name, test.qualifier, response.Code, response.Header().Get("X-Amzn-ErrorType"), response.Body.String())
		}
	}
	response := httptest.NewRecorder()
	service.ServeHTTP(response, httptest.NewRequest(http.MethodPost, invokePrefix+"metadata", nil))
	if response.Code != 405 || response.Header().Get("Allow") != "GET" {
		t.Fatalf("wrong method: %d %s", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	service.ServeHTTP(response, httptest.NewRequest(http.MethodGet, invokePrefix+"metadata/configuration", nil))
	if response.Code != 404 {
		t.Fatalf("unimplemented management API accepted: %d", response.Code)
	}
}
