package server_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/lyeith/eventbus/internal/scheduler"
	"github.com/lyeith/eventbus/internal/server"
)

func TestServerNativeJSONFallbackProtocols(t *testing.T) {
	for _, test := range []struct {
		target   string
		protocol awsprotocol.JSONProtocol
		status   int
	}{
		{"Firehose_20150804.NoSuchAction", awsprotocol.JSON11, 503}, {"AmazonSSM.NoSuchAction", awsprotocol.JSON11, 503}, {"secretsmanager.NoSuchAction", awsprotocol.JSON11, 503},
		{"AWSCognitoIdentityProviderService.NoSuchAction", awsprotocol.JSON10, 503}, {"AmazonSQS.NoSuchAction", awsprotocol.JSON10, 503}, {"OtherService.NoSuchAction", awsprotocol.JSON10, 400},
	} {
		for _, method := range []string{"POST", "GET"} {
			t.Run(method+"/"+test.target, func(t *testing.T) {
				router := server.New(server.Services{})
				response := httptest.NewRecorder()
				response.Header().Set("X-Amzn-RequestId", "owned-native-id")
				request := httptest.NewRequest(method, "/", strings.NewReader(`{}`))
				request.Header.Set("X-Amz-Target", test.target)
				router.ServeHTTP(response, request)
				want := test.status
				if method == "GET" {
					want = 405
				}
				if response.Code != want || response.Header().Get("Content-Type") != string(test.protocol) || response.Header().Get("X-Amzn-RequestId") != "owned-native-id" {
					t.Fatalf("native fallback mismatch %d %v %s", response.Code, response.Header(), response.Body.String())
				}
				var body map[string]string
				if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["__type"] != response.Header().Get("X-Amzn-ErrorType") {
					t.Fatalf("native JSON error mismatch %s", response.Body.String())
				}
			})
		}
	}
}

func TestServerNativeQueryFallbackNamespaceAndRequestID(t *testing.T) {
	for _, version := range []string{"", "unknown", "2010-03-31", "2012-11-05", "2010-12-01"} {
		for _, malformed := range []bool{false, true} {
			name := "valid"
			if malformed {
				name = "malformed"
			}
			t.Run(version+"/"+name, func(t *testing.T) {
				body := "Action=NoSuchAction&Version=" + version
				if malformed {
					body += "&Message=%ZZ"
				}
				request := httptest.NewRequest("POST", "/", strings.NewReader(body))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				response := httptest.NewRecorder()
				response.Header().Set("X-Amzn-RequestId", "owned-query-id")
				server.New(server.Services{}).ServeHTTP(response, request)
				var envelope struct {
					XMLName   xml.Name
					RequestID string `xml:"RequestId"`
					Error     struct{ Code string }
				}
				if err := xml.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
					t.Fatal(err)
				}
				namespace, _ := awsprotocol.QueryNamespace(version)
				want := 503
				if malformed {
					want = 400
				}
				if response.Code != want || envelope.XMLName.Space != namespace || envelope.RequestID != "owned-query-id" || response.Header().Get("X-Amzn-RequestId") != envelope.RequestID {
					t.Fatalf("Query fallback mismatch %d %+v %v", response.Code, envelope, response.Header())
				}
			})
		}
	}
}

type schedulerTarget struct{}

func (schedulerTarget) ValidateTarget(context.Context, string) error      { return nil }
func (schedulerTarget) AdmitTarget(context.Context, string, []byte) error { return nil }

func TestServerSchedulerRoutesAndExplicitGroupRefusal(t *testing.T) {
	service, err := scheduler.New(scheduler.Options{Region: "eu-west-1", AccountID: "123456789012", Dev: scheduler.DevOptions{Groups: []string{"fixture"}}}, schedulerTarget{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	router := server.New(server.Services{Scheduler: scheduler.NewHandler(service)})
	body := `{"GroupName":"fixture","ScheduleExpression":"at(2099-01-01T00:00:00)","FlexibleTimeWindow":{"Mode":"OFF"},"State":"DISABLED","Target":{"Arn":"arn:aws:lambda:eu-west-1:123456789012:function:owned:live","RoleArn":"arn:aws:iam::123456789012:role/owned","Input":"{\"owned\":true}"}}`
	for _, test := range []struct {
		method, path, body, contains string
		status                       int
	}{
		{"POST", "/schedules/owned", body, "arn:aws:scheduler:eu-west-1:123456789012:schedule/fixture/owned", 200},
		{"GET", "/schedules/owned?groupName=fixture", "", "DISABLED", 200},
		{"DELETE", "/schedules/owned?groupName=fixture", "", "", 200},
		{"GET", "/schedules/owned?groupName=fixture", "", "Schedule or configured group not found", 404},
		{"GET", "/schedules", "", "Unsupported Scheduler path", 400},
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(response, request)
		if response.Code != test.status || !strings.Contains(response.Body.String(), test.contains) || response.Header().Get("Content-Type") != "application/json" || response.Header().Get("X-Amzn-RequestId") == "" {
			t.Fatalf("Scheduler route %s %s: %d %v %s", test.method, test.path, response.Code, response.Header(), response.Body.String())
		}
	}
	for _, method := range []string{"GET", "POST", "PUT", "DELETE"} {
		for _, path := range []string{"/schedule-groups", "/schedule-groups/fixture"} {
			response := httptest.NewRecorder()
			response.Header().Set("X-Amzn-RequestId", "owned-group-id")
			router.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(`{}`)))
			if response.Code != 400 || response.Header().Get("X-Amzn-ErrorType") != "ValidationException" || response.Header().Get("X-Amzn-RequestId") != "owned-group-id" || !strings.Contains(response.Body.String(), "Schedule group management is not supported") {
				t.Fatalf("group management claimed support: %d %v %s", response.Code, response.Header(), response.Body.String())
			}
		}
	}
	for _, path := range []string{"/schedules/owned", "/2015-03-31/functions/owned/invocations"} {
		response := httptest.NewRecorder()
		server.New(server.Services{}).ServeHTTP(response, httptest.NewRequest("POST", path, strings.NewReader(`{}`)))
		want := 503
		if strings.HasPrefix(path, "/2015") {
			want = 404
		}
		if response.Code != want || response.Header().Get("Content-Type") != "application/json" || response.Header().Get("X-Amzn-RequestId") == "" {
			t.Fatalf("optional native service %s: %d %v", path, response.Code, response.Header())
		}
	}
}
