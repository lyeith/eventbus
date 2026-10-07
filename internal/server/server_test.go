package server_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/firehose"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/secrets"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/lyeith/eventbus/internal/ssm"
	"github.com/stretchr/testify/require"
)

func composedServices(t *testing.T) server.Services {
	t.Helper()
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	capture, err := ses.OpenSESCapture(filepath.Join(t.TempDir(), "ses.jsonl"))
	require.NoError(t, err)
	manager := ses.NewSESManager(ses.SESFixtures{}, capture)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	delivery := firehose.NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	t.Cleanup(func() { require.NoError(t, delivery.Shutdown()) })
	return server.Services{
		Messaging:      messaging.NewHandler(messaging.NewBroker("us-east-1", "000000000000", 14100)),
		Firehose:       firehose.NewHandler(delivery),
		SSM:            ssm.NewHandler(ssm.NewSSMStore()),
		Secrets:        secrets.NewHandler(secrets.NewSecretsStore("us-east-1", "000000000000")),
		Cognito:        cognito.NewHandler(store, cognito.Options{IssuerBase: "http://localhost:4100"}),
		SES:            ses.NewHandler(manager),
		CognitoURLs:    &server.CognitoURLs{Issuer: "http://localhost:4100", JWKS: "http://localhost:4100"},
		QueryBodyLimit: ses.QueryBodyLimit,
	}
}

func request(handler http.Handler, method, path, target, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if target != "" {
		r.Header.Set("X-Amz-Target", target)
		r.Header.Set("Content-Type", "application/x-amz-json-1.0")
	} else if strings.HasPrefix(path, "/v2/email/") {
		r.Header.Set("Content-Type", "application/json")
	} else {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	result := httptest.NewRecorder()
	handler.ServeHTTP(result, r)
	return result
}

func TestRoutesToServiceOwnedProtocols(t *testing.T) {
	router := server.New(composedServices(t))
	var createdPoolID string
	for _, tc := range []struct {
		name, method, path, target, body, contains string
		status                                     int
	}{
		{"SNS query", "POST", "/", "", "Action=CreateTopic&Name=route-topic", "TopicArn", 200},
		{"SQS JSON", "POST", "/", "AmazonSQS.CreateQueue", `{"QueueName":"route-queue"}`, "QueueUrl", 200},
		{"SQS queue path", "POST", "/queue/route-queue", "", "Action=GetQueueUrl&QueueName=route-queue", "GetQueueUrlResponse", 200},
		{"unknown target cannot invoke SQS", "POST", "/", "OtherService.ListQueues", `{}`, "UnknownOperationException", 400},
		{"Firehose JSON", "POST", "/", "Firehose_20150804.DescribeDeliveryStream", `{"DeliveryStreamName":"missing"}`, "ResourceNotFoundException", 400},
		{"SSM JSON", "POST", "/", "AmazonSSM.PutParameter", `{"Name":"/route","Value":"ok"}`, "Version", 200},
		{"Secrets JSON", "POST", "/", "secretsmanager.CreateSecret", `{"Name":"route-secret","SecretString":"ok"}`, "arn:aws:secretsmanager:us-east-1:000000000000", 200},
		{"Cognito JSON", "POST", "/", "AWSCognitoIdentityProviderService.CreateUserPool", `{"PoolName":"route-pool"}`, "UserPool", 200},
		{"SES query", "POST", "/", "", "Action=SendEmail&Version=2010-12-01&Source=sender%40example.test&Destination.ToAddresses.member.1=receiver%40example.test&Message.Subject.Data=Hello&Message.Body.Text.Data=World", "SendEmailResponse", 200},
		{"SES REST", "POST", "/v2/email/outbound-emails", "", `{"FromEmailAddress":"sender@example.test","Destination":{"ToAddresses":["receiver@example.test"]},"Content":{"Simple":{"Subject":{"Data":"Hello"},"Body":{"Text":{"Data":"World"}}}}}`, "MessageId", 200},
		{"JWKS suffix", "GET", "/route-pool/.well-known/jwks.json", "", "", "keys", 200},
		{"unsupported SNS query", "POST", "/", "", "Action=NoSuchAction", "InvalidAction", 400},
		{"unsupported SQS JSON", "POST", "/", "AmazonSQS.NoSuchAction", `{}`, "Unknown SQS action", 400},
		{"unsupported Firehose JSON", "POST", "/", "Firehose_20150804.NoSuchAction", `{}`, "Unknown Firehose action", 400},
		{"unsupported SSM JSON", "POST", "/", "AmazonSSM.NoSuchAction", `{}`, "Unknown SSM action", 400},
		{"unsupported Secrets JSON", "POST", "/", "secretsmanager.NoSuchAction", `{}`, "Unknown Secrets", 400},
		{"unsupported Cognito JSON", "POST", "/", "AWSCognitoIdentityProviderService.NoSuchAction", `{}`, "InvalidAction", 400},
		{"unsupported SES query", "POST", "/", "", "Action=NoSuchAction&Version=2010-12-01", "InvalidAction", 400},
		{"unsupported SES REST", "POST", "/v2/email/no-such-operation", "", `{}`, "SES operation is not implemented", 404},
		{"malformed query", "POST", "/", "", "Action=Publish&Message=%ZZ", "Malformed Query request", 400},
		{"malformed SES query", "POST", "/", "", "Action=SendEmail&Version=2010-12-01&Source=%ZZ", "Malformed Query request", 400},
		{"method envelope", "GET", "/", "", "", "<ErrorResponse>", 405},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "JWKS suffix" {
				require.NotEmpty(t, createdPoolID)
				tc.path = "/" + createdPoolID + "/.well-known/jwks.json"
			}
			result := request(router, tc.method, tc.path, tc.target, tc.body)
			require.Equal(t, tc.status, result.Code, result.Body.String())
			require.Contains(t, result.Body.String(), tc.contains)
			if tc.name == "Cognito JSON" {
				var created struct {
					UserPool struct {
						ID string `json:"Id"`
					} `json:"UserPool"`
				}
				require.NoError(t, json.Unmarshal(result.Body.Bytes(), &created))
				createdPoolID = created.UserPool.ID
			}
		})
	}
}

func TestHealthAdvertisesOnlyConfiguredCognitoURLs(t *testing.T) {
	for _, configured := range []bool{false, true} {
		services := composedServices(t)
		if !configured {
			services.CognitoURLs = nil
		}
		result := request(server.New(services), "GET", "/health", "", "")
		require.Equal(t, 200, result.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(result.Body.Bytes(), &body))
		require.Equal(t, "healthy", body["status"])
		require.Equal(t, "eventbus", body["service"])
		if configured {
			require.Equal(t, "http://localhost:4100", body["cognito_issuer"])
			require.Equal(t, "http://localhost:4100", body["cognito_jwks_url"])
		} else {
			require.NotContains(t, body, "cognito_issuer")
			require.NotContains(t, body, "cognito_jwks_url")
		}
	}
}

func TestOptionalServicesAndQueryLimit(t *testing.T) {
	services := composedServices(t)
	services.SES = nil
	services.Cognito = cognito.NewHandler(nil, cognito.Options{})
	services.CognitoURLs = nil
	router := server.New(services)
	require.Equal(t, 405, request(router, "GET", "/v2/email/outbound-emails", "", "").Code)
	require.Equal(t, 503, request(router, "GET", "/pool/.well-known/jwks.json", "", "").Code)
	result := request(router, "POST", "/", "AWSCognitoIdentityProviderService.GetUser", `{}`)
	require.Equal(t, 500, result.Code)
	require.Contains(t, result.Body.String(), "InternalErrorException")
	services.QueryBodyLimit = 8
	result = request(server.New(services), "POST", "/", "", "Action=CreateTopic&Name=too-large")
	require.Equal(t, 400, result.Code)
	require.Contains(t, result.Body.String(), "Malformed Query request")
}
