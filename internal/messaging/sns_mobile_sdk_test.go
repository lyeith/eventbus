package messaging

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// The unmodified Go SDK exercises all ten operations through the Query adapter;
// malformed XML/map/list wrappers would break these decoded contracts.
func TestSNSMobileAWSQuerySDK(t *testing.T) {
	broker, _, client, _ := setupTestServer(t)
	var capture bytes.Buffer
	broker.SetSNSCapture(&SNSCapture{writer: &capture})
	ctx := context.Background()
	application, err := client.CreatePlatformApplication(ctx, &sns.CreatePlatformApplicationInput{
		Name: aws.String("sdk-mobile"), Platform: aws.String("GCM"), Attributes: map[string]string{"PlatformCredential": "local-credential"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetPlatformApplicationAttributes(ctx, &sns.SetPlatformApplicationAttributesInput{
		PlatformApplicationArn: application.PlatformApplicationArn, Attributes: map[string]string{"SuccessFeedbackSampleRate": "50"},
	}); err != nil {
		t.Fatal(err)
	}
	attributes, err := client.GetPlatformApplicationAttributes(ctx, &sns.GetPlatformApplicationAttributesInput{PlatformApplicationArn: application.PlatformApplicationArn})
	if err != nil || attributes.Attributes["SuccessFeedbackSampleRate"] != "50" || attributes.Attributes["AuthenticationMethod"] != "Key" || attributes.Attributes["PlatformCredential"] != "" {
		t.Fatalf("application attributes: %#v, %v", attributes, err)
	}
	applications, err := client.ListPlatformApplications(ctx, &sns.ListPlatformApplicationsInput{})
	if err != nil || len(applications.PlatformApplications) != 1 || aws.ToString(applications.PlatformApplications[0].PlatformApplicationArn) != aws.ToString(application.PlatformApplicationArn) {
		t.Fatalf("application list: %#v, %v", applications, err)
	}
	endpoint, err := client.CreatePlatformEndpoint(ctx, &sns.CreatePlatformEndpointInput{
		PlatformApplicationArn: application.PlatformApplicationArn, Token: aws.String("device-token"), CustomUserData: aws.String("sdk & agent"),
	})
	if err != nil {
		t.Fatal(err)
	}
	endpointAttributes, err := client.GetEndpointAttributes(ctx, &sns.GetEndpointAttributesInput{EndpointArn: endpoint.EndpointArn})
	if err != nil || endpointAttributes.Attributes["Enabled"] != "true" || endpointAttributes.Attributes["CustomUserData"] != "sdk & agent" {
		t.Fatalf("endpoint attributes: %#v, %v", endpointAttributes, err)
	}
	endpoints, err := client.ListEndpointsByPlatformApplication(ctx, &sns.ListEndpointsByPlatformApplicationInput{PlatformApplicationArn: application.PlatformApplicationArn})
	if err != nil || len(endpoints.Endpoints) != 1 || aws.ToString(endpoints.Endpoints[0].EndpointArn) != aws.ToString(endpoint.EndpointArn) || endpoints.Endpoints[0].Attributes["Token"] != "device-token" {
		t.Fatalf("endpoint list: %#v, %v", endpoints, err)
	}
	published, err := client.Publish(ctx, &sns.PublishInput{TargetArn: endpoint.EndpointArn, Message: aws.String("local push")})
	if err != nil || aws.ToString(published.MessageId) == "" || !strings.Contains(capture.String(), "local push") {
		t.Fatalf("publish: %#v, %v, capture %s", published, err, capture.String())
	}
	if _, err := client.SetEndpointAttributes(ctx, &sns.SetEndpointAttributesInput{EndpointArn: endpoint.EndpointArn, Attributes: map[string]string{"Enabled": "false"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Publish(ctx, &sns.PublishInput{TargetArn: endpoint.EndpointArn, Message: aws.String("disabled")}); err == nil || !strings.Contains(err.Error(), "EndpointDisabled") {
		t.Fatalf("want SDK EndpointDisabled, got %v", err)
	}
	if _, err := client.DeleteEndpoint(ctx, &sns.DeleteEndpointInput{EndpointArn: endpoint.EndpointArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeletePlatformApplication(ctx, &sns.DeletePlatformApplicationInput{PlatformApplicationArn: application.PlatformApplicationArn}); err != nil {
		t.Fatal(err)
	}
}
