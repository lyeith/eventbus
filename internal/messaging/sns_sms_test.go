package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/stretchr/testify/require"
)

// These SDK requests exercise all 11 SMS operations, including the legacy
// lowercase Query members that differ from sandbox/origination members.
func TestSNSSMSOperationsSDK(t *testing.T) {
	broker, _, client, _ := setupTestServer(t)
	ctx := context.Background()
	capturePath := filepath.Join(t.TempDir(), "sns.jsonl")
	capture, err := OpenSNSCapture(capturePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, capture.Close()) })
	broker.SetSNSCapture(capture)
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	smsSetTestTime(broker, now)

	status, err := client.GetSMSSandboxAccountStatus(ctx, &sns.GetSMSSandboxAccountStatusInput{})
	require.NoError(t, err)
	require.True(t, status.IsInSandbox)
	originations, err := client.ListOriginationNumbers(ctx, &sns.ListOriginationNumbersInput{MaxResults: aws.Int32(30)})
	require.NoError(t, err)
	require.Empty(t, originations.PhoneNumbers)

	defaults, err := client.GetSMSAttributes(ctx, &sns.GetSMSAttributesInput{})
	require.NoError(t, err)
	require.Equal(t, "Promotional", defaults.Attributes["DefaultSMSType"])
	settings := map[string]string{
		"MonthlySpendLimit": "25.50", "DefaultSMSType": "Transactional", "DefaultSenderID": "EventBus",
		"DeliveryStatusSuccessSamplingRate": "75", "DeliveryStatusIAMRole": "arn:aws:iam::000000000000:role/sms",
		"UsageReportS3Bucket": "eventbus-sms-usage",
	}
	_, err = client.SetSMSAttributes(ctx, &sns.SetSMSAttributesInput{Attributes: settings})
	require.NoError(t, err)
	selected, err := client.GetSMSAttributes(ctx, &sns.GetSMSAttributesInput{Attributes: []string{"DefaultSMSType", "DefaultSenderID"}})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"DefaultSMSType": "Transactional", "DefaultSenderID": "EventBus"}, selected.Attributes)
	all, err := client.GetSMSAttributes(ctx, &sns.GetSMSAttributesInput{})
	require.NoError(t, err)
	require.Equal(t, settings, all.Attributes)

	phone := "+12065550100"
	optedOut, err := client.CheckIfPhoneNumberIsOptedOut(ctx, &sns.CheckIfPhoneNumberIsOptedOutInput{PhoneNumber: aws.String(phone)})
	require.NoError(t, err)
	require.False(t, optedOut.IsOptedOut)
	state := broker.snsState()
	state.mu.Lock()
	state.sms.optedOut[phone] = true // A carrier opt-out has no SNS API operation.
	state.mu.Unlock()
	optedOut, err = client.CheckIfPhoneNumberIsOptedOut(ctx, &sns.CheckIfPhoneNumberIsOptedOutInput{PhoneNumber: aws.String(phone)})
	require.NoError(t, err)
	require.True(t, optedOut.IsOptedOut)
	optedOutList, err := client.ListPhoneNumbersOptedOut(ctx, &sns.ListPhoneNumbersOptedOutInput{})
	require.NoError(t, err)
	require.Equal(t, []string{phone}, optedOutList.PhoneNumbers)
	_, err = client.CreateSMSSandboxPhoneNumber(ctx, &sns.CreateSMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone)})
	require.ErrorContains(t, err, "OptedOut")
	_, err = client.OptInPhoneNumber(ctx, &sns.OptInPhoneNumberInput{PhoneNumber: aws.String(phone)})
	require.NoError(t, err)
	optedOutList, err = client.ListPhoneNumbersOptedOut(ctx, &sns.ListPhoneNumbersOptedOutInput{})
	require.NoError(t, err)
	require.Empty(t, optedOutList.PhoneNumbers)
	_, err = client.OptInPhoneNumber(ctx, &sns.OptInPhoneNumberInput{PhoneNumber: aws.String(phone)})
	require.ErrorContains(t, err, "InvalidParameter")

	_, err = client.Publish(ctx, &sns.PublishInput{PhoneNumber: aws.String(phone), Message: aws.String("not verified")})
	require.ErrorContains(t, err, "AuthorizationError")
	_, err = client.CreateSMSSandboxPhoneNumber(ctx, &sns.CreateSMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone), LanguageCode: snstypes.LanguageCodeString("en-US")})
	require.NoError(t, err)
	records := smsReadCapture(t, capturePath)
	require.Len(t, records, 1)
	require.Equal(t, "CreateSMSSandboxPhoneNumber", records[0].Operation)
	require.Equal(t, phone, records[0].PhoneNumber)
	otp, ok := records[0].Details["otp"].(string)
	require.True(t, ok)
	require.Regexp(t, `^[0-9]{6}$`, otp)
	require.Contains(t, records[0].Message, otp)
	pending, err := client.ListSMSSandboxPhoneNumbers(ctx, &sns.ListSMSSandboxPhoneNumbersInput{})
	require.NoError(t, err)
	require.Len(t, pending.PhoneNumbers, 1)
	require.Equal(t, "Pending", string(pending.PhoneNumbers[0].Status))

	wrongOTP := "0" + otp[1:]
	if otp[0] == '0' {
		wrongOTP = "1" + otp[1:]
	}
	_, err = client.VerifySMSSandboxPhoneNumber(ctx, &sns.VerifySMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone), OneTimePassword: aws.String(wrongOTP)})
	require.ErrorContains(t, err, "VerificationException")
	var verification *snstypes.VerificationException
	require.ErrorAs(t, err, &verification)
	require.Equal(t, "FAILED", aws.ToString(verification.Status))
	_, err = client.VerifySMSSandboxPhoneNumber(ctx, &sns.VerifySMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone), OneTimePassword: aws.String(otp)})
	require.NoError(t, err)
	_, err = client.VerifySMSSandboxPhoneNumber(ctx, &sns.VerifySMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone), OneTimePassword: aws.String(otp)})
	require.ErrorContains(t, err, "VerificationException")

	published, err := client.Publish(ctx, &sns.PublishInput{
		PhoneNumber: aws.String(phone), Message: aws.String("Your sign-in code is 123456"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"AWS.SNS.SMS.SMSType":  {DataType: aws.String("String"), StringValue: aws.String("Transactional")},
			"AWS.SNS.SMS.MaxPrice": {DataType: aws.String("Number"), StringValue: aws.String("0.10")},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(published.MessageId))
	records = smsReadCapture(t, capturePath)
	require.Len(t, records, 2)
	require.Equal(t, "Publish", records[1].Operation)
	require.Equal(t, aws.ToString(published.MessageId), records[1].MessageID)
	require.Equal(t, "Your sign-in code is 123456", records[1].Message)
	require.Equal(t, "0.10", records[1].MessageAttributes["AWS.SNS.SMS.MaxPrice"].StringValue)
	require.Equal(t, "captured", records[1].Deliveries[0].Status)

	second := "+12065550101"
	_, err = client.CreateSMSSandboxPhoneNumber(ctx, &sns.CreateSMSSandboxPhoneNumberInput{PhoneNumber: aws.String(second)})
	require.NoError(t, err)
	firstPage, err := client.ListSMSSandboxPhoneNumbers(ctx, &sns.ListSMSSandboxPhoneNumbersInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, firstPage.PhoneNumbers, 1)
	require.Equal(t, phone, aws.ToString(firstPage.PhoneNumbers[0].PhoneNumber))
	require.Equal(t, "Verified", string(firstPage.PhoneNumbers[0].Status))
	require.NotEmpty(t, aws.ToString(firstPage.NextToken))
	secondPage, err := client.ListSMSSandboxPhoneNumbers(ctx, &sns.ListSMSSandboxPhoneNumbersInput{MaxResults: aws.Int32(1), NextToken: firstPage.NextToken})
	require.NoError(t, err)
	require.Len(t, secondPage.PhoneNumbers, 1)
	require.Equal(t, second, aws.ToString(secondPage.PhoneNumbers[0].PhoneNumber))
	require.Nil(t, secondPage.NextToken)
	_, err = client.ListOriginationNumbers(ctx, &sns.ListOriginationNumbersInput{NextToken: firstPage.NextToken})
	require.ErrorContains(t, err, "InvalidParameter")

	_, err = client.DeleteSMSSandboxPhoneNumber(ctx, &sns.DeleteSMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone)})
	require.ErrorContains(t, err, "UserError")
	smsSetTestTime(broker, now.Add(24*time.Hour))
	_, err = client.DeleteSMSSandboxPhoneNumber(ctx, &sns.DeleteSMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone)})
	require.NoError(t, err)
	_, err = client.DeleteSMSSandboxPhoneNumber(ctx, &sns.DeleteSMSSandboxPhoneNumberInput{PhoneNumber: aws.String(phone)})
	require.ErrorContains(t, err, "ResourceNotFound")
}

func TestSNSSMSSandboxLimitsAndCaptureRollback(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	handler := NewHandler(broker)
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	smsSetTestTime(broker, now)
	broker.SetSNSCapture(NewSNSCapture(smsFailWriter{}))
	response := smsTestQuery(t, handler, "CreateSMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550100"}})
	require.Equal(t, http.StatusInternalServerError, response.Code)
	state := broker.snsState()
	state.mu.Lock()
	require.Empty(t, state.sms.sandbox)
	state.mu.Unlock()
	capturePath := filepath.Join(t.TempDir(), "sns.jsonl")
	capture, err := OpenSNSCapture(capturePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, capture.Close()) })
	broker.SetSNSCapture(capture)
	for i := 0; i < 6; i++ {
		response = smsTestQuery(t, handler, "CreateSMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550100"}})
		require.Equal(t, http.StatusOK, response.Code)
	}
	response = smsTestQuery(t, handler, "CreateSMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550100"}})
	require.Equal(t, http.StatusTooManyRequests, response.Code)
	require.Contains(t, response.Body.String(), "<Code>Throttled</Code>")
	records := smsReadCapture(t, capturePath)
	require.Len(t, records, 6)
	lastOTP := records[5].Details["otp"].(string)
	smsSetTestTime(broker, now.Add(15*time.Minute))
	response = smsTestQuery(t, handler, "VerifySMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550100"}, "OneTimePassword": {lastOTP}})
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "<Status>FAILED</Status>")
	smsSetTestTime(broker, now.Add(24*time.Hour))
	response = smsTestQuery(t, handler, "CreateSMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550100"}})
	require.Equal(t, http.StatusOK, response.Code)
	for i := 1; i < 10; i++ {
		response = smsTestQuery(t, handler, "CreateSMSSandboxPhoneNumber", url.Values{"PhoneNumber": {fmt.Sprintf("+120655501%02d", i)}})
		require.Equal(t, http.StatusOK, response.Code)
	}
	response = smsTestQuery(t, handler, "CreateSMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550200"}})
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Contains(t, response.Body.String(), "<Code>UserError</Code>")

	state.mu.Lock()
	state.sms.sandbox["+12065550100"].status = "Verified"
	state.mu.Unlock()
	broker.SetSNSCapture(NewSNSCapture(smsFailWriter{}))
	_, err = broker.publishSMS(SNSPublishInput{PhoneNumber: "+12065550100", Message: "not recorded"})
	require.Error(t, err)
	var serviceError *snsError
	require.ErrorAs(t, err, &serviceError)
	require.Equal(t, "InternalError", serviceError.Code)
}

func TestSNSSMSValidationAndPagination(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	handler := NewHandler(broker)
	for _, test := range []struct {
		action string
		values url.Values
	}{
		{"CheckIfPhoneNumberIsOptedOut", url.Values{"PhoneNumber": {"+12065550100"}}}, // Wrong case is not a required member.
		{"OptInPhoneNumber", url.Values{"phoneNumber": {"(206) 555-0100"}}},
		{"CreateSMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550100"}, "LanguageCode": {"xx-YY"}}},
		{"VerifySMSSandboxPhoneNumber", url.Values{"PhoneNumber": {"+12065550100"}, "OneTimePassword": {"bad"}}},
		{"SetSMSAttributes", url.Values{"attributes.entry.1.key": {"DefaultSMSType"}, "attributes.entry.1.value": {"Unknown"}}},
		{"SetSMSAttributes", url.Values{"attributes.entry.1.key": {"MonthlySpendLimit"}, "attributes.entry.1.value": {"NaN"}}},
		{"GetSMSAttributes", url.Values{"attributes.member.1": {"NotAnAttribute"}}},
		{"ListSMSSandboxPhoneNumbers", url.Values{"MaxResults": {"101"}}},
		{"ListSMSSandboxPhoneNumbers", url.Values{"NextToken": {"invalid"}}},
		{"ListOriginationNumbers", url.Values{"MaxResults": {"0"}}},
	} {
		t.Run(test.action+test.values.Encode(), func(t *testing.T) {
			response := smsTestQuery(t, handler, test.action, test.values)
			require.Equal(t, http.StatusBadRequest, response.Code)
		})
	}
	// Validate the whole update before committing any default settings.
	response := smsTestQuery(t, handler, "SetSMSAttributes", url.Values{
		"attributes.entry.1.key": {"MonthlySpendLimit"}, "attributes.entry.1.value": {"5"},
		"attributes.entry.2.key": {"DefaultSMSType"}, "attributes.entry.2.value": {"Invalid"},
	})
	require.Equal(t, http.StatusBadRequest, response.Code)
	state := broker.snsState()
	state.mu.Lock()
	require.Equal(t, "1", state.sms.attributes["MonthlySpendLimit"])
	for i := 0; i < 101; i++ {
		state.sms.optedOut[fmt.Sprintf("+1206555%04d", i)] = true
	}
	state.sms.origination["+12065550100"] = smsOriginationNumber{createdAt: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC), phoneNumber: "+12065550100", status: "ACTIVE", iso2CountryCode: "US", routeType: "Transactional", numberCapabilities: []string{"SMS", "VOICE"}}
	state.mu.Unlock()
	response = smsTestQuery(t, handler, "ListPhoneNumbersOptedOut", nil)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, 100, strings.Count(response.Body.String(), "<member>"))
	token := strings.Split(strings.Split(response.Body.String(), "<nextToken>")[1], "</nextToken>")[0]
	response = smsTestQuery(t, handler, "ListPhoneNumbersOptedOut", url.Values{"nextToken": {token}})
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, 1, strings.Count(response.Body.String(), "<member>"))
	require.NotContains(t, response.Body.String(), "<nextToken>")
	response = smsTestQuery(t, handler, "ListOriginationNumbers", nil)
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "<Iso2CountryCode>US</Iso2CountryCode>")
	require.Contains(t, response.Body.String(), "<CreatedAt>2026-10-07T00:00:00Z</CreatedAt>")
	require.Contains(t, response.Body.String(), "<NumberCapabilities><member>SMS</member><member>VOICE</member></NumberCapabilities>")
}

func smsSetTestTime(broker *Broker, now time.Time) {
	state := broker.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.sms.now = func() time.Time { return now }
}

func smsReadCapture(t *testing.T, path string) []SNSCaptureRecord {
	t.Helper()
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	var records []SNSCaptureRecord
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		if line == "" {
			continue
		}
		var record SNSCaptureRecord
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		records = append(records, record)
	}
	return records
}

func smsTestQuery(t *testing.T, handler *Handler, action string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, request.ParseForm())
	response := httptest.NewRecorder()
	require.True(t, handler.handleSNSSMSQuery(response, request, action))
	return response
}

type smsFailWriter struct{}

func (smsFailWriter) Write([]byte) (int, error) { return 0, errors.New("capture unavailable") }

func TestSNSSMSMessageLimitUsesResolvedCharacters(t *testing.T) {
	text := strings.Repeat("界", 1600)
	resolved, err := smsDeliveryMessage(text, "")
	require.NoError(t, err)
	require.Equal(t, text, resolved)
	_, err = smsDeliveryMessage(text+"界", "")
	require.ErrorContains(t, err, "1600")
	encoded, err := json.Marshal(map[string]string{"default": strings.Repeat("x", 2000), "sms": "short text"})
	require.NoError(t, err)
	resolved, err = smsDeliveryMessage(string(encoded), "json")
	require.NoError(t, err)
	require.Equal(t, "short text", resolved)
	_, err = smsDeliveryMessage(`{"default":"ok","sms":"`+strings.Repeat("x", 1601)+`"}`, "json")
	require.ErrorContains(t, err, "1600")
	resolved, err = smsDeliveryMessage(`{"default":"fallback","sms":12}`, "json")
	require.NoError(t, err)
	require.Equal(t, "fallback", resolved)
}
