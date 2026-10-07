package ssm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

func requireSSMError(t *testing.T, err error, code string) {
	t.Helper()
	var failure smithy.APIError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, code, failure.ErrorCode())
}

func TestSSMNativeSDKVersionsAndSecureStringReadChoices(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()
	put, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{
		Name: aws.String("/native/secret"), Value: aws.String("first-secret"), Type: ssmtypes.ParameterTypeSecureString,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), put.Version)
	require.Equal(t, ssmtypes.ParameterTierStandard, put.Tier)
	ciphertext, err := client.GetParameter(ctx, &ssmsdk.GetParameterInput{Name: aws.String("/native/secret")})
	require.NoError(t, err)
	require.NotEqual(t, "first-secret", aws.ToString(ciphertext.Parameter.Value))
	plaintext, err := client.GetParameter(ctx, &ssmsdk.GetParameterInput{Name: aws.String(" /native/secret "), WithDecryption: aws.Bool(true)})
	require.NoError(t, err)
	require.Equal(t, "first-secret", aws.ToString(plaintext.Parameter.Value))
	require.Equal(t, "text", aws.ToString(plaintext.Parameter.DataType))
	require.NotNil(t, plaintext.Parameter.LastModifiedDate)
	// Omitting Type on overwrite preserves the existing SecureString type.
	put, err = client.PutParameter(ctx, &ssmsdk.PutParameterInput{Name: aws.String("/native/secret"), Value: aws.String("second-secret"), Overwrite: aws.Bool(true)})
	require.NoError(t, err)
	require.Equal(t, int64(2), put.Version)
	historical, err := client.GetParameter(ctx, &ssmsdk.GetParameterInput{Name: aws.String("/native/secret:1"), WithDecryption: aws.Bool(true)})
	require.NoError(t, err)
	require.Equal(t, "first-secret", aws.ToString(historical.Parameter.Value))
	require.Equal(t, int64(1), historical.Parameter.Version)
	require.Equal(t, ":1", aws.ToString(historical.Parameter.Selector))
	latest, err := client.GetParameter(ctx, &ssmsdk.GetParameterInput{Name: aws.String("/native/secret"), WithDecryption: aws.Bool(true)})
	require.NoError(t, err)
	require.Equal(t, "second-secret", aws.ToString(latest.Parameter.Value))
	require.Equal(t, int64(2), latest.Parameter.Version)
	_, err = client.GetParameter(ctx, &ssmsdk.GetParameterInput{Name: aws.String("/native/secret:99")})
	requireSSMError(t, err, "ParameterVersionNotFound")
	_, err = client.GetParameter(ctx, &ssmsdk.GetParameterInput{Name: aws.String("/native/secret:label")})
	requireSSMError(t, err, "ValidationException")
	page, err := client.GetParametersByPath(ctx, &ssmsdk.GetParametersByPathInput{Path: aws.String("/native"), WithDecryption: aws.Bool(true)})
	require.NoError(t, err)
	require.Len(t, page.Parameters, 1)
	require.Equal(t, "second-secret", aws.ToString(page.Parameters[0].Value))
	_, err = client.PutParameter(ctx, &ssmsdk.PutParameterInput{Name: aws.String("/native/secret"), Value: aws.String("plain"), Type: ssmtypes.ParameterTypeString, Overwrite: aws.Bool(true)})
	requireSSMError(t, err, "HierarchyTypeMismatchException")
	_, err = client.DeleteParameter(ctx, &ssmsdk.DeleteParameterInput{Name: aws.String("/native/secret")})
	require.NoError(t, err)
	_, err = client.DeleteParameter(ctx, &ssmsdk.DeleteParameterInput{Name: aws.String("/native/secret")})
	requireSSMError(t, err, "ParameterNotFound")
}

func TestSSMNativeSDKHierarchyDefaultAndPagination(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()
	for _, name := range []string{"/tree", "/tree/a", "/tree/b", "/tree/c", "/tree/nested/a", "/tree/nested/b", "/trees/leak"} {
		_, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{Name: aws.String(name), Value: aws.String(name), Type: ssmtypes.ParameterTypeString})
		require.NoError(t, err)
	}
	direct, err := client.GetParametersByPath(ctx, &ssmsdk.GetParametersByPathInput{Path: aws.String("/tree")})
	require.NoError(t, err)
	require.Len(t, direct.Parameters, 3, "Recursive defaults to false")
	first, err := client.GetParametersByPath(ctx, &ssmsdk.GetParametersByPathInput{Path: aws.String("/tree/"), Recursive: aws.Bool(true), MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, first.Parameters, 2)
	require.NotEmpty(t, aws.ToString(first.NextToken))
	for _, input := range []*ssmsdk.GetParametersByPathInput{
		{Path: aws.String("/other"), Recursive: aws.Bool(true), NextToken: first.NextToken},
		{Path: aws.String("/tree"), Recursive: aws.Bool(false), NextToken: first.NextToken},
		{Path: aws.String("/tree"), Recursive: aws.Bool(true), WithDecryption: aws.Bool(true), NextToken: first.NextToken},
		{Path: aws.String("/tree"), Recursive: aws.Bool(true), NextToken: aws.String("forged-token")},
	} {
		_, err := client.GetParametersByPath(ctx, input)
		requireSSMError(t, err, "InvalidNextToken")
	}
	paginator := ssmsdk.NewGetParametersByPathPaginator(client, &ssmsdk.GetParametersByPathInput{Path: aws.String("/tree"), Recursive: aws.Bool(true), MaxResults: aws.Int32(2)})
	names := make([]string, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Parameters), 2)
		for _, parameter := range page.Parameters {
			names = append(names, aws.ToString(parameter.Name))
		}
	}
	require.Equal(t, []string{"/tree/a", "/tree/b", "/tree/c", "/tree/nested/a", "/tree/nested/b"}, names)
}

func TestSSMNativeSDKStandardLimitsAndStringList(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()
	_, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{Name: aws.String("/limit"), Value: aws.String(strings.Repeat("é", 2048)), Type: ssmtypes.ParameterTypeString})
	require.NoError(t, err, "4 KiB decoded value is valid")
	_, err = client.PutParameter(ctx, &ssmsdk.PutParameterInput{Name: aws.String("/limit-over"), Value: aws.String(strings.Repeat("é", 2049)), Type: ssmtypes.ParameterTypeString})
	requireSSMError(t, err, "ValidationException")
	_, err = client.PutParameter(ctx, &ssmsdk.PutParameterInput{Name: aws.String("/list"), Value: aws.String("one, two , three"), Type: ssmtypes.ParameterTypeStringList})
	require.NoError(t, err)
	result, err := client.GetParameter(ctx, &ssmsdk.GetParameterInput{Name: aws.String("/list")})
	require.NoError(t, err)
	require.Equal(t, "one,two,three", aws.ToString(result.Parameter.Value))
}

func ssmNativeRequest(t *testing.T, handler *Handler, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	request.Header.Set("X-Amz-Target", target)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, "application/x-amz-json-1.1", response.Header().Get("Content-Type"))
	require.NotEmpty(t, response.Header().Get("X-Amzn-RequestId"))
	return response
}

func TestSSMNativeProtocolRefusesDiscardedOptionsAndMalformedBodies(t *testing.T) {
	store := NewSSMStore()
	handler := NewHandler(store)
	for i, options := range []map[string]interface{}{
		{"AllowedPattern": "[a-z]+"}, {"KeyId": "custom-kms-key"}, {"DataType": "aws:ec2:image"},
		{"Tier": "Advanced"}, {"Policies": "[{\"Type\":\"Expiration\"}]"}, {"Tags": []map[string]string{{"Key": "owner", "Value": "test"}}},
		{"Overwrite": "true"}, {"Type": "unsupported"}, {"Description": strings.Repeat("x", 1025)},
	} {
		input := map[string]interface{}{"Name": fmt.Sprintf("/unsupported/%d", i), "Value": "value", "Type": "String"}
		for key, value := range options {
			input[key] = value
		}
		encoded, err := json.Marshal(input)
		require.NoError(t, err)
		response := ssmNativeRequest(t, handler, "AmazonSSM.PutParameter", string(encoded))
		require.Equal(t, http.StatusBadRequest, response.Code)
		require.NotEmpty(t, response.Header().Get("X-Amzn-ErrorType"))
		_, err = store.GetParameter(input["Name"].(string))
		require.True(t, errors.Is(err, errParameterNotFound))
	}
	for _, input := range []string{
		"null", "{}", "{", `{"Path":"/app//"}`, `{"Path":"/app","MaxResults":0}`, `{"Path":"/app","MaxResults":11}`,
		`{"Path":"/app","Recursive":"true"}`, `{"Path":"/app","ParameterFilters":[{"Key":"Type","Values":["String"]}]}`,
	} {
		response := ssmNativeRequest(t, handler, "AmazonSSM.GetParametersByPath", input)
		require.Equal(t, http.StatusBadRequest, response.Code, "body=%q response=%s", input, response.Body.String())
	}
	oversized := `{"Name":"/oversized","Value":"value","Type":"String"}` + strings.Repeat(" ", int(maxJSONRequestBytes))
	response := ssmNativeRequest(t, handler, "AmazonSSM.PutParameter", oversized)
	require.Equal(t, http.StatusBadRequest, response.Code)
	_, err := store.GetParameter("/oversized")
	require.ErrorIs(t, err, errParameterNotFound)
	response = ssmNativeRequest(t, handler, "OtherService.GetParameter", `{"Name":"/any"}`)
	require.Equal(t, "UnknownOperationException", response.Header().Get("X-Amzn-ErrorType"))
}

func TestSSMNativeDescriptionOmissionPreservesMetadata(t *testing.T) {
	store := NewSSMStore()
	handler := NewHandler(store)
	for _, body := range []string{
		`{"Name":"/description","Value":"first","Type":"String","Description":"retained"}`,
		`{"Name":"/description","Value":"second","Overwrite":true}`,
	} {
		response := ssmNativeRequest(t, handler, "AmazonSSM.PutParameter", body)
		require.Equal(t, http.StatusOK, response.Code, "response=%s", response.Body.String())
	}
	parameter, err := store.GetParameter("/description")
	require.NoError(t, err)
	require.Equal(t, "retained", parameter.Description)
	require.Equal(t, int64(2), parameter.Version)
}
