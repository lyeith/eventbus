package secrets

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/require"
)

func secretsSDK(endpoint string) *secretsmanager.Client {
	return secretsmanager.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1}, func(options *secretsmanager.Options) { options.BaseEndpoint = aws.String(endpoint) })
}
func TestSecretsSDKMetadataBinaryStagesAndTypedErrors(t *testing.T) {
	serving, client := setupSecretsClient(t)
	ctx := context.Background()
	created, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String("metadata-sdk"), Description: aws.String("owned metadata")})
	require.NoError(t, err)
	require.Nil(t, created.VersionId)
	described, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: created.ARN})
	require.NoError(t, err)
	require.Empty(t, described.VersionIdsToStages)
	require.Nil(t, described.RotationEnabled)
	require.Equal(t, "owned metadata", aws.ToString(described.Description))
	_, err = client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: created.ARN})
	var missing *smtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &missing)

	originalToken, pendingToken := strings.Repeat("1", 32), strings.Repeat("2", 32)
	original, err := client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: created.ARN, ClientRequestToken: aws.String(originalToken), SecretBinary: []byte{0, 255, 10, 128}})
	require.NoError(t, err)
	require.Equal(t, []string{"AWSCURRENT"}, original.VersionStages)
	_, err = client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: created.ARN, VersionStage: aws.String("AWSPREVIOUS")})
	require.ErrorAs(t, err, &missing)
	pending, err := client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: created.ARN, ClientRequestToken: aws.String(pendingToken), SecretString: aws.String("candidate"), VersionStages: []string{"AWSPENDING", "CUSTOM"}})
	require.NoError(t, err)
	require.Equal(t, []string{"AWSPENDING", "CUSTOM"}, pending.VersionStages)
	current, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: created.ARN})
	require.NoError(t, err)
	require.Equal(t, []byte{0, 255, 10, 128}, current.SecretBinary)
	require.Nil(t, current.SecretString)
	require.Equal(t, originalToken, aws.ToString(current.VersionId))
	_, err = client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: created.ARN, VersionId: aws.String(originalToken), VersionStage: aws.String("AWSPENDING")})
	var invalid *smtypes.InvalidParameterException
	require.ErrorAs(t, err, &invalid)
	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: created.ARN, ClientRequestToken: aws.String(pendingToken), SecretString: aws.String("different")})
	var conflict *smtypes.ResourceExistsException
	require.ErrorAs(t, err, &conflict)
	_, err = client.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{SecretId: created.ARN, VersionStage: aws.String("AWSCURRENT"), MoveToVersionId: aws.String(pendingToken), RemoveFromVersionId: aws.String(originalToken)})
	require.NoError(t, err)
	previous, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: created.ARN, VersionStage: aws.String("AWSPREVIOUS")})
	require.NoError(t, err)
	require.Equal(t, originalToken, aws.ToString(previous.VersionId))
	require.Equal(t, []byte{0, 255, 10, 128}, previous.SecretBinary)
	update, err := client.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{SecretId: created.ARN, Description: aws.String("updated description")})
	require.NoError(t, err)
	require.Nil(t, update.VersionId)
	described, err = client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: created.ARN})
	require.NoError(t, err)
	require.Len(t, described.VersionIdsToStages, 2)
	require.Equal(t, "updated description", aws.ToString(described.Description))

	request, err := http.NewRequest(http.MethodPost, serving.URL, strings.NewReader(`{"SecretId":"metadata-sdk"}`))
	require.NoError(t, err)
	request.Header.Set("X-Amz-Target", "secretsmanager.DescribeSecret")
	response, err := serving.Client().Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, "application/x-amz-json-1.1", response.Header.Get("Content-Type"))
	require.NotEmpty(t, response.Header.Get("x-amzn-RequestId"))
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &raw))
	require.NotContains(t, raw, "SecretString")
	require.NotContains(t, raw, "SecretBinary")
	require.NotContains(t, string(data), "candidate")
}

func TestSecretsSDKOperationBoundsAndPasswords(t *testing.T) {
	_, client := setupSecretsClient(t)
	ctx := context.Background()
	nativeLimit := bytes.Repeat([]byte{255}, 65536)
	created, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String("large-binary"), SecretBinary: nativeLimit})
	require.NoError(t, err)
	got, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: created.ARN})
	require.NoError(t, err)
	require.Equal(t, nativeLimit, got.SecretBinary)
	_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: created.ARN, SecretBinary: append(nativeLimit, 0)})
	var invalid *smtypes.InvalidParameterException
	require.ErrorAs(t, err, &invalid)
	generated, err := client.GetRandomPassword(ctx, &secretsmanager.GetRandomPasswordInput{PasswordLength: aws.Int64(24), ExcludePunctuation: aws.Bool(true), ExcludeCharacters: aws.String("0Oo1Il"), RequireEachIncludedType: aws.Bool(true)})
	require.NoError(t, err)
	require.Len(t, aws.ToString(generated.RandomPassword), 24)
	require.False(t, strings.ContainsAny(aws.ToString(generated.RandomPassword), "0Oo1Il"))
	_, err = client.GetRandomPassword(ctx, &secretsmanager.GetRandomPasswordInput{PasswordLength: aws.Int64(1)})
	require.ErrorAs(t, err, &invalid)
}
