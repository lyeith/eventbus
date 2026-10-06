package ssm

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupSSMClient(t *testing.T) (*httptest.Server, *ssm.Client) {
	t.Helper()

	ts := httptest.NewServer(NewHandler(NewSSMStore()))
	t.Cleanup(ts.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := ssm.NewFromConfig(cfg, func(o *ssm.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
	})
	return ts, client
}

func TestSSMPutParameter(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()

	result, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/local/services/directory/url"),
		Value: aws.String("http://localhost:8006"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Version)
}

func TestSSMPutParameterOverwrite(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()

	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/test/param"),
		Value: aws.String("value1"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err)

	// Without overwrite, should fail
	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/test/param"),
		Value: aws.String("value2"),
		Type:  ssmtypes.ParameterTypeString,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ParameterAlreadyExists")

	// With overwrite, should succeed
	_, err = client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String("/test/param"),
		Value:     aws.String("value2"),
		Type:      ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})
	require.NoError(t, err)
}

func TestSSMGetParameter(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()

	// Put first
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/local/services/files/url"),
		Value: aws.String("http://localhost:8001"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err)

	// Get
	result, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name: aws.String("/local/services/files/url"),
	})
	require.NoError(t, err)
	assert.Equal(t, "/local/services/files/url", *result.Parameter.Name)
	assert.Equal(t, "http://localhost:8001", *result.Parameter.Value)
	assert.Equal(t, ssmtypes.ParameterTypeString, result.Parameter.Type)
}

func TestSSMGetParameterNotFound(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()

	_, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name: aws.String("/nonexistent/param"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ParameterNotFound")
}

func TestSSMGetParametersByPath(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()

	// Put multiple parameters under same path
	services := map[string]string{
		"/local/services/directory/url": "http://localhost:8006",
		"/local/services/files/url":     "http://localhost:8001",
		"/local/services/audit/url":     "http://localhost:8007",
	}
	for name, value := range services {
		_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  aws.String(name),
			Value: aws.String(value),
			Type:  ssmtypes.ParameterTypeString,
		})
		require.NoError(t, err)
	}

	// Also put one outside the path
	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/other/param"),
		Value: aws.String("other"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err)

	// Get by path
	result, err := client.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
		Path:      aws.String("/local/services/"),
		Recursive: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Len(t, result.Parameters, 3)

	names := make(map[string]bool)
	for _, p := range result.Parameters {
		names[*p.Name] = true
	}
	assert.True(t, names["/local/services/directory/url"])
	assert.True(t, names["/local/services/files/url"])
	assert.True(t, names["/local/services/audit/url"])
	assert.False(t, names["/other/param"])
}

func TestSSMDeleteParameter(t *testing.T) {
	_, client := setupSSMClient(t)
	ctx := context.Background()

	_, err := client.PutParameter(ctx, &ssm.PutParameterInput{
		Name:  aws.String("/delete/me"),
		Value: aws.String("value"),
		Type:  ssmtypes.ParameterTypeString,
	})
	require.NoError(t, err)

	_, err = client.DeleteParameter(ctx, &ssm.DeleteParameterInput{
		Name: aws.String("/delete/me"),
	})
	require.NoError(t, err)

	_, err = client.GetParameter(ctx, &ssm.GetParameterInput{
		Name: aws.String("/delete/me"),
	})
	assert.Error(t, err)
}
