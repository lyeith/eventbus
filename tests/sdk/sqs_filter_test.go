//go:build sdksmoke

package sdk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSQSFilteringPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	fixture := newSQSBatchSDKFixture(t, python, "A")
	env := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"SQS_FILTER_ENDPOINT="+fixture.serving.URL, "SQS_FILTER_ROOT="+fixture.root)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "sqs_filter_smoke.py"), env)
	t.Logf("real boto3 SQS filtering proof:\n%s", output)
	require.NoError(t, err)
}
