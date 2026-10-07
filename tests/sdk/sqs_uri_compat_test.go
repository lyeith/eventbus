//go:build sdksmoke

package sdk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Check the installed, unmodified SDK model rather than substituting a URI or
// overriding AWS_DATA_PATH. The current lane keeps its existing frozen SDK and
// full TestSQSBatchPythonSDKSmoke; the optional older lane runs that same proof.
func checkSQSMappingSDKURI(t *testing.T, python, botoVersion, coreVersion, uri string) {
	t.Helper()
	script := filepath.Join(t.TempDir(), "sdk_model.py")
	body := fmt.Sprintf(`import boto3
import botocore
import botocore.session
assert boto3.__version__ == %q, boto3.__version__
assert botocore.__version__ == %q, botocore.__version__
uri = botocore.session.get_session().get_service_model("lambda").operation_model("CreateEventSourceMapping").http["requestUri"]
assert uri == %q, uri
print("boto3=" + boto3.__version__ + " botocore=" + botocore.__version__ + " CreateEventSourceMapping=" + uri)
print("PASS")
`, botoVersion, coreVersion, uri)
	require.NoError(t, os.WriteFile(script, []byte(body), 0600))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, script, sdkEnvironment(t.TempDir(), "", "", ""))
	t.Logf("installed SDK model:\n%s", output)
	require.NoError(t, err)
}

func TestSQSMappingCurrentPythonSDKURI(t *testing.T) {
	// uv.lock freezes boto3 1.40.61 with botocore 1.40.76. Both that installed
	// model and upstream botocore 1.40.61 use the exact slashless collection.
	checkSQSMappingSDKURI(t, sdkPython(t), "1.40.61", "1.40.76", "/2015-03-31/event-source-mappings")
}

func TestSQSMappingLegacyPythonSDKSmoke(t *testing.T) {
	python := os.Getenv("EVENTBUS_SMOKE_PYTHON_LEGACY")
	if python == "" {
		t.Skip("set EVENTBUS_SMOKE_PYTHON_LEGACY to an existing boto3/botocore 1.39.4 environment")
	}
	require.True(t, filepath.IsAbs(python), "legacy SDK executable must be absolute")
	info, err := os.Stat(python)
	require.NoError(t, err)
	require.False(t, info.IsDir())
	checkSQSMappingSDKURI(t, python, "1.39.4", "1.39.4", "/2015-03-31/event-source-mappings/")
	first, second := newSQSBatchSDKFixture(t, python, "A"), newSQSBatchSDKFixture(t, python, "B")
	env := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"SQS_BATCH_ENDPOINT_A="+first.serving.URL, "SQS_BATCH_ROOT_A="+first.root,
		"SQS_BATCH_ENDPOINT_B="+second.serving.URL, "SQS_BATCH_ROOT_B="+second.root)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "sqs_batch_smoke.py"), env)
	t.Logf("unmodified legacy boto3 batch/concurrency/retry/joined-teardown proof:\n%s", output)
	require.NoError(t, err)
}
