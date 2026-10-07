//go:build sdksmoke

package sdk

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/scheduler"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This fixture composes the same typed Lambda admission port as the application;
// neither SDK traffic nor execution uses a mocked target or a shared stack.
type schedulerSDKTarget struct{ service *lambda.Service }

func (target schedulerSDKTarget) ValidateTarget(ctx context.Context, arn string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return target.service.ValidateTarget(arn, "")
}
func (target schedulerSDKTarget) AdmitTarget(ctx context.Context, arn string, payload []byte) error {
	_, err := target.service.Admit(ctx, lambda.InvokeInput{FunctionName: arn, Payload: payload})
	return err
}

type schedulerSDKFixture struct {
	endpoint, output, gate string
	lambda                 *lambda.Service
}

func newSchedulerSDKFixture(t *testing.T, node string) *schedulerSDKFixture {
	t.Helper()
	directory := t.TempDir()
	fixture := &schedulerSDKFixture{output: filepath.Join(directory, "delivered.jsonl"), gate: filepath.Join(directory, "gate")}
	source := "import fs from 'node:fs';\n" +
		"async function execute(kind,event) { if(event.block) while(!fs.existsSync(process.env.GATE)) await new Promise(resolve=>setTimeout(resolve,5)); fs.appendFileSync(process.env.OUTPUT,JSON.stringify({kind,event})+'\\n'); return {ok:true}; }\n" +
		"export const base=event=>execute('base',event);\nexport const alias=event=>execute('alias',event);\n"
	require.NoError(t, os.WriteFile(filepath.Join(directory, "handler.mjs"), []byte(source), 0600))
	function := lambda.Function{Runtime: "node", Command: []string{node}, Handler: "handler.mjs#base", Timeout: 15 * time.Second, Environment: map[string]string{"OUTPUT": fixture.output, "GATE": fixture.gate}}
	alias := function
	alias.Handler = "handler.mjs#alias"
	lambdaService, err := lambda.NewService(&lambda.Config{
		Functions: map[string]lambda.Function{"scheduled": function, "scheduled:live": alias},
		DevAsync:  &lambda.DevAsyncConfig{Workers: 1, Capacity: 8, RetryDelays: []time.Duration{0, 0}, LogPath: filepath.Join(directory, "lambda.jsonl")},
	}, directory)
	require.NoError(t, err)
	fixture.lambda = lambdaService
	schedules, err := scheduler.New(scheduler.Options{Region: "us-east-1", AccountID: "000000000000", Dev: scheduler.DevOptions{Groups: []string{"owned"}, ExactSeconds: true, RetryDelay: 10 * time.Millisecond, MaxSchedules: 32}}, schedulerSDKTarget{lambdaService})
	require.NoError(t, err)
	serving := httptest.NewServer(server.New(server.Services{Lambda: lambdaService, Scheduler: scheduler.NewHandler(schedules)}))
	fixture.endpoint = serving.URL
	t.Cleanup(func() {
		serving.Close()
		// Release only this fixture's handler gate even when the JS assertion
		// failed, then stop scheduling before draining Lambda-owned events.
		require.NoError(t, os.WriteFile(fixture.gate, []byte("cleanup"), 0600))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, schedules.Close(ctx))
		require.NoError(t, lambdaService.Close(ctx))
	})
	return fixture
}

func TestSchedulerJavascriptSDKSmoke(t *testing.T) {
	node := sdkNode(t)
	first, second := newSchedulerSDKFixture(t, node), newSchedulerSDKFixture(t, node)
	environment := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"SCHEDULER_ENDPOINT_URL="+first.endpoint,
		"SCHEDULER_OTHER_ENDPOINT_URL="+second.endpoint,
		"SCHEDULER_OUTPUT_FILE="+first.output,
		"SCHEDULER_OTHER_OUTPUT_FILE="+second.output,
		"SCHEDULER_GATE_FILE="+first.gate,
	)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	output, err := runJavascriptSDKProcess(ctx, node, fixturePath("javascript", "scheduler.mjs"), nil, environment)
	t.Logf("real Scheduler JavaScript SDK proof:\n%s", output)
	require.NoError(t, err)
	for _, fixture := range []*schedulerSDKFixture{first, second} {
		require.Eventually(t, func() bool {
			records := fixture.lambda.AsyncSnapshot()
			return len(records) == 1 && records[0].State == "succeeded"
		}, 5*time.Second, 10*time.Millisecond)
		records := fixture.lambda.AsyncSnapshot()
		require.Len(t, records, 1)
		assert.Equal(t, "scheduled:live", records[0].FunctionName)
		assert.Equal(t, 1, records[0].Attempts, "successful admission must execute exactly once")
	}
}
