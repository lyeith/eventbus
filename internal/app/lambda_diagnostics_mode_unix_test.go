//go:build linux || darwin

package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type diagnosticsModeObservation struct {
	RequestID string          `json:"request_id"`
	MessageID string          `json:"message_id"`
	Attempt   int             `json:"attempt"`
	PID       int             `json:"pid"`
	Event     json.RawMessage `json:"event"`
}

// This real provided-runtime handler fails once as an application would, then
// returns success. The native Lambda async owner alone decides whether to retry.
func TestDiagnosticsModeProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_DIAGNOSTICS_MODE_PROCESS") != "1" {
		return
	}
	if err := diagnosticsModeInvocation(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func diagnosticsModeInvocation() error {
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	api := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	response, err := client.Get(api + "next")
	if err != nil {
		return err
	}
	payload, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	id := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	var event struct {
		Records []struct {
			Source string `json:"EventSource"`
			SNS    struct {
				MessageID string `json:"MessageId"`
			} `json:"Sns"`
		} `json:"Records"`
	}
	if readErr != nil || response.StatusCode != http.StatusOK || id == "" || json.Unmarshal(payload, &event) != nil || len(event.Records) != 1 || event.Records[0].Source != "aws:sns" {
		return errors.New("real handler expected native SNS event")
	}
	root := os.Getenv("DIAGNOSTICS_MODE_ROOT")
	first, err := os.OpenFile(filepath.Join(root, "first-"+id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	attempt := 1
	if err == nil {
		_ = first.Close()
	} else if errors.Is(err, os.ErrExist) {
		attempt = 2
	} else {
		return err
	}
	observation := diagnosticsModeObservation{id, event.Records[0].SNS.MessageID, attempt, os.Getpid(), payload}
	data, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	path := filepath.Join(root, fmt.Sprintf("attempt-%d.json", attempt))
	if err = os.WriteFile(path+".tmp", data, 0600); err != nil {
		return err
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	operation, result := "response", `{"handled":true}`
	if attempt == 1 {
		fmt.Fprintln(os.Stderr, "private failed handler output")
		operation, result = "error", `{"errorType":"Application.ExpectedFailure","errorMessage":"private failed handler detail"}`
	} else {
		fmt.Fprintln(os.Stderr, "private successful handler output")
	}
	ack, err := client.Post(api+id+"/"+operation, "application/json", strings.NewReader(result))
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("native runtime %s status %d", operation, ack.StatusCode)
	}
	return nil
}

func TestCLIFailureOnlyDiagnosticsPreserveNativeSNSRetries(t *testing.T) {
	for _, scenario := range []struct {
		name string
		mode lambdaservice.DevDiagnosticsMode
		rows int
	}{{"omitted-all", "", 2}, {"failures", lambdaservice.DevDiagnosticsFailures, 1}} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			port := cliStartupPort(t)
			endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
			executable, err := os.Executable()
			require.NoError(t, err)
			asyncPath, diagnosticPath := filepath.Join(directory, "async.jsonl"), filepath.Join(directory, "private-diagnostics.jsonl")
			recipe, err := yaml.Marshal(lambdaservice.Config{Functions: map[string]lambdaservice.Function{"mode-handler:live": {
				Runtime: "provided", Command: []string{executable, "-test.run=^TestDiagnosticsModeProvidedProcess$"}, Timeout: 10 * time.Second,
				Environment: map[string]string{"EVENTBUS_DIAGNOSTICS_MODE_PROCESS": "1", "DIAGNOSTICS_MODE_ROOT": directory, "PRIVATE_ENVIRONMENT": "private environment must not enter diagnostic"},
			}}, DevAsync: &lambdaservice.DevAsyncConfig{Workers: 1, Capacity: 4, RetryDelays: []time.Duration{25 * time.Millisecond, 25 * time.Millisecond}, LogPath: asyncPath},
				DevDiagnostics: &lambdaservice.DevDiagnosticsConfig{LogPath: diagnosticPath, Mode: scenario.mode}})
			require.NoError(t, err)
			functionsPath := filepath.Join(directory, "functions.yaml")
			require.NoError(t, os.WriteFile(functionsPath, recipe, 0600))
			args, _ := cliStartupArguments(directory, port)
			args = append(args, "--issuer-base", endpoint, "--lambda-functions", functionsPath)
			command := exec.Command(executable, append([]string{"-test.run=^TestCLIStartupProcess$", "--"}, args...)...)
			if binary := os.Getenv("EVENTBUS_SMOKE_BINARY"); binary != "" {
				require.True(t, filepath.IsAbs(binary), "EVENTBUS_SMOKE_BINARY selects an absolute owned packaged executable")
				info, err := os.Stat(binary)
				require.NoError(t, err)
				require.False(t, info.IsDir())
				command = exec.Command(binary, args...)
				t.Logf("packaged EventBus native SNS diagnostic proof: %s", binary)
			}
			command.Dir = directory
			command.Env = append(os.Environ(), "EVENTBUS_CLI_STARTUP_CHILD=1")
			command.WaitDelay = time.Second
			logPath := filepath.Join(directory, "cli.log")
			log, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err)
			command.Stdout, command.Stderr = log, log
			require.NoError(t, command.Start())
			done := make(chan struct{})
			var waitErr error
			go func() { waitErr = command.Wait(); close(done) }()
			client := &http.Client{Timeout: time.Second}
			t.Cleanup(func() {
				_ = command.Process.Signal(syscall.SIGTERM)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					_ = command.Process.Kill()
					<-done
				}
				client.CloseIdleConnections()
				_ = log.Close()
				if t.Failed() {
					data, _ := os.ReadFile(logPath)
					t.Logf("owned CLI output:\n%s", data)
				}
			})
			require.Eventually(t, func() bool {
				response, err := client.Get(endpoint + "/health")
				if err != nil {
					return false
				}
				defer response.Body.Close()
				var health struct {
					Issuer string `json:"cognito_issuer"`
				}
				return response.StatusCode == http.StatusOK && json.NewDecoder(response.Body).Decode(&health) == nil && health.Issuer == endpoint
			}, 5*time.Second, 10*time.Millisecond, "owned CLI did not become ready")
			sdk := snsLifecycleSDK(endpoint, client)
			topic, err := sdk.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("diagnostics-mode")})
			require.NoError(t, err)
			subscribed, err := sdk.Subscribe(t.Context(), &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("lambda"), Endpoint: aws.String("arn:aws:lambda:us-east-1:000000000000:function:mode-handler:live")})
			require.NoError(t, err)
			published, err := sdk.Publish(t.Context(), &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String(`{"private":"private native event must not enter diagnostic"}`)})
			require.NoError(t, err)
			readLines := func(path string) [][]byte {
				data, err := os.ReadFile(path)
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				require.NoError(t, err)
				lines := bytes.Split(data, []byte{'\n'})
				return lines[:len(lines)-1]
			}
			asyncRows := func() []lambdaservice.AsyncRecord {
				var rows []lambdaservice.AsyncRecord
				for _, line := range readLines(asyncPath) {
					var row lambdaservice.AsyncRecord
					require.NoError(t, json.Unmarshal(line, &row))
					rows = append(rows, row)
				}
				return rows
			}
			require.Eventually(t, func() bool { rows := asyncRows(); return len(rows) > 0 && rows[len(rows)-1].State == "succeeded" }, 10*time.Second, 5*time.Millisecond, "native async retry never completed")
			_, err = sdk.Unsubscribe(t.Context(), &sns.UnsubscribeInput{SubscriptionArn: subscribed.SubscriptionArn})
			require.NoError(t, err)
			_, err = sdk.DeleteTopic(t.Context(), &sns.DeleteTopicInput{TopicArn: topic.TopicArn})
			require.NoError(t, err)
			// Joining the real CLI closes native owners and the private sink before
			// checking absence: a readable async append alone is not that barrier.
			require.NoError(t, command.Process.Signal(syscall.SIGTERM))
			select {
			case <-done:
				require.NoError(t, waitErr)
			case <-time.After(5 * time.Second):
				t.Fatal("native CLI did not join diagnostic proof shutdown")
			}
			var observed []diagnosticsModeObservation
			for _, attempt := range []int{1, 2} {
				data, err := os.ReadFile(filepath.Join(directory, fmt.Sprintf("attempt-%d.json", attempt)))
				require.NoError(t, err)
				var row diagnosticsModeObservation
				require.NoError(t, json.Unmarshal(data, &row))
				require.Equal(t, attempt, row.Attempt)
				require.Equal(t, aws.ToString(published.MessageId), row.MessageID)
				require.Positive(t, row.PID)
				observed = append(observed, row)
			}
			require.Equal(t, observed[0].RequestID, observed[1].RequestID, "SNS native async identity persists across its real retry")
			require.Equal(t, observed[0].Event, observed[1].Event, "native retry retains exact original SNS event")
			rows := asyncRows()
			require.Len(t, rows, 5)
			for index, state := range []string{"queued", "running", "retrying", "running", "succeeded"} {
				require.Equal(t, state, rows[index].State)
				require.Equal(t, observed[0].RequestID, rows[index].RequestID)
				require.Equal(t, "mode-handler:live", rows[index].FunctionName)
			}
			require.Equal(t, 1, rows[2].Attempts)
			require.Equal(t, "FunctionError", rows[2].ErrorType)
			require.Equal(t, 2, rows[4].Attempts)
			require.Empty(t, rows[4].ErrorType)
			type diagnostic struct {
				RequestID          string `json:"request_id"`
				Attempt            int    `json:"attempt"`
				State              string `json:"state"`
				OwnershipConfirmed bool   `json:"ownership_confirmed"`
				FunctionError      bool   `json:"function_error"`
				Stderr             struct {
					Data string `json:"data"`
				} `json:"stderr"`
				FunctionDiagnostic struct {
					Data string `json:"data"`
				} `json:"function_diagnostic"`
			}
			var diagnostics []diagnostic
			for _, line := range readLines(diagnosticPath) {
				var row diagnostic
				require.NoError(t, json.Unmarshal(line, &row))
				diagnostics = append(diagnostics, row)
			}
			require.Len(t, diagnostics, scenario.rows)
			failed := diagnostics[0]
			require.Equal(t, observed[0].RequestID, failed.RequestID)
			require.Equal(t, 1, failed.Attempt)
			require.Equal(t, "failed", failed.State)
			require.True(t, failed.OwnershipConfirmed)
			require.True(t, failed.FunctionError)
			require.Contains(t, failed.Stderr.Data, "private failed handler output")
			require.Contains(t, failed.FunctionDiagnostic.Data, "private failed handler detail")
			if scenario.rows == 2 {
				successful := diagnostics[1]
				require.Equal(t, observed[0].RequestID, successful.RequestID)
				require.Equal(t, 2, successful.Attempt)
				require.Equal(t, "succeeded", successful.State)
				require.True(t, successful.OwnershipConfirmed)
				require.False(t, successful.FunctionError)
				require.Contains(t, successful.Stderr.Data, "private successful handler output")
			}
			data, err := os.ReadFile(diagnosticPath)
			require.NoError(t, err)
			require.NotContains(t, string(data), "private native event must not enter diagnostic")
			require.NotContains(t, string(data), "private environment must not enter diagnostic")
			if scenario.rows == 1 {
				require.NotContains(t, string(data), "private successful handler output")
			}
			admissions := 0
			for _, line := range readLines(filepath.Join(directory, "sns.jsonl")) {
				var row struct {
					Operation  string `json:"operation"`
					MessageID  string `json:"message_id"`
					Deliveries []struct {
						Status    string `json:"status"`
						RequestID string `json:"invocation_request_id"`
					} `json:"deliveries"`
				}
				require.NoError(t, json.Unmarshal(line, &row))
				if row.MessageID != aws.ToString(published.MessageId) {
					continue
				}
				require.NotEqual(t, "DeliveryFailure", row.Operation)
				if row.Operation == "DeliveryAdmission" {
					require.Len(t, row.Deliveries, 1)
					require.Equal(t, "admitted", row.Deliveries[0].Status)
					require.Equal(t, observed[0].RequestID, row.Deliveries[0].RequestID)
					admissions++
				}
			}
			require.Equal(t, 1, admissions, "SNS admission must not be duplicated by Lambda retry")
			for _, row := range observed {
				require.ErrorIs(t, syscall.Kill(row.PID, 0), syscall.ESRCH, "real attempt worker remained after native owner join")
			}
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			require.NoError(t, err)
			require.NoError(t, listener.Close())
		})
	}
}
