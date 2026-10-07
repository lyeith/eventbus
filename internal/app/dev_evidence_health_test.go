package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/eventsource"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

type devEvidenceUnavailableWriter struct{ failure error }

func (writer devEvidenceUnavailableWriter) Write([]byte) (int, error) { return 0, writer.failure }

func TestDevInvocationEvidenceMakesCaptureFailureStrictWithoutClosingOwners(t *testing.T) {
	require.NoError(t, devInvocationEvidence(nil, nil))
	directory := t.TempDir()
	executable, err := os.Executable()
	require.NoError(t, err)
	functions, err := lambdaservice.NewService(&lambdaservice.Config{Functions: map[string]lambdaservice.Function{"health": {
		Runtime: "provided", Command: []string{executable, "-test.run=^TestDevEvidenceProvidedProcess$"}, Timeout: time.Second,
		Environment: map[string]string{"EVENTBUS_DEV_EVIDENCE_PROCESS": "1", "DEV_EVIDENCE_ROLE": "consumer", "DEV_EVIDENCE_DIRECTORY": directory, "AWS_ENDPOINT_URL_SQS": "http://127.0.0.1:1"},
	}}}, directory)
	require.NoError(t, err)
	t.Cleanup(func() { _ = functions.Close(context.Background()) })
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	queue := broker.CreateQueue("health-evidence", time.Minute, time.Hour)
	_, err = broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: `{"suite":"one"}`})
	require.NoError(t, err)
	captureFailure := errors.New("owned evidence writer failed")
	mappings, err := eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000", Dev: eventsource.DevOptions{
		DeliveryCapture: &eventsource.DevDeliveryCaptureConfig{LogWriter: devEvidenceUnavailableWriter{failure: captureFailure}},
	}}, sqsMappingSource{broker: broker}, eventSourceLambdaInvoker{runtime: functions})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mappings.Close(context.Background()) })
	batchSize := 1
	_, err = mappings.Create(t.Context(), eventsource.CreateInput{EventSourceARN: queue.ARN, FunctionName: "arn:aws:lambda:us-east-1:000000000000:function:health", BatchSize: &batchSize})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return errors.Is(devInvocationEvidence(functions, mappings), captureFailure) }, time.Second, time.Millisecond)
	// This coordinator receives no activity observer; only the production health
	// composition can prevent zero-count evidence from becoming fixture-safe.
	owner := devquiescence.New(func() error { return devInvocationEvidence(functions, mappings) })
	snapshot, err := owner.Quiesce(t.Context())
	require.ErrorIs(t, err, devquiescence.ErrEvidence)
	require.False(t, snapshot.FixtureSafe)
	_, err = owner.Resume(snapshot.Generation)
	require.ErrorIs(t, err, devquiescence.ErrEvidence)
	require.ErrorIs(t, devInvocationEvidence(functions, mappings), captureFailure, "checking retained health must not close or clear the failing sink")
	_, err = os.Stat(filepath.Join(directory, "consumer-launched"))
	require.ErrorIs(t, err, os.ErrNotExist, "failed admission capture must prevent real child launch")
}
