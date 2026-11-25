package eventsource

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReportBatchItemFailuresSettlesOnlySuccessfulReceipts(t *testing.T) {
	const oneFailure = `{"batchItemFailures":[{"itemIdentifier":"item-1"}]}`
	for _, tc := range []struct {
		name      string
		enabled   bool
		response  string
		invokeErr error
		result    string
		deleted   []int
	}{
		{name: "selected failure", enabled: true, response: oneFailure, result: "Partial batch failure", deleted: []int{0, 2}},
		{name: "all failed", enabled: true, response: `{"batchItemFailures":[{"itemIdentifier":"item-0"},{"itemIdentifier":"item-1"},{"itemIdentifier":"item-2"}]}`, result: "Partial batch failure"},
		{name: "option disabled", response: oneFailure, result: "OK", deleted: []int{0, 1, 2}},
		{name: "option disabled ignores malformed output", response: "not JSON", result: "OK", deleted: []int{0, 1, 2}},
		{name: "empty object", enabled: true, response: `{}`, result: "OK", deleted: []int{0, 1, 2}},
		{name: "null response", enabled: true, response: `null`, result: "OK", deleted: []int{0, 1, 2}},
		{name: "empty failures", enabled: true, response: `{"batchItemFailures":[]}`, result: "OK", deleted: []int{0, 1, 2}},
		{name: "null failures", enabled: true, response: `{"batchItemFailures":null}`, result: "OK", deleted: []int{0, 1, 2}},
		{name: "malformed JSON", enabled: true, response: `{"batchItemFailures":`, result: "Invalid batch response"},
		{name: "trailing JSON", enabled: true, response: oneFailure + " null", result: "Invalid batch response"},
		{name: "invalid top-level type", enabled: true, response: `[]`, result: "Invalid batch response"},
		{name: "malformed failures", enabled: true, response: `{"batchItemFailures":{"itemIdentifier":"item-1"}}`, result: "Invalid batch response"},
		{name: "null item", enabled: true, response: `{"batchItemFailures":[null]}`, result: "Invalid batch response"},
		{name: "wrong item key", enabled: true, response: `{"batchItemFailures":[{"messageId":"item-1"}]}`, result: "Invalid batch response"},
		{name: "null item ID", enabled: true, response: `{"batchItemFailures":[{"itemIdentifier":null}]}`, result: "Invalid batch response"},
		{name: "empty item ID", enabled: true, response: `{"batchItemFailures":[{"itemIdentifier":""}]}`, result: "Invalid batch response"},
		{name: "receipt is not a message ID", enabled: true, response: `{"batchItemFailures":[{"itemIdentifier":"item-1-receipt-1"}]}`, result: "Invalid batch response"},
		{name: "unknown ID invalidates entire response", enabled: true, response: `{"batchItemFailures":[{"itemIdentifier":"item-1"},{"itemIdentifier":"other-batch"}]}`, result: "Invalid batch response"},
		{name: "function error overrides valid response", enabled: true, response: oneFailure, invokeErr: errors.New("handler failed"), result: "Function invocation failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := batchInput(3, 0)
			if tc.enabled {
				input.FunctionResponseTypes = []string{"ReportBatchItemFailures"}
			}
			batch := leasedBatch("item", 3, 1)
			q := newFakeQueue()
			q.batches <- batch
			s := newTestService(t, q, fakeInvoker{result: []byte(tc.response), invoke: func(context.Context, string, []byte) error {
				return tc.invokeErr
			}})
			mapping, err := s.Create(t.Context(), input)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				got, err := s.Get(mapping.UUID)
				return err == nil && got.LastProcessingResult == tc.result
			}, time.Second, time.Millisecond)
			require.NoError(t, s.Close(t.Context()))
			require.Len(t, q.deleted, len(tc.deleted))
			for _, index := range tc.deleted {
				require.Equal(t, batch[index].ReceiptHandle, <-q.deleted)
			}
		})
	}
}

func TestReportBatchItemFailuresObservedDeliveryEvidence(t *testing.T) {
	const report = `{"batchItemFailures":[{"itemIdentifier":"item-1"}],"private":"RAW_PAYLOAD_SECRET"}`
	for _, tc := range []struct {
		name     string
		response string
		stale    bool
		state    string
		result   string
		reported bool
	}{
		{name: "partial failure", response: report, state: "partial_failure", result: "Partial batch failure", reported: true},
		{name: "failed acknowledgment", response: report, stale: true, state: "ack_failed", result: "Source acknowledgment failed", reported: true},
		{name: "invalid response", response: `{"batchItemFailures":[{"itemIdentifier":"RAW_PAYLOAD_SECRET"}]}`, state: "failed", result: "Invalid batch response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			batch := leasedBatch("item", 3, 1)
			q := evidenceQueue{fakeQueue: newFakeQueue(),
				ack: func(_ context.Context, receipt string) (ReceiptOutcome, error) {
					if receipt == batch[1].ReceiptHandle || !tc.reported {
						t.Error("attempted to acknowledge a receipt that should be retried")
					}
					if tc.stale && receipt == batch[0].ReceiptHandle {
						return ReceiptStaleOrExpired, nil
					}
					return ReceiptMappingSettled, nil
				},
				inspect: func(context.Context, string) (ReceiptOutcome, error) {
					return ReceiptUnacknowledged, nil
				},
			}
			q.batches <- batch
			metadata := InvocationMetadata{RequestID: "joined-invocation", FunctionARN: targetARN}
			invoker := observedInvoker{observe: func(_ context.Context, admit func(InvocationMetadata) error) (InvocationOutcome, error) {
				if err := admit(metadata); err != nil {
					return InvocationOutcome{Metadata: metadata, State: InvocationNotStarted}, err
				}
				return InvocationOutcome{Metadata: metadata, State: InvocationSucceeded, Payload: []byte(tc.response)}, nil
			}}
			var capture bytes.Buffer
			s := newDeliveryService(t, q, invoker, &DevDeliveryCaptureConfig{LogWriter: &capture})
			input := batchInput(3, 0)
			input.FunctionResponseTypes = []string{"ReportBatchItemFailures"}
			mapping, err := s.Create(t.Context(), input)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				got, err := s.Get(mapping.UUID)
				return err == nil && got.LastProcessingResult == tc.result
			}, time.Second, time.Millisecond)
			require.NoError(t, s.Close(t.Context()))
			rows := deliveryRows(t, capture.Bytes())
			require.Len(t, rows, 2)
			terminal := rows[1]
			require.Equal(t, tc.state, terminal.State)
			require.Equal(t, metadata.RequestID, terminal.RequestID)
			require.Equal(t, InvocationSucceeded, terminal.InvocationState)
			require.True(t, terminal.Joined)
			require.Len(t, terminal.Messages, len(batch))
			for index, message := range terminal.Messages {
				require.Equal(t, batch[index].MessageID, message.MessageID)
				retryReported := tc.reported && index == 1
				acknowledged := tc.reported && !retryReported
				require.Equal(t, retryReported, message.RetryReported)
				require.Equal(t, acknowledged, message.AcknowledgeAttempted)
				settlement := ReceiptUnacknowledged
				if acknowledged {
					settlement = ReceiptMappingSettled
					if tc.stale && index == 0 {
						settlement = ReceiptStaleOrExpired
					}
				}
				require.Equal(t, settlement, message.Settlement)
			}
			require.NotContains(t, capture.String(), "RAW_PAYLOAD_SECRET")
		})
	}
}
