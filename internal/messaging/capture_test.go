package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSNSCaptureOwnsSchemaAndTimestamp(t *testing.T) {
	var output bytes.Buffer
	capture := NewSNSCapture(&output)
	before := time.Now().UTC()
	require.NoError(t, capture.Append(SNSCaptureRecord{
		SchemaVersion: "caller-provided", Operation: "Publish", PhoneNumber: "+12025550123", Message: "local request",
		MessageAttributes: map[string]MessageAttribute{"binary": {DataType: "Binary", BinaryValue: []byte{0, 1, 127}}},
	}))
	explicitTime := time.Date(2020, time.January, 2, 3, 4, 5, 0, time.UTC)
	require.NoError(t, capture.Append(SNSCaptureRecord{
		Operation: "CreateSMSSandboxPhoneNumber", CapturedAt: explicitTime, Details: map[string]any{"otp": "123456"},
	}))
	require.NoError(t, capture.Close())
	records := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	require.Len(t, records, 2)
	var first, second SNSCaptureRecord
	require.NoError(t, json.Unmarshal(records[0], &first))
	require.Equal(t, "eventbus.sns.capture.v1", first.SchemaVersion)
	require.Equal(t, "Publish", first.Operation)
	require.Equal(t, "+12025550123", first.PhoneNumber)
	require.Equal(t, "local request", first.Message)
	require.False(t, first.CapturedAt.Before(before))
	require.False(t, first.CapturedAt.After(time.Now().UTC()))
	require.Equal(t, time.UTC, first.CapturedAt.Location())
	require.Equal(t, []byte{0, 1, 127}, first.MessageAttributes["binary"].BinaryValue)
	require.NoError(t, json.Unmarshal(records[1], &second))
	require.Equal(t, "eventbus.sns.capture.v1", second.SchemaVersion)
	require.Equal(t, explicitTime, second.CapturedAt)
	require.Equal(t, "123456", second.Details["otp"])
}

type snsFailingWriter struct {
	calls   int
	failure error
}

func (writer *snsFailingWriter) Write(data []byte) (int, error) {
	writer.calls++
	return 1, writer.failure
}

func TestBrokerUsesConfiguredSNSCapture(t *testing.T) {
	failure := errors.New("disk full")
	writer := &snsFailingWriter{failure: failure}
	broker := newTestBroker()
	capture := NewSNSCapture(writer)
	broker.SetSNSCapture(capture)
	err := broker.CaptureSNS(SNSCaptureRecord{Operation: "Publish"})
	require.ErrorIs(t, err, failure)
	require.EqualError(t, err, "append SNS capture: disk full")
	require.ErrorIs(t, capture.Close(), failure)
}

func TestSNSCaptureMissingAndClosedWriterErrors(t *testing.T) {
	var missing *SNSCapture
	require.EqualError(t, missing.Append(SNSCaptureRecord{}), "SNS capture is not configured")
	require.NoError(t, missing.Close())
	require.EqualError(t, (&SNSCapture{}).Append(SNSCaptureRecord{}), "SNS capture writer is not configured")
	capture := NewSNSCapture(nil)
	require.EqualError(t, capture.Append(SNSCaptureRecord{}), "SNS capture writer is not configured")
	require.NoError(t, capture.Close())
	require.EqualError(t, capture.Append(SNSCaptureRecord{}), "SNS capture is closed")
}
