package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSNSCaptureErrRequiresAvailableEvidenceAndLeavesCaptureOpen(t *testing.T) {
	var missing *SNSCapture
	require.EqualError(t, missing.Err(), "SNS capture is not configured")
	require.EqualError(t, (&SNSCapture{}).Err(), "SNS capture writer is not configured")
	require.EqualError(t, NewSNSCapture(nil).Err(), "SNS capture writer is not configured")
	var output bytes.Buffer
	capture := NewSNSCapture(&output)
	require.NoError(t, capture.Err())
	require.NoError(t, capture.Append(SNSCaptureRecord{Operation: "Publish"}))
	require.NoError(t, capture.Err())
	require.NoError(t, capture.Append(SNSCaptureRecord{Operation: "Publish"}))
	require.Equal(t, 2, bytes.Count(output.Bytes(), []byte{'\n'}))
	require.NoError(t, capture.Close())
	require.EqualError(t, capture.Err(), "SNS capture is closed")
}

type snsAdmissionEvidenceWriter struct {
	bytes.Buffer
	failure error
	calls   int
}

func (writer *snsAdmissionEvidenceWriter) Write(data []byte) (int, error) {
	writer.calls++
	var record SNSCaptureRecord
	if err := json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		return 0, err
	}
	if record.Operation == "DeliveryAdmission" {
		return 0, writer.failure
	}
	return writer.Buffer.Write(data)
}

func TestSNSCaptureErrReportsPostAcceptanceAdmissionEvidenceFailure(t *testing.T) {
	broker := newTestBroker()
	delivery := &snsTestLambdaDelivery{}
	broker.SetLambdaDelivery(delivery)
	topic := broker.CreateTopic("retained-evidence")
	_, err := broker.Subscribe(topic.ARN, "lambda", snsTestLambdaARN, nil)
	require.NoError(t, err)
	failure := errors.New("admission capture unavailable")
	writer := &snsAdmissionEvidenceWriter{failure: failure}
	capture := NewSNSCapture(writer)
	broker.SetSNSCapture(capture)
	require.NoError(t, capture.Err())

	result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "accepted before outcome capture failed"})
	require.NoError(t, err, "native Publish acceptance is retained after outcome capture failure")
	require.NotEmpty(t, result.MessageID)
	require.Len(t, delivery.admitted, 1)
	require.Equal(t, 2, writer.calls, "publication capture succeeds, admission outcome capture fails")
	require.ErrorIs(t, capture.Err(), failure, "successful Publish cannot certify complete evidence")
	records := snsLambdaCaptureRecords(t, &writer.Buffer)
	require.Len(t, records, 1)
	require.Equal(t, "Publish", records[0].Operation)
	require.Equal(t, result.MessageID, records[0].MessageID)
	require.Equal(t, "scheduled", records[0].Deliveries[0].Status)

	_, err = broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "later refusal"})
	require.Error(t, err, "terminal sink failure rejects a later publication before admission")
	require.Len(t, delivery.admitted, 1)
	require.Equal(t, 2, writer.calls, "failed sink is not written again")
	require.ErrorIs(t, capture.Err(), failure)
	require.ErrorIs(t, capture.Close(), failure)
	require.ErrorIs(t, capture.Err(), failure)
}
