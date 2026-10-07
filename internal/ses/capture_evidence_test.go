package ses

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type sesEvidenceWriter struct{ err error }

func (writer sesEvidenceWriter) Write([]byte) (int, error) { return 0, writer.err }

func TestSESCaptureErrIsNonClosingAndRetainsDeliveryFailure(t *testing.T) {
	var output bytes.Buffer
	capture := NewSESCapture(&output)
	require.NoError(t, capture.Err())
	require.NoError(t, capture.append(map[string]string{"id": "first"}))
	require.NoError(t, capture.Err())
	require.NoError(t, capture.append(map[string]string{"id": "second"}))
	require.Contains(t, output.String(), "first")
	require.Contains(t, output.String(), "second")
	require.NoError(t, capture.Close())
	require.Error(t, capture.Err())

	failure := errors.New("fixture capture write failure")
	capture = NewSESCapture(sesEvidenceWriter{failure})
	require.ErrorIs(t, capture.append(map[string]string{"id": "failed"}), failure)
	require.ErrorIs(t, capture.Err(), failure)
	require.ErrorIs(t, capture.Close(), failure)
	require.ErrorIs(t, capture.Err(), failure)
	var absent *SESCapture
	require.Error(t, absent.Err())
	require.Error(t, (&SESCapture{}).Err())
}
