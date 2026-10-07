package cognito

import (
	"bytes"
	"errors"
	"testing"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/stretchr/testify/require"
)

type notificationEvidenceWriter struct{ err error }

func (writer notificationEvidenceWriter) Write([]byte) (int, error) { return 0, writer.err }

func TestDevNotificationCaptureErrIsNonClosingAndRetainsDeliveryFailure(t *testing.T) {
	var output bytes.Buffer
	capture := &NotificationCapture{sink: devcapture.NewWriter(&output, "Cognito notifications")}
	require.NoError(t, capture.Err())
	require.NoError(t, capture.Deliver(t.Context(), Notification{Operation: "SignUp", Code: "012345"}))
	require.NoError(t, capture.Err())
	require.NoError(t, capture.Deliver(t.Context(), Notification{Operation: "ForgotPassword", Code: "543210"}))
	require.Contains(t, output.String(), "012345")
	require.Contains(t, output.String(), "543210")
	require.NoError(t, capture.Close())
	require.Error(t, capture.Err())

	failure := errors.New("fixture capture write failure")
	capture = &NotificationCapture{sink: devcapture.NewWriter(notificationEvidenceWriter{failure}, "Cognito notifications")}
	require.ErrorIs(t, capture.Deliver(t.Context(), Notification{}), failure)
	require.ErrorIs(t, capture.Err(), failure)
	require.ErrorIs(t, capture.Close(), failure)
	require.ErrorIs(t, capture.Err(), failure)
	var absent *NotificationCapture
	require.Error(t, absent.Err())
	require.Error(t, (&NotificationCapture{}).Err())
}
