package cognito

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDevNotificationCapturePersistsAgentReadableJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifications.jsonl")
	capture, err := OpenNotificationCapture(path)
	require.NoError(t, err)
	notification := Notification{Operation: "SignUp", PoolID: "us-east-1_pool", ClientID: "client", Username: "stable-user", UserSub: "durable-sub", Destination: "recipient@example.test", DeliveryMedium: "EMAIL", AttributeName: "email", Purpose: "signup", Code: "012345", Timestamp: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)}
	require.NoError(t, capture.Deliver(t.Context(), notification))
	require.NoError(t, capture.Close())
	capture, err = OpenNotificationCapture(path)
	require.NoError(t, err)
	notification.Operation = "ForgotPassword"
	notification.Purpose = "recovery"
	require.NoError(t, capture.Deliver(t.Context(), notification))
	require.NoError(t, capture.Close())
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSuffix(string(contents), "\n"), "\n")
	require.Len(t, lines, 2)
	for _, line := range lines {
		var record map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		require.Equal(t, "eventbus.cognito.notification.v1", record["schema_version"])
		require.Equal(t, "012345", record["code"])
		require.Equal(t, "recipient@example.test", record["destination"])
		require.Equal(t, "durable-sub", record["user_sub"])
		require.Equal(t, "2026-10-07T00:00:00Z", record["timestamp"])
	}
	require.Error(t, capture.Deliver(t.Context(), notification))
	var absent *NotificationCapture
	require.Error(t, absent.Deliver(context.Background(), notification))
	require.NoError(t, absent.Close())
}

func TestDevNotificationCaptureHonorsCanceledDelivery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifications.jsonl")
	capture, err := OpenNotificationCapture(path)
	require.NoError(t, err)
	defer capture.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, capture.Deliver(ctx, Notification{}), context.Canceled)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Empty(t, body)
}
