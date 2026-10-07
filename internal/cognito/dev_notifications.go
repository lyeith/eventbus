package cognito

import (
	"context"
	"github.com/lyeith/eventbus/internal/devcapture"
)

// NotificationCapture adapts native Cognito messages to append-only harness
// evidence. Codes are available to local agents; no messages leave the harness.
type NotificationCapture struct{ sink *devcapture.Sink }

func OpenNotificationCapture(path string) (*NotificationCapture, error) {
	sink, err := devcapture.Open(path, "Cognito notifications")
	if err != nil {
		return nil, err
	}
	return &NotificationCapture{sink: sink}, nil
}
func (capture *NotificationCapture) Deliver(ctx context.Context, notification Notification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var sink *devcapture.Sink
	if capture != nil {
		sink = capture.sink
	}
	return sink.Append(struct {
		SchemaVersion string `json:"schema_version"`
		Notification
	}{"eventbus.cognito.notification.v1", notification})
}
func (capture *NotificationCapture) Close() error {
	if capture == nil {
		return nil
	}
	return capture.sink.Close()
}

// Err checks capture evidence without closing it. A failed append/sync remains
// sticky, and an in-progress append must finish before evidence can be healthy.
func (capture *NotificationCapture) Err() error {
	var sink *devcapture.Sink
	if capture != nil {
		sink = capture.sink
	}
	return sink.Err()
}
