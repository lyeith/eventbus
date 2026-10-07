package ses

import (
	"errors"
	"io"

	"github.com/lyeith/eventbus/internal/devcapture"
)

// SESCapture preserves the SES capture contract through the shared harness sink.
// The SES adapter owns its record schema and capture-before-response decision.
type SESCapture struct {
	sink *devcapture.Sink
}

func OpenSESCapture(path string) (*SESCapture, error) {
	sink, err := devcapture.Open(path, "SES")
	if err != nil {
		return nil, err
	}
	return &SESCapture{sink: sink}, nil
}

// NewSESCapture borrows writer for embedded hosts; Close leaves writer open.
func NewSESCapture(writer io.Writer) *SESCapture {
	return &SESCapture{sink: devcapture.NewWriter(writer, "SES")}
}

func (capture *SESCapture) append(record any) error {
	if capture == nil {
		return errors.New("SES capture is not configured")
	}
	if capture.sink == nil {
		return errors.New("SES capture writer is not configured")
	}
	return capture.sink.Append(record)
}

func (capture *SESCapture) Close() error {
	if capture == nil {
		return nil
	}
	return capture.sink.Close()
}

// Err checks capture evidence without closing it. A failed append/sync remains
// sticky, and an in-progress append must finish before evidence can be healthy.
func (capture *SESCapture) Err() error {
	var sink *devcapture.Sink
	if capture != nil {
		sink = capture.sink
	}
	return sink.Err()
}
