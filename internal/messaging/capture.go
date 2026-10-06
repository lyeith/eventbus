package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SNSCaptureRecord is the agent-readable local intent for a send or sandbox OTP.
// Binary attribute values use JSON's base64 encoding. No external delivery occurs.
type SNSCaptureRecord struct {
	SchemaVersion          string                      `json:"schema_version"`
	CapturedAt             time.Time                   `json:"captured_at"`
	Operation              string                      `json:"operation"`
	RequestID              string                      `json:"request_id,omitempty"`
	MessageID              string                      `json:"message_id,omitempty"`
	TargetARN              string                      `json:"target_arn,omitempty"`
	PhoneNumber            string                      `json:"phone_number,omitempty"`
	Subject                string                      `json:"subject,omitempty"`
	Message                string                      `json:"message,omitempty"`
	MessageStructure       string                      `json:"message_structure,omitempty"`
	MessageAttributes      map[string]MessageAttribute `json:"message_attributes,omitempty"`
	MessageGroupID         string                      `json:"message_group_id,omitempty"`
	MessageDeduplicationID string                      `json:"message_deduplication_id,omitempty"`
	SequenceNumber         string                      `json:"sequence_number,omitempty"`
	Deliveries             []SNSCaptureDelivery        `json:"deliveries,omitempty"`
	Details                map[string]any              `json:"details,omitempty"`
}

type SNSCaptureDelivery struct {
	Protocol  string `json:"protocol"`
	Endpoint  string `json:"endpoint"`
	Status    string `json:"status"`
	MessageID string `json:"message_id,omitempty"`
	Error     string `json:"error,omitempty"`
}

// SNSCapture appends synchronously. A failed write is terminal: another record
// cannot turn a partial write into a syntactically misleading JSON Lines stream.
type SNSCapture struct {
	mu        sync.Mutex
	writer    io.Writer
	syncFile  func() error
	closeFile func() error
	failure   error
	closeErr  error
	closed    bool
}

func OpenSNSCapture(path string) (*SNSCapture, error) {
	if path == "-" {
		return &SNSCapture{writer: os.Stdout}, nil
	}
	if path == "" {
		return nil, errors.New("SNS log path must not be empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errors.New("SNS log must be a regular file; use '-' for stdout")
	}
	if err == nil && info.Size() > 0 {
		last := make([]byte, 1)
		_, err = file.ReadAt(last, info.Size()-1)
		if err == nil && last[0] != '\n' {
			err = errors.New("SNS log ends with an incomplete record; repair it or select a new log")
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &SNSCapture{writer: file, syncFile: file.Sync, closeFile: file.Close}, nil
}

func (capture *SNSCapture) Append(record SNSCaptureRecord) error {
	if capture == nil {
		return errors.New("SNS capture is not configured")
	}
	record.SchemaVersion = "eventbus.sns.capture.v1"
	if record.CapturedAt.IsZero() {
		record.CapturedAt = time.Now().UTC()
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.closed {
		return errors.New("SNS capture is closed")
	}
	if capture.failure != nil {
		return capture.failure
	}
	if capture.writer == nil {
		return errors.New("SNS capture writer is not configured")
	}
	written, err := capture.writer.Write(encoded)
	if err == nil && written != len(encoded) {
		err = io.ErrShortWrite
	}
	if err == nil && capture.syncFile != nil {
		err = capture.syncFile()
	}
	if err != nil {
		capture.failure = fmt.Errorf("append SNS capture: %w", err)
	}
	return capture.failure
}

func (capture *SNSCapture) Close() error {
	if capture == nil {
		return nil
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.closed {
		return capture.closeErr
	}
	capture.closed = true
	var err error
	if capture.closeFile != nil {
		err = capture.closeFile()
	}
	capture.closeErr = errors.Join(capture.failure, err)
	return capture.closeErr
}

func (b *Broker) SetSNSCapture(capture *SNSCapture) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.capture = capture
}

func (b *Broker) CaptureSNS(record SNSCaptureRecord) error {
	b.mu.RLock()
	capture := b.capture
	b.mu.RUnlock()
	// Embedded broker hosts may opt out; the EventBus CLI always configures it.
	if capture == nil {
		return nil
	}
	return capture.Append(record)
}
