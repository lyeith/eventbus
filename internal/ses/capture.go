package ses

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// SESCapture is synchronous: no response succeeds ahead of its complete append.
// A failed append is terminal, so subsequent requests cannot append after a
// partial record and turn the remainder of the stream into invalid JSONL.
type SESCapture struct {
	mu        sync.Mutex
	writer    io.Writer
	syncFile  func() error
	closeFile func() error
	closed    bool
	failure   error
	closeErr  error
}

func OpenSESCapture(path string) (*SESCapture, error) {
	if path == "-" {
		return &SESCapture{writer: os.Stdout}, nil
	}
	if path == "" {
		return nil, errors.New("SES log path must not be empty")
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
		err = errors.New("SES log must be a regular file; use '-' for stdout")
	}
	if err == nil && info.Size() > 0 {
		last := make([]byte, 1)
		_, err = file.ReadAt(last, info.Size()-1)
		if err == nil && last[0] != '\n' {
			err = errors.New("SES log ends with an incomplete record; repair it or select a new log")
		}
	}
	if err != nil {
		file.Close()
		return nil, err
	}
	return &SESCapture{writer: file, syncFile: file.Sync, closeFile: file.Close}, nil
}

func (capture *SESCapture) append(record any) error {
	if capture == nil {
		return errors.New("SES capture is not configured")
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.closed {
		return errors.New("SES capture is closed")
	}
	if capture.failure != nil {
		return capture.failure
	}
	if capture.writer == nil {
		return errors.New("SES capture writer is not configured")
	}
	written, err := capture.writer.Write(encoded)
	if err == nil && written != len(encoded) {
		err = io.ErrShortWrite
	}
	if err == nil && capture.syncFile != nil {
		err = capture.syncFile()
	}
	if err != nil {
		capture.failure = fmt.Errorf("append SES capture: %w", err)
	}
	return capture.failure
}

func (capture *SESCapture) Close() error {
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
