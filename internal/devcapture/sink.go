// Package devcapture owns durable JSON Lines output for the development harness.
// Services own record schemas and decide which captures precede API acceptance.
package devcapture

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Sink serializes complete append/sync operations. A failed write or sync is
// terminal, so a later append cannot obscure an incomplete or undurable record.
type Sink struct {
	mu        sync.Mutex
	name      string
	writer    io.Writer
	syncFile  func() error
	closeFile func() error
	failure   error
	closeErr  error
	closed    bool
}

// Open owns an append-only regular file, or borrows stdout when path is "-".
// Name identifies the service in existing human-readable capture errors.
func Open(path, name string) (*Sink, error) {
	if path == "-" {
		return NewWriter(os.Stdout, name), nil
	}
	if path == "" {
		return nil, fmt.Errorf("%s log path must not be empty", name)
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
		err = fmt.Errorf("%s log must be a regular file; use '-' for stdout", name)
	}
	if err == nil && info.Size() > 0 {
		last := make([]byte, 1)
		_, err = file.ReadAt(last, info.Size()-1)
		if err == nil && last[0] != '\n' {
			err = fmt.Errorf("%s log ends with an incomplete record; repair it or select a new log", name)
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Sink{name: name, writer: file, syncFile: file.Sync, closeFile: file.Close}, nil
}

// NewWriter borrows writer. Closing the sink never closes the supplied writer;
// only Open takes ownership of a file and synchronizes it after each append.
func NewWriter(writer io.Writer, name string) *Sink {
	return &Sink{name: name, writer: writer}
}

func (sink *Sink) Append(record any) error {
	if sink == nil {
		return errors.New("capture is not configured")
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.closed {
		return fmt.Errorf("%s capture is closed", sink.name)
	}
	if sink.failure != nil {
		return sink.failure
	}
	if sink.writer == nil {
		return fmt.Errorf("%s capture writer is not configured", sink.name)
	}
	written, err := sink.writer.Write(encoded)
	if err == nil && written != len(encoded) {
		err = io.ErrShortWrite
	}
	if err == nil && sink.syncFile != nil {
		err = sink.syncFile()
	}
	if err != nil {
		sink.failure = fmt.Errorf("append %s capture: %w", sink.name, err)
	}
	return sink.failure
}

// Close waits for an admitted append, closes an owned file once and retains both
// delivery and close errors. Borrowed output, including stdout, remains open.
func (sink *Sink) Close() error {
	if sink == nil {
		return nil
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.closed {
		return sink.closeErr
	}
	sink.closed = true
	var err error
	if sink.closeFile != nil {
		err = sink.closeFile()
	}
	sink.closeErr = errors.Join(sink.failure, err)
	return sink.closeErr
}
