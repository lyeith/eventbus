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

// Sink serializes JSONL writes and acknowledges owned-file appends only after
// a Sync covering the whole record. Records already waiting can share a Sync;
// a failed write or Sync is terminal for the entire affected group.
type Sink struct {
	mu        sync.Mutex
	changed   *sync.Cond
	name      string
	writer    io.Writer
	syncFile  func() error
	closeFile func() error
	failure   error
	closeErr  error
	closing   bool
	closed    bool

	pending      []*appendRecord
	pendingBytes int
	writing      bool
	admitted     uint64
	completed    uint64
}

type appendRecord struct {
	encoded []byte
	done    bool
	err     error
}

// A pending group is bounded independently of the active group. A record above
// the byte limit occupies a group by itself; service admission owns record size.
const (
	maxPendingRecords = 64
	maxPendingBytes   = 1 << 20
)

var (
	errInterruptedWrite = errors.New("write or sync interrupted")
	errInterruptedClose = errors.New("close interrupted")
)

func (sink *Sink) conditionLocked() *sync.Cond {
	if sink.changed == nil {
		sink.changed = sync.NewCond(&sink.mu)
	}
	return sink.changed
}

func (sink *Sink) canQueueLocked(size int) bool {
	return len(sink.pending) == 0 ||
		(len(sink.pending) < maxPendingRecords && sink.pendingBytes <= maxPendingBytes &&
			size <= maxPendingBytes-sink.pendingBytes)
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
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return newFileSink(file, info, name)
}

// newFileSink shares durable record-boundary validation and file ownership
// between ordinary and private capture opening.
func newFileSink(file *os.File, info os.FileInfo, name string) (*Sink, error) {
	var err error
	if info.Size() > 0 {
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
// Open and OpenPrivate own files and synchronize every acknowledged record.
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
	changed := sink.conditionLocked()
	for {
		if sink.closing {
			return fmt.Errorf("%s capture is closed", sink.name)
		}
		if sink.failure != nil {
			return sink.failure
		}
		if sink.writer == nil {
			return fmt.Errorf("%s capture writer is not configured", sink.name)
		}
		if sink.canQueueLocked(len(encoded)) {
			break
		}
		changed.Wait()
	}
	request := &appendRecord{encoded: encoded}
	sink.pending = append(sink.pending, request)
	sink.pendingBytes += len(encoded)
	sink.admitted++
	changed.Broadcast()
	for !request.done {
		if sink.writing {
			changed.Wait()
			continue
		}
		// Any awaiting caller can lead one already-admitted group. Releasing
		// leadership after that group lets completed callers return without
		// waiting for later arrivals, and needs no background worker or sleep.
		batch := sink.pending
		sink.pending = nil
		sink.pendingBytes = 0
		sink.writing = true
		changed.Broadcast()
		sink.writeGroupLocked(batch)
	}
	return request.err
}

// writeGroupLocked returns with the state lock held, including during panic or
// Goexit unwinding from a borrowed writer. Cleanup must fail every affected
// admission before preserving the caller's original interruption.
func (sink *Sink) writeGroupLocked(batch []*appendRecord) {
	err := errInterruptedWrite
	defer func() {
		sink.mu.Lock()
		if err != nil {
			sink.failure = fmt.Errorf("append %s capture: %w", sink.name, err)
		}
		sink.completeLocked(batch)
		if sink.failure != nil {
			sink.completeLocked(sink.pending)
			sink.pending = nil
			sink.pendingBytes = 0
		}
		sink.writing = false
		sink.conditionLocked().Broadcast()
	}()
	sink.mu.Unlock()
	err = sink.writeBatch(batch)
}

func (sink *Sink) writeBatch(batch []*appendRecord) error {
	for _, request := range batch {
		written, err := sink.writer.Write(request.encoded)
		if err == nil && written != len(request.encoded) {
			err = io.ErrShortWrite
		}
		if err != nil {
			return err
		}
	}
	if sink.syncFile != nil {
		return sink.syncFile()
	}
	return nil
}

func (sink *Sink) completeLocked(batch []*appendRecord) {
	for _, request := range batch {
		request.err = sink.failure
		request.encoded = nil
		request.done = true
	}
	sink.completed += uint64(len(batch))
}

// Err reports capture availability and retained delivery/close failures without
// closing the sink. It waits for all records admitted before the check, so a
// pending or undurable record cannot be observed as healthy evidence.
func (sink *Sink) Err() error {
	if sink == nil {
		return errors.New("capture is not configured")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	changed := sink.conditionLocked()
	target := sink.admitted
	for sink.completed < target || (sink.closing && !sink.closed) {
		changed.Wait()
	}
	if sink.failure != nil {
		return sink.failure
	}
	if sink.closed {
		if sink.closeErr != nil {
			return sink.closeErr
		}
		return fmt.Errorf("%s capture is closed", sink.name)
	}
	if sink.writer == nil {
		return fmt.Errorf("%s capture writer is not configured", sink.name)
	}
	return nil
}

// Close stops admission, joins all admitted append groups, closes an owned file
// once and retains delivery/close errors. Borrowed output remains open.
func (sink *Sink) Close() error {
	if sink == nil {
		return nil
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	changed := sink.conditionLocked()
	if sink.closing {
		for !sink.closed {
			changed.Wait()
		}
		return sink.closeErr
	}
	sink.closing = true
	changed.Broadcast()
	for sink.writing || len(sink.pending) != 0 {
		changed.Wait()
	}
	// Keep close itself outside the state lock. Concurrent Close/Err callers
	// still join its completion, and late appends can reject immediately.
	sink.closeOwnedFileLocked()
	return sink.closeErr
}

func (sink *Sink) closeOwnedFileLocked() {
	err := errInterruptedClose
	defer func() {
		sink.mu.Lock()
		sink.closeErr = errors.Join(sink.failure, err)
		sink.closed = true
		sink.conditionLocked().Broadcast()
	}()
	sink.mu.Unlock()
	if sink.closeFile == nil {
		err = nil
	} else {
		err = sink.closeFile()
	}
}
