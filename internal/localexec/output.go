package localexec

import "bytes"

// BoundedOutput drains a process stream while retaining at most the selected
// limit. Callers must reject overflow before trusting a complete result. Each
// stream has one writer; read it after the process and its copy goroutine join.
type BoundedOutput struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func NewBoundedOutput(limit int) *BoundedOutput {
	return &BoundedOutput{limit: max(0, limit)}
}

func (output *BoundedOutput) Write(data []byte) (int, error) {
	size := len(data)
	remaining := output.limit - output.buffer.Len()
	if size > remaining {
		output.overflow = true
		data = data[:remaining]
	}
	_, _ = output.buffer.Write(data)
	// Draining continues after overflow so the child cannot block on a full
	// output pipe. Overflowed distinguishes a retained prefix from a result.
	return size, nil
}

func (output *BoundedOutput) Overflowed() bool { return output.overflow }
func (output *BoundedOutput) Len() int         { return output.buffer.Len() }
func (output *BoundedOutput) String() string   { return output.buffer.String() }

// Bytes returns an owned copy rather than exposing the retained buffer.
func (output *BoundedOutput) Bytes() []byte {
	return append([]byte(nil), output.buffer.Bytes()...)
}
