//go:build performance

package lambda

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
)

// Measure the real terminal transition, including capture, shared state and
// wake publication. Compare the same fixture against the prior source to
// attribute history retention; this is not a handler or durable-file benchmark.
func BenchmarkPerformanceAsyncHistoryCompletion(b *testing.B) {
	for _, limit := range []int{256, 10000} {
		b.Run(fmt.Sprintf("history_%d", limit), func(b *testing.B) {
			sink := devcapture.NewWriter(io.Discard, "history")
			defer sink.Close()
			service := &Service{asyncHistoryLimit: limit, asyncTasks: make(map[string]*asyncTask), asyncWake: make(chan struct{}), asyncCapture: sink}
			base := time.Unix(1700000000, 0).UTC()
			for index := 0; index < limit; index++ {
				service.asyncHistory = append(service.asyncHistory, AsyncRecord{RequestID: fmt.Sprint(index), QueuedAt: base.Add(time.Duration(index) * time.Second)})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				task := &asyncTask{record: AsyncRecord{RequestID: "completion", State: "running", QueuedAt: base}}
				service.asyncTransitionMu.Lock()
				service.mu.Lock()
				service.asyncTasks[task.record.RequestID] = task
				service.asyncOutstanding++
				service.mu.Unlock()
				service.finishAsync(task, "succeeded", "")
				service.asyncTransitionMu.Unlock()
			}
		})
	}
}

var performanceAsyncSnapshotSink []AsyncRecord

func BenchmarkPerformanceAsyncHistorySnapshot(b *testing.B) {
	for _, limit := range []int{256, 10000} {
		b.Run(fmt.Sprintf("history_%d", limit), func(b *testing.B) {
			service := &Service{asyncHistoryLimit: limit, asyncTasks: make(map[string]*asyncTask)}
			base := time.Unix(1700000000, 0).UTC()
			for index := 0; index < limit; index++ {
				service.asyncHistory = append(service.asyncHistory, AsyncRecord{RequestID: fmt.Sprint(index), QueuedAt: base.Add(time.Duration(limit-index) * time.Second)})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				performanceAsyncSnapshotSink = service.AsyncSnapshot()
			}
		})
	}
}
