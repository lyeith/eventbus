//go:build performance

package lambda

import (
	"bytes"
	"fmt"
	"testing"
)

var performanceDiagnosticResult invocationDiagnostics

// Measures the real bounded log projection after writers have joined, without
// child-launch/import/fsync costs. The result escapes as in a native invocation.
func BenchmarkPerformanceLogDiagnostics(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		for _, retained := range []int{4 << 10, 64 << 10} {
			b.Run(fmt.Sprintf("private_%t_retained_%d", enabled, retained), func(b *testing.B) {
				logs := newInvocationLogs(enabled, false)
				logs.merged.limit = retained
				body := bytes.Repeat([]byte("x"), 1<<20)
				if count, err := logs.stdoutWriter().Write(body); err != nil || count != len(body) {
					b.Fatalf("log setup: %d %v", count, err)
				}
				result := logs.diagnostics()
				if result.tailBytes != int64(len(body)) || len(logs.merged.Bytes()) != retained {
					b.Fatal("bounded tail/count setup changed")
				}
				if enabled && (result.stdoutBytes != int64(len(body)) || len(result.stdout) != maxLogs) {
					b.Fatal("private stream/count setup changed")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					performanceDiagnosticResult = logs.diagnostics()
				}
			})
		}
	}
}
