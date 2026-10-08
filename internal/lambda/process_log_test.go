package lambda

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// Counts include discarded prefixes; log projection must retain separate tails
// after both pipe-copy writers have joined.
func TestInvocationDiagnosticsPreserveConcurrentTruncatedStreams(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("private_%t", enabled), func(t *testing.T) {
			logs := newInvocationLogs(enabled, false)
			logs.merged.limit = 4096
			stdout := bytes.Repeat([]byte("o"), 128<<10)
			stderr := bytes.Repeat([]byte("e"), 96<<10)
			var writers sync.WaitGroup
			writers.Add(2)
			go func() {
				defer writers.Done()
				_, _ = logs.stdoutWriter().Write(stdout)
			}()
			go func() {
				defer writers.Done()
				_, _ = logs.stderrWriter().Write(stderr)
			}()
			writers.Wait()
			result := logs.diagnostics()
			if result.tailBytes != int64(len(stdout)+len(stderr)) || len(logs.merged.Bytes()) != 4096 {
				t.Fatalf("merged bounded count/tail changed: %#v", result)
			}
			if enabled {
				if result.stdoutBytes != int64(len(stdout)) || result.stderrBytes != int64(len(stderr)) ||
					!bytes.Equal(result.stdout, stdout[len(stdout)-maxLogs:]) ||
					!bytes.Equal(result.stderr, stderr[len(stderr)-maxLogs:]) {
					t.Fatal("private stream count/truncated tail changed")
				}
			} else if result.stdoutBytes != 0 || result.stderrBytes != 0 || len(result.stdout)+len(result.stderr) != 0 {
				t.Fatal("disabled private capture retained separated streams")
			}
		})
	}
}
