//go:build performance && (linux || darwin)

package devcapture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// These opt-in measurements retain the durable file contract. The discard
// writer is explicitly a non-durable comparison, never a replacement policy.
func TestPerformanceReviewCaptureAppend(t *testing.T) {
	for _, size := range []int{1024, 64 << 10} {
		for _, mode := range []string{"discard", "private_file"} {
			t.Run(fmt.Sprintf("%s_%d", mode, size), func(t *testing.T) {
				const count = 20
				path := filepath.Join(t.TempDir(), "private", "performance.jsonl")
				var sink *Sink
				var err error
				if mode == "private_file" {
					sink, err = OpenPrivate(path, "performance")
				} else {
					sink = NewWriter(io.Discard, "performance")
				}
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := sink.Close(); err != nil {
						t.Error(err)
					}
				})
				text := strings.Repeat("x", size)
				var samples []float64
				for index := range count {
					record := struct {
						Index int    `json:"index"`
						Data  string `json:"data"`
					}{Index: index, Data: text}
					started := time.Now()
					err := sink.Append(record)
					elapsedMS := float64(time.Since(started)) / float64(time.Millisecond)
					if err != nil {
						t.Fatalf("sample %d append failed: %v", index, err)
					}
					samples = append(samples, elapsedMS)
					t.Logf("PERFORMANCE_CAPTURE_SAMPLE mode=%s payload_bytes=%d index=%d append_ms=%.6f", mode, size, index, elapsedMS)
				}
				if err := sink.Err(); err != nil {
					t.Fatal(err)
				}
				if err := sink.Close(); err != nil {
					t.Fatal(err)
				}
				if mode == "private_file" {
					info, err := os.Stat(path)
					if err != nil || info.Mode().Perm() != 0600 {
						t.Fatalf("private owned measurement file: %v %v", info, err)
					}
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					decoder := json.NewDecoder(bytes.NewReader(data))
					for index := range count {
						var record struct {
							Index int    `json:"index"`
							Data  string `json:"data"`
						}
						if err := decoder.Decode(&record); err != nil || record.Index != index || record.Data != text {
							t.Fatalf("durable record %d changed: %#v %v", index, record, err)
						}
					}
					var extra any
					if err := decoder.Decode(&extra); err != io.EOF {
						t.Fatalf("unexpected retained record after samples: %v", err)
					}
				}
				sort.Float64s(samples)
				median := (samples[count/2-1] + samples[count/2]) / 2
				t.Logf("PERFORMANCE_CAPTURE_SUMMARY mode=%s payload_bytes=%d count=%d min=%.6f median=%.6f max=%.6f", mode, size, count, samples[0], median, samples[count-1])
			})
		}
	}
}
