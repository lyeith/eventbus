//go:build performance && (linux || darwin)

package devcapture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
)

// Fixed total work distinguishes durable throughput from producer lock waiting.
// Every acknowledged append is covered by an actual owned-file Sync.
// Instrumentation counts and times the opportunistic durable groups.
func TestPerformanceRound2CaptureConcurrency(t *testing.T) {
	const count = 64
	type record struct {
		Producer int    `json:"producer"`
		Index    int    `json:"index"`
		Data     string `json:"data"`
	}
	for _, size := range []int{1024, 64 << 10} {
		for _, mode := range []string{"discard", "private_file"} {
			for _, producers := range []int{1, 4, 16} {
				t.Run(fmt.Sprintf("%s/bytes_%d/producers_%d", mode, size, producers), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "capture.jsonl")
					var sink *Sink
					var err error
					var syncWall []float64
					if mode == "private_file" {
						sink, err = OpenPrivate(path, "round2")
						if err == nil {
							actualSync := sink.syncFile
							sink.syncFile = func() error {
								started := time.Now()
								err := actualSync()
								syncWall = append(syncWall, float64(time.Since(started))/float64(time.Millisecond))
								return err
							}
						}
					} else {
						sink = NewWriter(io.Discard, "round2")
					}
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := sink.Close(); err != nil {
							t.Error(err)
						}
					})
					payload := strings.Repeat("x", size)
					start := make(chan struct{})
					failures := make(chan error, producers)
					samples := make([]float64, count)
					var joined sync.WaitGroup
					for producer := range producers {
						joined.Add(1)
						go func(producer int) {
							defer joined.Done()
							<-start
							for index := range count / producers {
								started := time.Now()
								err := sink.Append(record{producer, index, payload})
								samples[producer*(count/producers)+index] = float64(time.Since(started)) / float64(time.Millisecond)
								if err != nil {
									failures <- err
									return
								}
							}
						}(producer)
					}
					started := time.Now()
					close(start)
					joined.Wait()
					elapsed := time.Since(started)
					close(failures)
					for err := range failures {
						t.Fatal(err)
					}
					if err := sink.Close(); err != nil {
						t.Fatal(err)
					}
					testperf.Report(t, t.Name(), "append_ms", samples)
					testperf.Report(t, t.Name(), "fixed_64_records_wall_ms", []float64{float64(elapsed) / float64(time.Millisecond)})
					t.Logf("PERFORMANCE_ROUND2_CAPTURE mode=%s payload_bytes=%d producers=%d records=%d throughput_records_per_second=%.3f joined=true append_samples=producer_index_order", mode, size, producers, count, float64(count)/elapsed.Seconds())
					if mode == "private_file" {
						if len(syncWall) < 1 || len(syncWall) > count {
							t.Fatalf("real sync calls=%d records=%d", len(syncWall), count)
						}
						t.Logf("PERFORMANCE_ROUND2_CAPTURE_DURABILITY records=%d real_sync_calls=%d records_per_sync=%.3f", count, len(syncWall), float64(count)/float64(len(syncWall)))
						testperf.Report(t, t.Name(), "actual_sync_ms", syncWall)
						info, err := os.Stat(path)
						if err != nil || info.Mode().Perm() != 0600 {
							t.Fatalf("private capture: %v %v", info, err)
						}
						data, err := os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						decoder := json.NewDecoder(bytes.NewReader(data))
						next := make([]int, producers)
						for range count {
							var got record
							if err := decoder.Decode(&got); err != nil {
								t.Fatal(err)
							}
							if got.Producer < 0 || got.Producer >= producers || got.Index != next[got.Producer] || got.Data != payload {
								t.Fatalf("durable record changed: producer=%d index=%d", got.Producer, got.Index)
							}
							next[got.Producer]++
						}
						for producer, actual := range next {
							if actual != count/producers {
								t.Fatalf("producer %d records=%d", producer, actual)
							}
						}
						var extra any
						if err := decoder.Decode(&extra); err != io.EOF {
							t.Fatalf("unexpected extra capture: %v", err)
						}
					}
				})
			}
		}
	}
}
