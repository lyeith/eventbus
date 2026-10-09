//go:build performance && linux

package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
)

// Successful values traverse the unchanged wrapper, native Runtime API, output
// boundary, private optional capture and ExecuteObserved owner. Application data
// preparation is outside timing after one untimed warmup for each payload size.
const performanceRound2Python = `import os
cache = {}
count = 0
def handler(event, context):
    global count
    count += 1
    size = event['bytes']
    if size not in cache:
        cache[size] = 'x' * size
    return {'id':context.aws_request_id, 'pid':os.getpid(), 'count':count, 'data':cache[size]}
`

const performanceRound2Node = `const cache = new Map(); let count = 0;
export function handler(event,context) {
 count++;
 if (!cache.has(event.bytes)) cache.set(event.bytes,'x'.repeat(event.bytes));
 return {id:context.awsRequestId,pid:process.pid,count,data:cache.get(event.bytes)};
}
`

func performanceRound2RSS(t *testing.T, pid int) float64 {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			kib, err := strconv.ParseInt(strings.Fields(line)[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return float64(kib * 1024)
		}
	}
	t.Fatal("worker RSS unavailable")
	return 0
}

// /proc ticks attribute CPU to the actual native child across the response
// boundary. Resolution is deliberately reported as ticks, without assuming the
// host's clock frequency or treating this whole boundary as serialization only.
func performanceRound2ChildCPUTicks(t *testing.T, pid int) uint64 {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	endName := strings.LastIndexByte(string(data), ')')
	if endName < 0 {
		t.Fatal("worker process stat name unavailable")
	}
	fields := strings.Fields(string(data[endName+1:]))
	if len(fields) < 13 {
		t.Fatal("worker process stat CPU unavailable")
	}
	user, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	system, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return user + system
}

func TestPerformanceRound2WarmResponseSizes(t *testing.T) {
	const samples = 20
	for _, language := range []string{"python", "node"} {
		t.Run(language, func(t *testing.T) {
			command, filename, source := []string{"node"}, "reply.mjs", performanceRound2Node
			if language == "python" {
				command, filename, source = []string{performanceReviewInterpreter(t)}, "reply.py", performanceRound2Python
			} else if _, err := exec.LookPath(command[0]); err != nil {
				t.Skip(err)
			}
			for _, diagnostics := range []bool{false, true} {
				t.Run(fmt.Sprintf("diagnostics_%t", diagnostics), func(t *testing.T) {
					dir := t.TempDir()
					if err := os.WriteFile(filepath.Join(dir, filename), []byte(source), 0600); err != nil {
						t.Fatal(err)
					}
					config := &Config{Functions: map[string]Function{"reply": {Runtime: language, Command: command, Handler: filename + "#handler", Timeout: 10 * time.Second}}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{Workers: 1, LogWriter: io.Discard}}
					if diagnostics {
						config.DevDiagnostics = &DevDiagnosticsConfig{LogPath: filepath.Join(dir, "private.jsonl")}
					}
					service, err := NewService(config, dir)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := service.Close(context.Background()); err != nil {
							t.Errorf("actual worker closure: %v", err)
						}
					})
					seen := map[string]bool{}
					pid, wantCount := 0, 0
					for _, size := range []int{1 << 10, 256 << 10, 4 << 20} {
						var wall, allocated, allocs, rss, cpuTicks []float64
						input := InvokeInput{FunctionName: "reply", Payload: []byte(fmt.Sprintf("{\"bytes\":%d}", size))}
						for index := -1; index < samples; index++ {
							var before, after runtime.MemStats
							var childBefore uint64
							if pid != 0 {
								childBefore = performanceRound2ChildCPUTicks(t, pid)
							}
							runtime.ReadMemStats(&before)
							started := time.Now()
							outcome, err := service.ExecuteObserved(context.Background(), input, nil)
							elapsed := time.Since(started)
							runtime.ReadMemStats(&after)
							var childAfter uint64
							if pid != 0 {
								childAfter = performanceRound2ChildCPUTicks(t, pid)
							}
							if err != nil || outcome.State != InvocationSucceeded || outcome.Output.FunctionError || outcome.OwnershipErr != nil || outcome.CompletionScope != CompletionInvocation {
								t.Fatalf("native completion %+v err%v", outcome, err)
							}
							var reply struct {
								ID    string `json:"id"`
								PID   int    `json:"pid"`
								Count int    `json:"count"`
								Data  string `json:"data"`
							}
							if err := json.Unmarshal(outcome.Output.Payload, &reply); err != nil {
								t.Fatal(err)
							}
							wantCount++
							if reply.ID != outcome.Metadata.RequestID || reply.ID != outcome.Output.RequestID || seen[reply.ID] || reply.Count != wantCount || len(reply.Data) != size || strings.Trim(reply.Data, "x") != "" {
								t.Fatalf("native identity/data changed size%d reply count%d", size, reply.Count)
							}
							seen[reply.ID] = true
							if pid == 0 {
								pid = reply.PID
							}
							if reply.PID != pid {
								t.Fatalf("warm PID changed %d -> %d", pid, reply.PID)
							}
							if index < 0 {
								continue
							}
							wall = append(wall, float64(elapsed)/float64(time.Millisecond))
							allocated = append(allocated, float64(after.TotalAlloc-before.TotalAlloc))
							allocs = append(allocs, float64(after.Mallocs-before.Mallocs))
							rss = append(rss, performanceRound2RSS(t, pid))
							if childAfter < childBefore {
								t.Fatal("worker CPU counter moved backwards")
							}
							cpuTicks = append(cpuTicks, float64(childAfter-childBefore))
						}
						name := fmt.Sprintf("warm_%s_response_%d_diagnostics_%t", language, size, diagnostics)
						for _, metric := range []struct {
							name   string
							values []float64
						}{{"joined_wall_ms", wall}, {"go_allocated_bytes", allocated}, {"go_allocations", allocs}, {"retained_worker_rss_bytes", rss}, {"worker_cpu_ticks", cpuTicks}} {
							testperf.Report(t, name, metric.name, metric.values)
						}
					}
					if err := service.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					if processAlive(pid) {
						t.Fatal("actual warm worker remains after owner closure")
					}
					if diagnostics {
						file, err := os.Open(filepath.Join(dir, "private.jsonl"))
						if err != nil {
							t.Fatal(err)
						}
						defer file.Close()
						decoder := json.NewDecoder(file)
						records := 0
						for {
							var record invocationDiagnosticRecord
							if err := decoder.Decode(&record); err == io.EOF {
								break
							} else if err != nil {
								t.Fatal(err)
							}
							if !seen[record.RequestID] || !record.OwnershipConfirmed || record.CompletionScope != CompletionInvocation || record.FunctionError || record.FunctionDiagnostic != nil || record.Stdout == nil || record.Stdout.Bytes != 0 || record.Tail.Bytes != 0 {
								t.Fatalf("private evidence changed %+v", record)
							}
							records++
						}
						if records != len(seen) {
							t.Fatalf("diagnostics count%d native invocations%d", records, len(seen))
						}
					}
				})
			}
		})
	}
}

// This isolates the real Runtime API response admission/ReadAll/JSON-validation
// boundary. It is not a process-launch, HTTP-network or handler-serialization
// benchmark. Request/result ownership and the normal native size check remain.
var performanceRound2ReplySink invocationResult

func BenchmarkPerformanceRound2RuntimeResponse(b *testing.B) {
	for _, size := range []int{1 << 10, 256 << 10, 4 << 20} {
		b.Run(fmt.Sprintf("bytes_%d", size), func(b *testing.B) {
			payload := "\"" + strings.Repeat("x", size) + "\""
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				owner := &runtimeInvocation{ctx: context.Background(), input: invocation{requestID: "round2"}, delivered: true, result: make(chan invocationResult, 1)}
				request := httptest.NewRequest(http.MethodPost, runtimePrefix+"invocation/round2/response", strings.NewReader(payload))
				reply := httptest.NewRecorder()
				owner.ServeHTTP(reply, request)
				result := <-owner.result
				if reply.Code != http.StatusAccepted || result.functionError || len(result.payload) != len(payload) {
					b.Fatal("native Runtime API response admission changed")
				}
				performanceRound2ReplySink = result
			}
		})
	}
}
