//go:build performance && (linux || darwin)

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/rs/zerolog"
)

// A start barrier at the downstream HTTP boundary makes every measured
// producer an actual initial-cache-miss caller. It adds no timed sleep and
// leaves Lambda admission, native REQUEST payload, runtime execution, policy
// parse/evaluation and proxy mapping in their existing owners.
func TestPerformanceRound2GatewayAuthorizerBurst(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, producers := range []int{1, 8, 16} {
			t.Run(fmt.Sprintf("warm_%t/producers_%d", warm, producers), func(t *testing.T) {
				directory := t.TempDir()
				tracePath := filepath.Join(directory, "authorizer.jsonl")
				module := `const { appendFileSync } = require('node:fs');
exports.handler = async (event, context) => {
  if (!context.awsRequestId || event.type !== 'REQUEST') throw Error('invalid native authorizer contract');
  appendFileSync(process.env.ROUND2_TRACE, JSON.stringify({request_id:context.awsRequestId,pid:process.pid,method_arn:event.methodArn})+'\n', {mode:0o600});
  return {principalId:'round2-owned',policyDocument:{Version:'2012-10-17',Statement:[{Action:'execute-api:Invoke',Effect:'Allow',Resource:event.methodArn}]}};
};`
				if err := os.WriteFile(filepath.Join(directory, "auth.cjs"), []byte(module), 0600); err != nil {
					t.Fatal(err)
				}
				cfg := &lambdaservice.Config{Functions: map[string]lambdaservice.Function{
					"round2-auth": {Runtime: "node", Handler: "auth.cjs#handler", Environment: map[string]string{"ROUND2_TRACE": tracePath}, Timeout: 5 * time.Second},
				}}
				if warm {
					cfg.DevWarm = &lambdaservice.DevWarmConfig{MaxWorkers: 1}
				}
				functions, err := lambdaservice.NewService(cfg, directory)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := functions.Close(ctx); err != nil {
						t.Error(err)
					}
				})
				var gateMu sync.Mutex
				var gate chan struct{}
				admissions, wanted := 0, 0
				aws := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gateMu.Lock()
					if gate != nil {
						admissions++
						if admissions == wanted {
							close(gate)
						}
						release := gate
						gateMu.Unlock()
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
					} else {
						gateMu.Unlock()
					}
					functions.ServeHTTP(w, r)
				}))
				t.Cleanup(aws.Close)
				ttl := 300
				edge := newTestGateway(t, Config{
					Authorizers: map[string]AuthorizerConfig{"auth": {Type: "REQUEST", InvokeURL: aws.URL + "/2015-03-31/functions/round2-auth/invocations", TTL: &ttl, IdentitySources: []string{"method.request.header.Authorization"}, Timeout: 10 * time.Second}},
					Routes:      []RouteConfig{{Path: "/private/{id}", Method: "GET", Authorizer: "auth", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: "http://round2.invalid/backend/{id}"}}},
				}, Options{Logger: zerolog.Nop()})
				edge.routes[0].proxy.Transport = performanceGatewayTransport(func(request *http.Request) (*http.Response, error) {
					if request.Body != nil {
						_ = request.Body.Close()
					}
					return &http.Response{StatusCode: 204, Header: make(http.Header), Body: http.NoBody, Request: request}, nil
				})
				request := func(path string) *http.Request {
					r := httptest.NewRequest("GET", path, nil)
					r.Header.Set("Authorization", "Bearer round2-owned")
					return r
				}
				// Real native priming excludes the first Node start from warm
				// burst timing and checks the initial cached policy boundary.
				primed := httptest.NewRecorder()
				edge.ServeHTTP(primed, request("/private/one"))
				if primed.Code != 204 {
					t.Fatalf("prime status %d", primed.Code)
				}
				refused := httptest.NewRecorder()
				edge.ServeHTTP(refused, request("/private/two"))
				if refused.Code != 403 {
					t.Fatalf("cached exact policy escaped current ARN: %d", refused.Code)
				}
				totalExpected := 1
				warmCap := 0
				if warm {
					warmCap = 1
				}
				var batch, perCall []float64
				for sample := 0; sample < 3; sample++ {
					authorizer := edge.authorizers["auth"]
					authorizer.mu.Lock()
					clear(authorizer.cache)
					authorizer.mu.Unlock()
					gateMu.Lock()
					gate, admissions, wanted = make(chan struct{}), 0, producers
					gateMu.Unlock()
					start := make(chan struct{})
					results := make([]int, producers)
					elapsed := make([]float64, producers)
					var joined sync.WaitGroup
					ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
					for index := range producers {
						joined.Add(1)
						go func(index int) {
							defer joined.Done()
							<-start
							started := time.Now()
							response := httptest.NewRecorder()
							edge.ServeHTTP(response, request("/private/one").WithContext(ctx))
							elapsed[index] = float64(time.Since(started)) / float64(time.Millisecond)
							results[index] = response.Code
						}(index)
					}
					started := time.Now()
					close(start)
					joined.Wait()
					batch = append(batch, float64(time.Since(started))/float64(time.Millisecond))
					cancel()
					for _, code := range results {
						if code != 204 {
							t.Fatalf("native burst status %d", code)
						}
					}
					gateMu.Lock()
					actualAdmissions := admissions
					gate = nil
					gateMu.Unlock()
					if actualAdmissions != producers {
						t.Fatalf("initial miss barrier admissions=%d want=%d", actualAdmissions, producers)
					}
					totalExpected += producers
					data, err := os.ReadFile(tracePath)
					if err != nil {
						t.Fatal(err)
					}
					lines := strings.Split(strings.TrimSpace(string(data)), "\n")
					if len(lines) != totalExpected {
						t.Fatalf("actual native handler calls=%d want=%d", len(lines), totalExpected)
					}
					ids := make(map[string]bool)
					pids := make(map[int]bool)
					for _, line := range lines {
						var trace struct {
							RequestID string `json:"request_id"`
							PID       int    `json:"pid"`
							ARN       string `json:"method_arn"`
						}
						if err := json.Unmarshal([]byte(line), &trace); err != nil {
							t.Fatal(err)
						}
						if trace.RequestID == "" || ids[trace.RequestID] || !strings.HasSuffix(trace.ARN, "/GET/private/one") {
							t.Fatalf("native correlation: %+v", trace)
						}
						ids[trace.RequestID], pids[trace.PID] = true, true
					}
					if warm && len(pids) != 1 {
						t.Fatalf("warm service changed PID: %v", pids)
					}
					perCall = append(perCall, elapsed...)
					cached := httptest.NewRecorder()
					edge.ServeHTTP(cached, request("/private/one"))
					if cached.Code != 204 {
						t.Fatalf("post-burst cache status %d", cached.Code)
					}
					after, err := os.ReadFile(tracePath)
					if err != nil || len(after) != len(data) {
						t.Fatalf("cache hit invoked backend: err=%v", err)
					}
					t.Logf("PERFORMANCE_ROUND2_AUTH sample=%d warm=%t warm_cap=%d producers=%d native_invocations=%d actual_distinct_pids=%d initial_miss_barrier=true all_calls_joined=true", sample, warm, warmCap, producers, actualAdmissions, len(pids))
				}
				name := fmt.Sprintf("gateway/initial_miss/warm_%t/producers_%d", warm, producers)
				testperf.Report(t, name, "burst_wall_ms", batch)
				testperf.Report(t, name, "request_wall_ms", perCall)
				if err := edge.Close(); err != nil {
					t.Fatal(err)
				}
				aws.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := functions.Close(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
