//go:build performance

package gateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/rs/zerolog"
)

// The same frozen fixture measures the real request owner before and after
// static plan compilation. Only downstream I/O is replaced; setup is excluded.
func TestPerformanceReviewGatewayStaticPlans(t *testing.T) {
	for _, redactions := range []int{0, 10, 100} {
		t.Run(fmt.Sprintf("redactions_%d", redactions), func(t *testing.T) {
			cfg := Config{
				BasePath: "/edge", StageVariables: map[string]string{"version": "v1"},
				Routes: []RouteConfig{{Path: "/api/{proxy+}", Method: "GET", Integration: IntegrationConfig{
					Type: "HTTP_PROXY", URI: "http://performance.invalid/base/{proxy}/suffix?fixed=recipe&tag=recipe1&tag=recipe2",
					RemoveHeaders: []string{"Authorization", "Cookie"},
					RequestParameters: map[string]string{
						"integration.request.path.proxy":          "method.request.path.proxy",
						"integration.request.header.X-Source":     "method.request.header.X-Source",
						"integration.request.querystring.source":  "method.request.querystring.source",
						"integration.request.querystring.version": "stageVariables.version",
					},
				}}},
			}
			for index := 0; index < 8; index++ {
				cfg.Routes[0].Integration.RequestParameters[fmt.Sprintf("integration.request.header.X-Static-%d", index)] = "'fixture'"
			}
			for index := 0; index < redactions; index++ {
				cfg.LogRedactions = append(cfg.LogRedactions, fmt.Sprintf("/unmatched/r%d/{id}", index))
			}
			if redactions > 0 {
				cfg.LogRedactions[redactions-1] = "/edge/api/{proxy+}"
			}
			gateway := newTestGateway(t, cfg, Options{Logger: zerolog.Nop()})
			gateway.routes[0].proxy.Transport = performanceGatewayTransport(func(request *http.Request) (*http.Response, error) {
				if request.Body != nil {
					defer request.Body.Close()
				}
				return &http.Response{StatusCode: 204, Header: http.Header{
					"X-Observed-Path":    []string{request.URL.EscapedPath()},
					"X-Observed-Query":   []string{request.URL.RawQuery},
					"X-Observed-Source":  []string{request.Header.Get("X-Source")},
					"X-Observed-Static":  []string{request.Header.Get("X-Static-7")},
					"X-Observed-Private": []string{request.Header.Get("Authorization") + request.Header.Get("Cookie")},
				}, Body: http.NoBody, Request: request}, nil
			})
			request := httptest.NewRequest("GET", "/edge/api/folder/a%20b?tag=caller&source=first&source=second", nil)
			request.Header.Set("X-Source", "owned")
			request.Header.Set("Authorization", "private")
			request.Header.Set("Cookie", "private")
			beforeURL, beforeHeaders := *request.URL, request.Header.Clone()
			check := func(response *httptest.ResponseRecorder) {
				t.Helper()
				if response.Code != 204 || response.Header().Get("X-Observed-Path") != "/base/folder/a%20b/suffix" ||
					response.Header().Get("X-Observed-Query") != "fixed=recipe&source=first%2Csecond&tag=recipe1&tag=recipe2&version=v1" ||
					response.Header().Get("X-Observed-Source") != "owned" || response.Header().Get("X-Observed-Static") != "fixture" ||
					response.Header().Get("X-Observed-Private") != "" {
					t.Fatalf("native mapping changed: status=%d headers=%v", response.Code, response.Header())
				}
			}
			verification := httptest.NewRecorder()
			gateway.ServeHTTP(verification, request)
			check(verification)
			wall, allocatedBytes, allocations := []float64{}, []float64{}, []float64{}
			for sample := 0; sample < performanceGatewaySamples; sample++ {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				var response *httptest.ResponseRecorder
				for operation := 0; operation < performanceGatewayBatch; operation++ {
					response = httptest.NewRecorder()
					gateway.ServeHTTP(response, request)
				}
				elapsed := time.Since(started)
				runtime.ReadMemStats(&after)
				check(response)
				wall = append(wall, float64(elapsed)/float64(time.Millisecond)/performanceGatewayBatch)
				allocatedBytes = append(allocatedBytes, float64(after.TotalAlloc-before.TotalAlloc)/performanceGatewayBatch)
				allocations = append(allocations, float64(after.Mallocs-before.Mallocs)/performanceGatewayBatch)
			}
			if *request.URL != beforeURL || !reflect.DeepEqual(request.Header, beforeHeaders) {
				t.Fatal("request mapping mutated the borrowed incoming request")
			}
			name := fmt.Sprintf("gateway_static_plans_redactions_%d", redactions)
			t.Logf("PERFORMANCE_GATEWAY_FIXTURE case=%s operations_per_sample=%d mappings=12 downstream=bounded_transport setup=excluded", name, performanceGatewayBatch)
			testperf.Report(t, name, "serve_http_per_op_ms", wall)
			testperf.Report(t, name, "bytes_per_op", allocatedBytes)
			testperf.Report(t, name, "allocs_per_op", allocations)
		})
	}
}
