//go:build performance

package secrets

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
)

// This measures the store's native value/snapshot owner with identical fixture
// populations and APIs on both source versions. Creation is outside timing.
func TestPerformanceReviewSecretsCanonicalLookup(t *testing.T) {
	const samples, batch = 20, 100
	for _, count := range []int{10, 1000, 10000} {
		t.Run(fmt.Sprintf("secrets_%d", count), func(t *testing.T) {
			store := NewSecretsStore("us-east-1", "000000000000")
			var selected *Secret
			for index := 0; index < count; index++ {
				secret, err := store.CreateSecret(fmt.Sprintf("performance/s%05d", index), "owned-value", "", nil)
				if err != nil {
					t.Fatal(err)
				}
				if index == count/2 {
					selected = secret
				}
			}
			for _, lookup := range []struct {
				name, id string
				absent   bool
			}{
				{"name", selected.Name, false},
				{"arn", selected.ARN, false},
				{"missing_arn", "arn:aws:secretsmanager:us-east-1:000000000000:secret:performance/missing-abcdef", true},
			} {
				t.Run(lookup.name, func(t *testing.T) {
					check := func(secret *Secret, err error) {
						t.Helper()
						if lookup.absent {
							if api, ok := err.(*APIError); !ok || api.Code != "ResourceNotFoundException" || secret != nil {
								t.Fatalf("missing canonical ARN changed: secret=%v error=%v", secret, err)
							}
						} else if err != nil || secret == nil || secret.ARN != selected.ARN || secret.SecretString != "owned-value" {
							t.Fatalf("canonical identity/value changed: secret=%v error=%v", secret, err)
						}
					}
					check(store.GetValue(lookup.id, "", ""))
					wall, allocatedBytes, allocations := []float64{}, []float64{}, []float64{}
					for sample := 0; sample < samples; sample++ {
						var before, after runtime.MemStats
						runtime.ReadMemStats(&before)
						started := time.Now()
						var secret *Secret
						var err error
						for operation := 0; operation < batch; operation++ {
							secret, err = store.GetValue(lookup.id, "", "")
						}
						elapsed := time.Since(started)
						runtime.ReadMemStats(&after)
						check(secret, err)
						wall = append(wall, float64(elapsed)/float64(time.Millisecond)/batch)
						allocatedBytes = append(allocatedBytes, float64(after.TotalAlloc-before.TotalAlloc)/batch)
						allocations = append(allocations, float64(after.Mallocs-before.Mallocs)/batch)
					}
					name := fmt.Sprintf("secrets_%d_%s", count, lookup.name)
					t.Logf("PERFORMANCE_SECRETS_FIXTURE case=%s operations_per_sample=%d native=GetValue setup=excluded", name, batch)
					testperf.Report(t, name, "lookup_per_op_ms", wall)
					testperf.Report(t, name, "bytes_per_op", allocatedBytes)
					testperf.Report(t, name, "allocs_per_op", allocations)
				})
			}
		})
	}
}
