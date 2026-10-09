//go:build performance

package ssm

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/lyeith/eventbus/internal/testperf"
)

func TestPerformanceRound2SSMPagination(t *testing.T) {
	traversals := 3
	if testing.Short() {
		traversals = 1 // One complete traversal is enough for instrumented correctness checks.
	}
	for _, size := range []int{100, 1000, 10000} {
		t.Run(fmt.Sprintf("names_%d", size), func(t *testing.T) {
			store := NewSSMStore()
			for index := 0; index < size; index++ {
				kind := "String"
				if index%10 == 0 {
					kind = "SecureString"
				}
				name := fmt.Sprintf("/round2/all/p%05d", index)
				if err := store.PutParameter(name, "round2-value", kind, false); err != nil {
					t.Fatal(err)
				}
			}
			// Sparse paths still visit the complete registry. Keep their actual
			// result cardinality fixed across registry sizes.
			for index := 0; index < 10; index++ {
				if err := store.PutParameter(fmt.Sprintf("/round2/needle/p%05d", index), "round2-value", "String", false); err != nil {
					t.Fatal(err)
				}
			}
			serving := httptest.NewServer(NewHandler(store))
			t.Cleanup(serving.Close)
			client := ssmsdk.NewFromConfig(aws.Config{
				Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("owned", "owned", ""),
				HTTPClient: serving.Client(),
			}, func(options *ssmsdk.Options) { options.BaseEndpoint = aws.String(serving.URL) })
			for _, selected := range []struct {
				path  string
				count int
			}{{"/round2/all", size}, {"/round2/needle", 10}} {
				name := fmt.Sprintf("ssm/registry_%d/matches_%d", size+10, selected.count)
				firstPage := make([]float64, 0, 20)
				for range 20 {
					started := time.Now()
					page, _, err := store.ListParametersByPath(selected.path, true, true, 10, "")
					firstPage = append(firstPage, float64(time.Since(started))/float64(time.Millisecond))
					if err != nil || len(page) != min(10, selected.count) {
						t.Fatalf("native first page: count=%d err=%v", len(page), err)
					}
				}
				testperf.Report(t, name, "core_first_page_ms", firstPage)
				core := make([]float64, 0, traversals)
				native := make([]float64, 0, traversals)
				for sample := range traversals {
					started := time.Now()
					token, previous, count, pages := "", "", 0, 0
					for {
						page, next, err := store.ListParametersByPath(selected.path, true, true, 10, token)
						if err != nil {
							t.Fatal(err)
						}
						for _, parameter := range page {
							if parameter.Name <= previous || !strings.HasPrefix(parameter.Name, selected.path+"/") || parameter.Value != "round2-value" {
								t.Fatalf("core pagination lost order, path boundary or decryption: %+v", parameter)
							}
							previous = parameter.Name
							count++
						}
						pages++
						if pages > (selected.count+9)/10 {
							t.Fatal("core cursor did not terminate within expected pages")
						}
						token = next
						if next == "" {
							break
						}
					}
					core = append(core, float64(time.Since(started))/float64(time.Millisecond))
					if count != selected.count || pages != (selected.count+9)/10 {
						t.Fatalf("core enumeration count=%d pages=%d", count, pages)
					}
					ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
					started = time.Now()
					paginator := ssmsdk.NewGetParametersByPathPaginator(client, &ssmsdk.GetParametersByPathInput{
						Path: aws.String(selected.path), Recursive: aws.Bool(true), WithDecryption: aws.Bool(true), MaxResults: aws.Int32(10),
					})
					count, pages, previous = 0, 0, ""
					for paginator.HasMorePages() {
						page, err := paginator.NextPage(ctx)
						if err != nil {
							cancel()
							t.Fatal(err)
						}
						for _, parameter := range page.Parameters {
							actual := aws.ToString(parameter.Name)
							if actual <= previous || !strings.HasPrefix(actual, selected.path+"/") || aws.ToString(parameter.Value) != "round2-value" {
								cancel()
								t.Fatalf("SDK pagination lost order, path boundary or decryption: %+v", parameter)
							}
							previous = actual
							count++
						}
						pages++
						if pages > (selected.count+9)/10 {
							cancel()
							t.Fatal("SDK cursor did not terminate within expected pages")
						}
					}
					native = append(native, float64(time.Since(started))/float64(time.Millisecond))
					cancel()
					if count != selected.count || pages != (selected.count+9)/10 {
						t.Fatalf("SDK enumeration count=%d pages=%d", count, pages)
					}
					t.Logf("PERFORMANCE_ROUND2_SSM sample=%d registry=%d matches=%d pages=%d producer=unchanged_go_sdk transport=loopback state=private_memory setup=excluded", sample, size+10, count, pages)
				}
				testperf.Report(t, name, "core_full_enumeration_ms", core)
				testperf.Report(t, name, "sdk_full_enumeration_ms", native)
			}
		})
	}
}
