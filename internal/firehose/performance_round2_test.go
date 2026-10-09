//go:build performance

package firehose

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/stretchr/testify/require"
)

// Records enter the actual configured stream owner; only destination transport
// is bounded in-memory I/O. The same small bodies isolate configured timezone
// preparation rather than metadata extraction, gzip or storage durability.
func TestPerformanceRound2FirehoseTimeZone(t *testing.T) {
	for _, timezone := range []string{"UTC", "Asia/Singapore", "America/New_York"} {
		t.Run(timezone, func(t *testing.T) {
			var objectPaths []string
			var delivered []byte
			manager := firehoseFollowupManager(t, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
				defer request.Body.Close()
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				objectPaths = append(objectPaths, request.URL.Path)
				delivered = append(delivered, body...)
				return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
			}))
			t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
			config := StreamConfig{
				Name: "round2-timezone", BucketARN: "arn:aws:s3:::bucket",
				RoleARN:           "arn:aws:iam::000000000000:role/firehose",
				CompressionFormat: "UNCOMPRESSED", CustomTimeZone: timezone,
				Prefix:            "data/!{timestamp:yyyy/MM/dd/HH/}",
				ErrorOutputPrefix: "errors/!{firehose:error-output-type}/",
			}
			stream := firehoseFollowupStream(t, manager, config)
			records := make([][]byte, 500)
			var expected bytes.Buffer
			for index := range records {
				records[index] = []byte(fmt.Sprintf("{\"id\":%d,\"value\":\"data\"}\n", index))
			}
			var wall, allocBytes, allocCount []float64
			for sample := range firehoseFollowupSamples {
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				results, err := manager.AcceptRecordBatch(t.Context(), stream, records)
				wall = append(wall, float64(time.Since(started))/float64(time.Millisecond))
				runtime.ReadMemStats(&after)
				allocBytes = append(allocBytes, float64(after.TotalAlloc-before.TotalAlloc))
				allocCount = append(allocCount, float64(after.Mallocs-before.Mallocs))
				require.NoError(t, err)
				require.Len(t, results, len(records))
				for index, result := range results {
					require.NotEmpty(t, result.RecordID)
					require.Empty(t, result.ErrorCode)
					_, _ = expected.Write(records[index])
				}
				snapshot, ok := manager.Snapshot(stream.Name)
				require.True(t, ok)
				require.Equal(t, (sample+1)*len(records), snapshot.BufferedRecords)
			}
			stream.mu.Lock()
			arrived := stream.buffer[0].arrived
			stream.mu.Unlock()
			location, err := time.LoadLocation(timezone)
			require.NoError(t, err)
			prefix := "/bucket/data/" + arrived.In(location).Format("2006/01/02/15/")
			require.NoError(t, manager.flushBuffer(t.Context(), stream))
			require.Equal(t, expected.Bytes(), delivered)
			require.NotEmpty(t, objectPaths)
			// A test can cross an hour during admission; each native partition is
			// valid. The first object's oldest record must use the configured zone.
			require.True(t, strings.HasPrefix(objectPaths[0], prefix), objectPaths[0])
			snapshot, _ := manager.Snapshot(stream.Name)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.BufferedBytes)
			testperf.Report(t, t.Name(), "admission_500_records_ms", wall)
			testperf.Report(t, t.Name(), "allocated_bytes", allocBytes)
			testperf.Report(t, t.Name(), "allocations", allocCount)
		})
	}
}
