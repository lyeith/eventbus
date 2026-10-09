//go:build performance

package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/stretchr/testify/require"
)

const firehoseFollowupSamples = 20

// The actual stream owner admits/processes records and builds/signs delivery
// objects. Only S3 transport is bounded in-memory I/O. Setup, worker cancellation,
// assertions and final joined delivery are outside the measurement intervals.
func firehoseFollowupManager(t *testing.T, transport http.RoundTripper) *FirehoseManager {
	t.Helper()
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	manager.httpClient.Transport = transport
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	return manager
}

func firehoseFollowupStream(t *testing.T, manager *FirehoseManager, config StreamConfig) *DeliveryStream {
	t.Helper()
	config.BufferingHints = BufferingHints{SizeInMBs: 128, IntervalInSeconds: 900}
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	// Existing stream tests use joined dormant workers to own manual flush timing.
	stream.cancel()
	<-stream.done
	return stream
}

func firehoseFollowupAccept(t *testing.T, manager *FirehoseManager, stream *DeliveryStream, records [][]byte) {
	t.Helper()
	results, err := manager.AcceptRecordBatch(t.Context(), stream, records)
	require.NoError(t, err)
	require.Len(t, results, len(records))
	for _, result := range results {
		require.Empty(t, result.ErrorCode)
		require.NotEmpty(t, result.RecordID)
	}
}

func TestPerformanceFollowupFirehoseMetadataBatch(t *testing.T) {
	for _, query := range []struct{ name, expression string }{
		{"simple", `{tenant:.customer_id}`},
		{"compound", `(.payload // .) | {tenant:(.customer_id|ascii_downcase),year:(.timestamp|strftime("%Y")),total:(.values|map(.n)|add|tostring)}`},
	} {
		t.Run(query.name, func(t *testing.T) {
			delivered := 0
			manager := firehoseFollowupManager(t, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
				defer request.Body.Close()
				compressed, err := gzip.NewReader(request.Body)
				if err != nil {
					return nil, err
				}
				body, err := io.ReadAll(compressed)
				closeErr := compressed.Close()
				if err != nil {
					return nil, err
				}
				if closeErr != nil {
					return nil, closeErr
				}
				delivered += bytes.Count(body, []byte{'\n'})
				return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
			}))
			t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
			config := partitionConfig("metadata-" + query.name)
			config.ProcessingConfiguration.Processors[0].Parameters[1].ParameterValue = query.expression
			stream := firehoseFollowupStream(t, manager, config)
			records := make([][]byte, 500)
			for i := range records {
				records[i] = []byte(fmt.Sprintf(`{"customer_id":"NORTH","timestamp":1577934245,"values":[{"n":2},{"n":3}],"id":%d}`, i))
			}
			samples := make([]float64, 0, firehoseFollowupSamples)
			for sample := 0; sample < firehoseFollowupSamples; sample++ {
				started := time.Now()
				results, err := manager.AcceptRecordBatch(t.Context(), stream, records)
				elapsed := float64(time.Since(started)) / float64(time.Millisecond)
				require.NoError(t, err)
				for _, result := range results {
					require.Empty(t, result.ErrorCode)
					require.NotEmpty(t, result.RecordID)
				}
				snapshot, ok := manager.Snapshot(stream.Name)
				require.True(t, ok)
				require.Equal(t, (sample+1)*len(records), snapshot.BufferedRecords)
				samples = append(samples, elapsed)
			}
			require.NoError(t, manager.flushBuffer(t.Context(), stream))
			require.Equal(t, firehoseFollowupSamples*len(records), delivered)
			snapshot, _ := manager.Snapshot(stream.Name)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.BufferedBytes)
			testperf.Report(t, "firehose/metadata/"+query.name, "admission_500_records_ms", samples)
		})
	}
}

func TestPerformanceFollowupFirehoseBacklogAdmission(t *testing.T) {
	for _, backlog := range []int{0, 1000, 10000} {
		t.Run(fmt.Sprint(backlog), func(t *testing.T) {
			var available atomic.Bool
			var delivered atomic.Int64
			manager := firehoseFollowupManager(t, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
				defer request.Body.Close()
				if !available.Load() {
					_, err := io.Copy(io.Discard, request.Body)
					if err != nil {
						return nil, err
					}
					return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
				}
				compressed, err := gzip.NewReader(request.Body)
				if err != nil {
					return nil, err
				}
				body, err := io.ReadAll(compressed)
				closeErr := compressed.Close()
				if err != nil {
					return nil, err
				}
				if closeErr != nil {
					return nil, closeErr
				}
				delivered.Add(int64(bytes.Count(body, []byte{'\n'})))
				return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
			}))
			t.Cleanup(func() { available.Store(true); require.NoError(t, manager.Shutdown()) })
			stream := firehoseFollowupStream(t, manager, partitionConfig("backlog"))
			for offset := 0; offset < backlog; offset += 500 {
				records := make([][]byte, min(500, backlog-offset))
				for i := range records {
					records[i] = []byte(fmt.Sprintf(`{"customer_id":"partition-%05d","id":%d}`, offset+i, offset+i))
				}
				firehoseFollowupAccept(t, manager, stream, records)
			}
			if backlog != 0 {
				require.Error(t, manager.flushBuffer(t.Context(), stream))
			}
			snapshot, _ := manager.Snapshot(stream.Name)
			require.Equal(t, backlog, snapshot.BufferedRecords)
			require.Equal(t, backlog, snapshot.PendingObjects)
			records := make([][]byte, 500)
			for i := range records {
				records[i] = []byte(`{"customer_id":"new","id":1}`)
			}
			samples := make([]float64, 0, firehoseFollowupSamples)
			for sample := 0; sample < firehoseFollowupSamples; sample++ {
				started := time.Now()
				results, err := manager.AcceptRecordBatch(t.Context(), stream, records)
				elapsed := float64(time.Since(started)) / float64(time.Millisecond)
				require.NoError(t, err)
				for _, result := range results {
					require.Empty(t, result.ErrorCode)
					require.NotEmpty(t, result.RecordID)
				}
				snapshot, _ = manager.Snapshot(stream.Name)
				require.Equal(t, backlog+(sample+1)*500, snapshot.BufferedRecords)
				require.Equal(t, backlog, snapshot.PendingObjects)
				samples = append(samples, elapsed)
			}
			available.Store(true)
			require.NoError(t, manager.flushBuffer(t.Context(), stream))
			require.Equal(t, int64(backlog+firehoseFollowupSamples*500), delivered.Load())
			snapshot, _ = manager.Snapshot(stream.Name)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.BufferedBytes)
			testperf.Report(t, fmt.Sprintf("firehose/backlog/%d_pending_objects", backlog), "admission_500_records_ms", samples)
		})
	}
}

func TestPerformanceFollowupFirehoseGZIPContention(t *testing.T) {
	// Deterministic incompressible bytes keep the GZIP preparation interval visible;
	// fixed 32MiB fills are admitted through eight native <=4MiB batches.
	record := make([]byte, 512<<10)
	random := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < len(record); i += 8 {
		binary.LittleEndian.PutUint64(record[i:], random.Uint64())
	}
	batch := make([][]byte, 8)
	for i := range batch {
		batch[i] = record
	}
	sentinel := []byte("late-admission")
	expected := sha256.New()
	for range 64 {
		_, _ = expected.Write(record)
	}
	_, _ = expected.Write(sentinel)
	var admissionSamples, buildSamples []float64
	for sample := 0; sample < 5; sample++ {
		t.Run(fmt.Sprint(sample), func(t *testing.T) {
			deliveryEntered := make(chan struct{})
			release := make(chan struct{})
			delivered := sha256.New()
			calls := 0
			manager := firehoseFollowupManager(t, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
				defer request.Body.Close()
				compressed, err := gzip.NewReader(request.Body)
				if err != nil {
					return nil, err
				}
				_, err = io.Copy(delivered, compressed)
				closeErr := compressed.Close()
				if err != nil {
					return nil, err
				}
				if closeErr != nil {
					return nil, closeErr
				}
				calls++
				if calls == 1 {
					close(deliveryEntered)
					select {
					case <-release:
					case <-request.Context().Done():
						return nil, request.Context().Err()
					}
				}
				return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
			}))
			// All launched operations join before manager shutdown and fixture disposal.
			config := StreamConfig{Name: "gzip", BucketARN: "arn:aws:s3:::bucket", RoleARN: "arn:aws:iam::000000000000:role/firehose", CompressionFormat: "GZIP", CustomTimeZone: "UTC"}
			stream := firehoseFollowupStream(t, manager, config)
			for range 8 {
				firehoseFollowupAccept(t, manager, stream, batch)
			}
			flushDone := make(chan error, 1)
			admissionDone := make(chan struct {
				results []RecordResult
				err     error
				elapsed float64
			}, 1)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			releaseOpen := true
			flushJoined, admissionJoined, admissionStarted := false, false, false
			t.Cleanup(func() {
				if releaseOpen {
					close(release)
				}
				cancel()
				if !flushJoined {
					<-flushDone
				}
				if admissionStarted && !admissionJoined {
					<-admissionDone
				}
				require.NoError(t, manager.Shutdown())
			})
			stream.mu.Lock()
			go func() { flushDone <- manager.flushBuffer(ctx, stream) }()
			until := time.Now().Add(3 * time.Second)
			for len(stream.flushSlot) == 0 && time.Now().Before(until) {
				runtime.Gosched()
			}
			if len(stream.flushSlot) == 0 {
				stream.mu.Unlock()
				cancel()
				<-flushDone
				flushJoined = true
				t.Fatal("flush did not acquire its serialized attempt")
			}
			buildStarted := time.Now()
			stream.mu.Unlock()
			runtime.Gosched()
			admissionStarted = true
			go func() {
				started := time.Now()
				results, err := manager.AcceptRecordBatch(ctx, stream, [][]byte{sentinel})
				admissionDone <- struct {
					results []RecordResult
					err     error
					elapsed float64
				}{results, err, float64(time.Since(started)) / float64(time.Millisecond)}
			}()
			select {
			case <-deliveryEntered:
			case <-ctx.Done():
				cancel()
				<-flushDone
				flushJoined = true
				t.Fatal("GZIP destination did not begin")
			}
			buildElapsed := float64(time.Since(buildStarted)) / float64(time.Millisecond)
			admitted := <-admissionDone
			admissionJoined = true
			require.NoError(t, admitted.err)
			require.Len(t, admitted.results, 1)
			require.Empty(t, admitted.results[0].ErrorCode)
			snapshot, _ := manager.Snapshot(stream.Name)
			require.Equal(t, 65, snapshot.BufferedRecords, "admission remains retained until the destination response joins")
			close(release)
			releaseOpen = false
			flushErr := <-flushDone
			flushJoined = true
			require.NoError(t, flushErr)
			require.NoError(t, manager.flushBuffer(t.Context(), stream))
			require.Equal(t, expected.Sum(nil), delivered.Sum(nil), "every original byte and the concurrent sentinel must be delivered once in order")
			snapshot, _ = manager.Snapshot(stream.Name)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.BufferedBytes)
			admissionSamples = append(admissionSamples, admitted.elapsed)
			buildSamples = append(buildSamples, buildElapsed)
		})
	}
	testperf.Report(t, "firehose/gzip/32MiB_concurrent_admission", "admission_ms", admissionSamples)
	testperf.Report(t, "firehose/gzip/32MiB_concurrent_admission", "build_and_bounded_transport_read_ms", buildSamples)
}
