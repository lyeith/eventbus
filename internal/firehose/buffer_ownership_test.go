package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func pausedBuildStream(t *testing.T, builder objectBuilder, transport http.RoundTripper, config StreamConfig, release func()) (*FirehoseManager, *DeliveryStream) {
	t.Helper()
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	manager.buildObject = builder
	manager.httpClient.Transport = transport
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	t.Cleanup(func() { release(); require.NoError(t, manager.Shutdown()) })
	config.BufferingHints = BufferingHints{SizeInMBs: 128, IntervalInSeconds: 900}
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	stream.cancel()
	<-stream.done
	return manager, stream
}

func acceptBuildRecords(t *testing.T, manager *FirehoseManager, stream *DeliveryStream, records ...[]byte) {
	t.Helper()
	results, err := manager.AcceptRecordBatch(t.Context(), stream, records)
	require.NoError(t, err)
	for _, result := range results {
		require.Empty(t, result.ErrorCode)
		require.NotEmpty(t, result.RecordID)
	}
}

func decodedBuildRequest(request *http.Request) ([]byte, error) {
	defer request.Body.Close()
	compressed, err := gzip.NewReader(request.Body)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(compressed)
	closeErr := compressed.Close()
	return body, errors.Join(err, closeErr)
}

func TestObjectBuildAllowsQuotaCountedConcurrentSamePartitionAdmission(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	unblock := func() { once.Do(func() { close(release) }) }
	builder := func(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return makeObject(ctx, config, location, records)
	}
	var delivered [][]byte
	manager, stream := pausedBuildStream(t, builder, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		body, err := decodedBuildRequest(request)
		if err != nil {
			return nil, err
		}
		delivered = append(delivered, body)
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}), partitionConfig("building"), unblock)
	first, late := []byte(`{"customer_id":"north","id":1}`), []byte(`{"customer_id":"north","id":2}`)
	manager.bufferLimit = len(first) + len(late)
	acceptBuildRecords(t, manager, stream, first)
	finished := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-finished
		}
	})
	go func() { finished <- manager.flushBuffer(t.Context(), stream) }()
	awaitFirehoseSignal(t, entered)
	unlocked := stream.mu.TryLock()
	if unlocked {
		stream.mu.Unlock()
	}
	require.True(t, unlocked, "native object preparation must not own the admission mutex")
	registryUnlocked := manager.mu.TryLock()
	if registryUnlocked {
		manager.mu.Unlock()
	}
	require.True(t, registryUnlocked, "object preparation must not block registry mutations")
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords)
	require.Equal(t, len(first), snapshot.BufferedBytes)
	oversized := []byte(`{"customer_id":"north","id":"too-big-to-fit"}`)
	results, err := manager.AcceptRecordBatch(t.Context(), stream, [][]byte{oversized, late})
	require.NoError(t, err)
	require.Equal(t, "ServiceUnavailableException", results[0].ErrorCode, "building records still consume quota")
	require.Empty(t, results[1].ErrorCode)
	require.NotEmpty(t, results[1].RecordID)
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Equal(t, 2, snapshot.BufferedRecords)
	require.Equal(t, manager.bufferLimit, snapshot.BufferedBytes)
	unblock()
	flushErr := <-finished
	joined = true
	require.NoError(t, flushErr)
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords, "same-partition concurrent appends are outside the captured prefix")
	require.Equal(t, len(late), snapshot.BufferedBytes)
	require.Equal(t, [][]byte{append(bytes.Clone(first), '\n')}, delivered)
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	require.Equal(t, [][]byte{append(bytes.Clone(first), '\n'), append(bytes.Clone(late), '\n')}, delivered)
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
	require.Zero(t, snapshot.BufferedBytes)
}

func TestObjectBuildFailureKeepsWholeSnapshotAndConcurrentAppends(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	unblock := func() { once.Do(func() { close(release) }) }
	buildFailure := errors.New("owned object preparation failed")
	builder := func(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
			return nil, buildFailure
		}
		return makeObject(ctx, config, location, records)
	}
	delivered := map[string][]byte{}
	manager, stream := pausedBuildStream(t, builder, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		body, err := decodedBuildRequest(request)
		if err != nil {
			return nil, err
		}
		if strings.Contains(request.URL.Path, "tenant=north/") {
			delivered["north"] = append(delivered["north"], body...)
		} else {
			delivered["south"] = append(delivered["south"], body...)
		}
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}), partitionConfig("build-fault"), unblock)
	north, south, late := []byte(`{"customer_id":"north","id":1}`), []byte(`{"customer_id":"south","id":2}`), []byte(`{"customer_id":"north","id":3}`)
	acceptBuildRecords(t, manager, stream, north, south)
	finished := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-finished
		}
	})
	go func() { finished <- manager.flushBuffer(t.Context(), stream) }()
	awaitFirehoseSignal(t, entered)
	acceptBuildRecords(t, manager, stream, late)
	unblock()
	err := <-finished
	joined = true
	require.ErrorIs(t, err, buildFailure)
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 3, snapshot.BufferedRecords)
	require.Equal(t, len(north)+len(south)+len(late), snapshot.BufferedBytes)
	require.Zero(t, snapshot.PendingObjects, "no partially built group is published")
	require.Empty(t, delivered)
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	require.Equal(t, append(append(bytes.Clone(north), '\n'), append(bytes.Clone(late), '\n')...), delivered["north"])
	require.Equal(t, append(bytes.Clone(south), '\n'), delivered["south"])
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
	require.Zero(t, snapshot.BufferedBytes)
}

func TestCanceledObjectBuildCannotPublishPreparedObjects(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	unblock := func() { once.Do(func() { close(release) }) }
	builder := func(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return makeObject(context.WithoutCancel(ctx), config, location, records)
		}
		return makeObject(ctx, config, location, records)
	}
	var deliveries atomic.Int32
	manager, stream := pausedBuildStream(t, builder, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		deliveries.Add(1)
		_, err := io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}), partitionConfig("cancel-build"), unblock)
	record := []byte(`{"customer_id":"north"}`)
	acceptBuildRecords(t, manager, stream, record)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-finished
		}
	})
	go func() { finished <- manager.flushBuffer(ctx, stream) }()
	awaitFirehoseSignal(t, entered)
	cancel()
	unblock()
	err := <-finished
	joined = true
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, deliveries.Load())
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords)
	require.Equal(t, len(record), snapshot.BufferedBytes)
	require.Zero(t, snapshot.PendingObjects)
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	require.EqualValues(t, 1, deliveries.Load())
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
}

func TestDeleteFencesAdmissionWhileAcceptedObjectBuildJoins(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	unblock := func() { once.Do(func() { close(release) }) }
	builder := func(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return makeObject(ctx, config, location, records)
	}
	manager, stream := pausedBuildStream(t, builder, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		_, err := io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	}), partitionConfig("stop-build"), unblock)
	acceptBuildRecords(t, manager, stream, []byte(`{"customer_id":"north"}`))
	finished := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-finished
		}
	})
	go func() { finished <- manager.flushBuffer(t.Context(), stream) }()
	awaitFirehoseSignal(t, entered)
	deletion, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.DeleteStream(deletion, stream.Name), context.DeadlineExceeded)
	require.Same(t, stream, manager.GetStream(stream.Name), "failed deletion retains original ownership")
	results, err := manager.AcceptRecordBatch(t.Context(), stream, [][]byte{[]byte(`{"customer_id":"north","late":true}`)})
	require.NoError(t, err)
	require.Equal(t, "ServiceUnavailableException", results[0].ErrorCode)
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, "DELETING", snapshot.Status)
	require.Equal(t, 1, snapshot.BufferedRecords)
	unblock()
	flushErr := <-finished
	joined = true
	require.NoError(t, flushErr)
	require.NoError(t, manager.DeleteStream(t.Context(), stream.Name))
	require.Nil(t, manager.GetStream(stream.Name))
}

func TestRecordCountQuotaIncludesBuildingAndResponseCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	body := &firehoseBlockingClose{entered: make(chan struct{}), release: make(chan struct{})}
	var bodyOnce sync.Once
	releaseBody := func() { bodyOnce.Do(func() { close(body.release) }) }
	builder := func(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
		close(entered)
		<-release
		return makeObject(ctx, config, location, records)
	}
	config := StreamConfig{Name: "record-count", BucketARN: "arn:aws:s3:::bucket", RoleARN: "arn:aws:iam::000000000000:role/firehose", CompressionFormat: "UNCOMPRESSED", CustomTimeZone: "UTC"}
	manager, stream := pausedBuildStream(t, builder, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		_, err := io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		return firehoseTestResponse(body), nil
	}), config, func() { unblock(); releaseBody() })
	batch := make([][]byte, 500)
	for range 200 {
		acceptBuildRecords(t, manager, stream, batch...)
	}
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 100000, snapshot.BufferedRecords)
	require.Zero(t, snapshot.BufferedBytes)
	finished := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		unblock()
		releaseBody()
		if !joined {
			<-finished
		}
	})
	go func() { finished <- manager.flushBuffer(t.Context(), stream) }()
	awaitFirehoseSignal(t, entered)
	results, err := manager.AcceptRecordBatch(t.Context(), stream, [][]byte{nil})
	require.NoError(t, err)
	require.Equal(t, "ServiceUnavailableException", results[0].ErrorCode)
	unblock()
	awaitFirehoseSignal(t, body.entered)
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Equal(t, 100000, snapshot.BufferedRecords, "2xx without joined body cleanup frees no records")
	require.Equal(t, 1, snapshot.PendingObjects)
	releaseBody()
	flushErr := <-finished
	joined = true
	require.NoError(t, flushErr)
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
	require.Zero(t, snapshot.BufferedBytes)
}

func TestDevAbortJoinsObjectPreparationBeforeReleasingRetainedLease(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	manager.buildObject = func(ctx context.Context, _ StreamConfig, _ *time.Location, _ []bufferedRecord) (*deliveryObject, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, ctx.Err()
	}
	var deliveries atomic.Int32
	manager.httpClient.Transport = firehoseTestTransport(func(*http.Request) (*http.Response, error) {
		deliveries.Add(1)
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	})
	activity := &firehoseTestActivity{}
	require.NoError(t, manager.SetDevActivity(activity))
	t.Cleanup(func() { unblock(); _ = manager.DevAbortJoin(context.Background()) })
	stream, err := manager.CreateStream("abort-build", "bucket", "", "", 128, 900)
	require.NoError(t, err)
	acceptBuildRecords(t, manager, stream, []byte("owned original"))
	require.NoError(t, manager.DevBeginDrain())
	awaitFirehoseSignal(t, entered)
	abort, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.DevAbortJoin(abort), context.DeadlineExceeded)
	awaitFirehoseSignal(t, canceled)
	count, completed, dirty := activity.counts()
	require.Equal(t, 1, count)
	require.Zero(t, completed)
	require.Zero(t, dirty)
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords)
	require.Equal(t, len("owned original"), snapshot.BufferedBytes)
	require.Zero(t, snapshot.PendingObjects)
	unblock()
	require.ErrorContains(t, manager.DevAbortJoin(t.Context()), "1 retained records")
	count, completed, dirty = activity.counts()
	require.Zero(t, count)
	require.Equal(t, 1, completed)
	require.Equal(t, 1, dirty)
	require.Zero(t, deliveries.Load())
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords, "joined abort preserves original data and quota evidence")
	require.Error(t, manager.DevEvidence())
}

func TestRetryReplacementFailureOrCancellationPreservesPendingOwnership(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "build-failure"
		if canceled {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var calls atomic.Int32
			unblock := func() { once.Do(func() { close(release) }) }
			buildFailure := errors.New("retry replacement preparation failed")
			builder := func(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
				if calls.Add(1) == 2 {
					close(entered)
					<-release
					if canceled {
						return makeObject(context.WithoutCancel(ctx), config, location, records)
					}
					return nil, buildFailure
				}
				return makeObject(ctx, config, location, records)
			}
			var available atomic.Bool
			delivered := map[string][]byte{}
			config := partitionConfig("retry-build")
			config.DynamicPartitioningConfiguration.RetryOptions = &RetryOptions{DurationInSeconds: 0}
			manager, stream := pausedBuildStream(t, builder, firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
				var body []byte
				var err error
				errorOutput := strings.Contains(request.URL.Path, "errors/dynamic-partitioning-failed/")
				if errorOutput {
					body, err = io.ReadAll(request.Body)
					_ = request.Body.Close()
				} else {
					body, err = decodedBuildRequest(request)
				}
				if err != nil {
					return nil, err
				}
				if !available.Load() {
					return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil))}, nil
				}
				if errorOutput {
					delivered["error"] = append(delivered["error"], body...)
				} else {
					delivered["south"] = append(delivered["south"], body...)
				}
				return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
			}), config, unblock)
			north, south := []byte(`{"customer_id":"north","id":1}`), []byte(`{"customer_id":"south","id":2}`)
			acceptBuildRecords(t, manager, stream, north)
			require.Error(t, manager.flushBuffer(t.Context(), stream))
			stream.mu.Lock()
			original := stream.pending[0]
			originalKey, originalData := original.key, bytes.Clone(original.data)
			stream.mu.Unlock()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			finished := make(chan error, 1)
			joined := false
			t.Cleanup(func() {
				available.Store(true)
				unblock()
				if !joined {
					<-finished
				}
			})
			go func() { finished <- manager.flushBuffer(ctx, stream) }()
			awaitFirehoseSignal(t, entered)
			acceptBuildRecords(t, manager, stream, south)
			snapshot, _ := manager.Snapshot(stream.Name)
			require.Equal(t, 2, snapshot.BufferedRecords)
			require.Equal(t, len(north)+len(south), snapshot.BufferedBytes)
			require.Equal(t, 1, snapshot.PendingObjects)
			if canceled {
				cancel()
			}
			unblock()
			err := <-finished
			joined = true
			if canceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, buildFailure)
			}
			stream.mu.Lock()
			pending := append([]*deliveryObject(nil), stream.pending...)
			stream.mu.Unlock()
			require.Len(t, pending, 1)
			require.Same(t, original, pending[0])
			require.Equal(t, originalKey, original.key)
			require.Equal(t, originalData, original.data)
			require.Empty(t, original.records[0].errorType)
			snapshot, _ = manager.Snapshot(stream.Name)
			require.Equal(t, 2, snapshot.BufferedRecords)
			available.Store(true)
			require.NoError(t, manager.flushBuffer(t.Context(), stream))
			require.Equal(t, append(bytes.Clone(south), '\n'), delivered["south"])
			var diagnostic struct {
				RawData string `json:"rawData"`
			}
			require.NoError(t, json.Unmarshal(bytes.TrimSpace(delivered["error"]), &diagnostic))
			require.Equal(t, base64.StdEncoding.EncodeToString(north), diagnostic.RawData)
			snapshot, _ = manager.Snapshot(stream.Name)
			require.Zero(t, snapshot.BufferedRecords)
			require.Zero(t, snapshot.BufferedBytes)
			require.Zero(t, snapshot.PendingObjects)
		})
	}
}
