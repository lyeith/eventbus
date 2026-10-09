package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingPreparedMetadata struct {
	native                                    *GoJQMetadataExtractor
	prepareCalls, validateCalls, extractCalls atomic.Int32
	failure                                   error
	empty                                     bool
}

func (extractor *countingPreparedMetadata) Validate(ctx context.Context, query string) error {
	extractor.validateCalls.Add(1)
	return extractor.native.Validate(ctx, query)
}
func (extractor *countingPreparedMetadata) Extract(ctx context.Context, query string, record []byte) (map[string]string, error) {
	extractor.extractCalls.Add(1)
	return extractor.native.Extract(ctx, query, record)
}
func (extractor *countingPreparedMetadata) Prepare(ctx context.Context, query string) (RecordMetadataExtractor, error) {
	extractor.prepareCalls.Add(1)
	if extractor.failure != nil {
		return nil, extractor.failure
	}
	if extractor.empty {
		return nil, nil
	}
	return extractor.native.Prepare(ctx, query)
}

func TestConfiguredStreamOwnsPreparedQueryAcrossConcurrentPuts(t *testing.T) {
	manager := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	manager.httpClient.Transport = firehoseTestTransport(func(request *http.Request) (*http.Response, error) {
		_, err := io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		return firehoseTestResponse(io.NopCloser(bytes.NewReader(nil))), nil
	})
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	extractor := &countingPreparedMetadata{native: NewGoJQMetadataExtractor()}
	require.NoError(t, manager.SetMetadataExtractor(extractor))
	config := partitionConfig("prepared")
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	stream.cancel()
	<-stream.done
	require.EqualValues(t, 1, extractor.prepareCalls.Load())
	require.Zero(t, extractor.validateCalls.Load(), "preparation owns compile-time validation once")
	config.ProcessingConfiguration.Processors[0].Parameters[1].ParameterValue = "changed fixture query"
	var workers sync.WaitGroup
	failures := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			records := make([][]byte, 50)
			for i := range records {
				records[i] = []byte(fmt.Sprintf(`{"customer_id":"tenant-%d","id":%d}`, worker, i))
			}
			results, err := manager.AcceptRecordBatch(t.Context(), stream, records)
			if err == nil {
				for _, result := range results {
					if result.ErrorCode != "" {
						err = errors.New(result.ErrorCode)
						break
					}
				}
			}
			failures <- err
		}(worker)
	}
	workers.Wait()
	for range 8 {
		require.NoError(t, <-failures)
	}
	require.EqualValues(t, 1, extractor.prepareCalls.Load())
	require.Zero(t, extractor.extractCalls.Load(), "record requests reuse the owning stream's prepared runtime")
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 400, snapshot.BufferedRecords)
	stream.mu.Lock()
	buffer := append([]bufferedRecord(nil), stream.buffer...)
	stream.mu.Unlock()
	for _, record := range buffer {
		require.Empty(t, record.errorType)
		require.Contains(t, record.group, "tenant=tenant-")
	}
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
}

func TestPreparedQueryHasFreshExecutionAndCancellationDoesNotPoisonReuse(t *testing.T) {
	extractor := NewGoJQMetadataExtractor()
	query, err := extractor.Prepare(t.Context(), `def spin: spin; if .wait then spin else {key:.value} end`)
	require.NoError(t, err)
	canceled, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err = query.Extract(canceled, []byte(`{"wait":true}`))
	require.Error(t, err)
	require.ErrorIs(t, canceled.Err(), context.DeadlineExceeded)
	var workers sync.WaitGroup
	failures := make(chan error, 32)
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			expected := fmt.Sprint(i)
			keys, err := query.Extract(t.Context(), []byte(fmt.Sprintf(`{"value":"%s"}`, expected)))
			if err == nil && keys["key"] != expected {
				err = fmt.Errorf("execution used another record: %v", keys)
			}
			if err == nil {
				keys["key"] = "caller-owned mutation"
			}
			failures <- err
		}(i)
	}
	workers.Wait()
	for range 32 {
		require.NoError(t, <-failures)
	}
	keys, err := query.Extract(t.Context(), []byte(`{"value":"final"}`))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"key": "final"}, keys)
}

type legacyBoundMetadata struct {
	validated, extracted int
	query                string
}

func (extractor *legacyBoundMetadata) Validate(_ context.Context, query string) error {
	extractor.validated++
	extractor.query = query
	return nil
}
func (extractor *legacyBoundMetadata) Extract(ctx context.Context, query string, record []byte) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query != extractor.query {
		return nil, errors.New("bound query changed")
	}
	var value struct {
		CustomerID string `json:"customer_id"`
	}
	if err := json.Unmarshal(record, &value); err != nil {
		return nil, err
	}
	extractor.extracted++
	return map[string]string{"tenant": value.CustomerID}, nil
}
func TestConfiguredStreamPreservesLegacyInjectedMetadataContract(t *testing.T) {
	manager := newTestFirehoseManager(t)
	extractor := &legacyBoundMetadata{}
	require.NoError(t, manager.SetMetadataExtractor(extractor))
	stream, err := manager.CreateConfiguredStream(t.Context(), partitionConfig("legacy-port"))
	require.NoError(t, err)
	stream.cancel()
	<-stream.done
	require.Equal(t, 1, extractor.validated)
	results, err := manager.AcceptRecordBatch(t.Context(), stream, [][]byte{[]byte(`{"customer_id":"north"}`), []byte(`{"customer_id":"south"}`)})
	require.NoError(t, err)
	for _, result := range results {
		require.Empty(t, result.ErrorCode)
	}
	require.Equal(t, 2, extractor.extracted)
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
}

func TestPreparedMetadataFailureCannotPublishStream(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			manager := newTestFirehoseManager(t)
			extractor := &countingPreparedMetadata{native: NewGoJQMetadataExtractor(), empty: empty}
			if !empty {
				extractor.failure = errors.New("compile fault")
			}
			require.NoError(t, manager.SetMetadataExtractor(extractor))
			_, err := manager.CreateConfiguredStream(t.Context(), partitionConfig("not-published"))
			require.ErrorContains(t, err, "invalid MetadataExtractionQuery")
			require.Nil(t, manager.GetStream("not-published"))
			require.EqualValues(t, 1, extractor.prepareCalls.Load())
			require.Zero(t, extractor.validateCalls.Load())
		})
	}
}
