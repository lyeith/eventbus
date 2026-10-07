package firehose

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDynamicRetryWindowRoutesOriginalDataToRetainedErrorPrefix(t *testing.T) {
	var available atomic.Bool
	var mu sync.Mutex
	var paths []string
	var data [][]byte
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		data = append(data, body)
		mu.Unlock()
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(sink.Close)
	manager := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	t.Cleanup(func() { _ = manager.Shutdown() })
	config := partitionConfig("retry-window")
	config.DynamicPartitioningConfiguration.RetryOptions = &RetryOptions{DurationInSeconds: 0}
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	record := []byte(`{"customer_id":"north","id":1}`)
	_, err = manager.PutRecord(stream, record)
	require.NoError(t, err)
	require.Error(t, manager.flushBuffer(t.Context(), stream))
	require.Error(t, manager.flushBuffer(t.Context(), stream))
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords)
	require.Equal(t, 1, snapshot.PendingObjects)
	available.Store(true)
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(paths), 3)
	require.Contains(t, paths[0], "tenant=north/")
	require.Contains(t, paths[1], "errors/dynamic-partitioning-failed/")
	for index := 2; index < len(paths); index++ {
		require.Equal(t, paths[1], paths[index], "error destination itself has a stable retry key")
		require.Equal(t, data[1], data[index])
	}
	var diagnostic map[string]interface{}
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(data[2]), &diagnostic))
	require.Equal(t, base64.StdEncoding.EncodeToString(record), diagnostic["rawData"])
	require.Equal(t, "DynamicPartitioning.DeliveryFailed", diagnostic["errorCode"])
}

func TestFirehoseDefaultsPrefixesAndExplicitUnsupportedFormats(t *testing.T) {
	manager := newTestFirehoseManager(t)
	config := partitionConfig("native")
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	for _, prefix := range []string{"unclosed!{timestamp:yyyy", "!{timestamp:unsupported}", "!{partitionKeyFromLambda:tenant}", "!{firehose:error-output-type}"} {
		config.Prefix = prefix
		_, err := manager.CreateConfiguredStream(t.Context(), config)
		require.Error(t, err, prefix)
	}
	config = partitionConfig("native")
	config.ErrorOutputPrefix = "!{timestamp:yyyy}/"
	_, err := manager.CreateConfiguredStream(t.Context(), config)
	require.Error(t, err)
	// Prefixes without timestamp expressions use the native default date suffix.
	rendered, err := renderPrefix("fixed/", time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), nil, "", false, false, false)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(rendered, "fixed/2020/01/02/03/"))
}

func TestNativePrefixTimestampMillisecondsAndQuotedLiterals(t *testing.T) {
	instant := time.Date(2020, 1, 2, 3, 4, 5, 987654321, time.UTC)
	rendered, err := timestampFormat("yyyy-MM-dd'T'HH:mm:ss.SSS/DDD/'literal'/''", instant)
	require.NoError(t, err)
	require.Equal(t, "2020-01-02T03:04:05.987/002/literal/'", rendered)
}
