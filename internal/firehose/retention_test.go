package firehose

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcknowledgedBuffersReleaseReferencesWithoutDroppingRetainedRecords(t *testing.T) {
	var available atomic.Bool
	available.Store(true)
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(sink.Close)
	manager := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	t.Cleanup(func() { available.Store(true); require.NoError(t, manager.Shutdown()) })
	stream, err := manager.CreateConfiguredStream(t.Context(), partitionConfig("references"))
	require.NoError(t, err)
	// Own manual flushes so the age/capacity assertions cannot race the worker.
	stream.cancel()
	<-stream.done
	north := []byte(`{"customer_id":"north","id":1}`)
	south := []byte(`{"customer_id":"south","id":2}`)
	_, err = manager.AcceptRecordBatch(t.Context(), stream, [][]byte{north, north, south})
	require.NoError(t, err)
	stream.buffer[0].arrived = time.Now().Add(-61 * time.Second)
	originalBacking := stream.buffer[:cap(stream.buffer)]
	require.NoError(t, manager.flushReady(t.Context(), stream, false))
	require.Len(t, stream.buffer, 1, "unselected partition remains admitted")
	require.Equal(t, south, stream.buffer[0].source)
	for _, record := range originalBacking[len(stream.buffer):] {
		require.Nil(t, record.source, "retired raw slots must not root acknowledged payloads")
		require.Nil(t, record.data)
		require.Nil(t, record.keys)
	}
	for _, object := range stream.pending[:cap(stream.pending)] {
		require.Nil(t, object, "acknowledged objects must not remain rooted by the pending capacity")
	}

	available.Store(false)
	require.Error(t, manager.flushBuffer(t.Context(), stream))
	require.Empty(t, stream.buffer)
	require.Len(t, stream.pending, 1, "failed destination data remains reachable")
	retained := stream.pending[0]
	require.Equal(t, south, retained.records[0].source)
	key, data := retained.key, bytes.Clone(retained.data)
	pendingBacking := stream.pending[:cap(stream.pending)]
	for _, record := range originalBacking {
		require.Nil(t, record.source)
		require.Nil(t, record.data)
		require.Nil(t, record.keys)
	}
	available.Store(true)
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	require.Equal(t, key, retained.key, "recovery keeps the retained object identity")
	require.Equal(t, data, retained.data)
	require.Empty(t, stream.pending)
	for _, object := range pendingBacking {
		require.Nil(t, object, "successful recovery releases the final pending reference")
	}
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
	require.Zero(t, snapshot.BufferedBytes)
}

func TestNativeDestinationARNLengthsRefuseConfigurationBeforeMutation(t *testing.T) {
	manager := newTestFirehoseManager(t)
	handler := NewHandler(manager)
	rolePrefix := "arn:aws:iam::000000000000:role/"
	bucketPrefix, bucketSuffix := "arn:aws-", ":s3:::bucket"
	for _, test := range []struct{ name, bucket, role string }{
		{"role-too-long", "arn:aws:s3:::bucket", rolePrefix + strings.Repeat("r", 513-len(rolePrefix))},
		{"bucket-arn-too-long", bucketPrefix + strings.Repeat("a", 2049-len(bucketPrefix)-len(bucketSuffix)) + bucketSuffix, "arn:aws:iam::000000000000:role/local"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, err := json.Marshal(map[string]interface{}{
				"DeliveryStreamName": test.name,
				"ExtendedS3DestinationConfiguration": map[string]string{
					"BucketARN": test.bucket, "RoleARN": test.role,
				},
			})
			require.NoError(t, err)
			result := httptest.NewRecorder()
			handler.ServeAction(result, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(request)), "CreateDeliveryStream")
			require.Equal(t, http.StatusBadRequest, result.Code)
			require.Contains(t, result.Body.String(), "InvalidArgumentException")
			require.Nil(t, manager.GetStream(test.name))
		})
	}
	role := rolePrefix + strings.Repeat("r", 512-len(rolePrefix))
	request, err := json.Marshal(map[string]interface{}{
		"DeliveryStreamName": "role-at-limit",
		"ExtendedS3DestinationConfiguration": map[string]string{
			"BucketARN": "arn:aws:s3:::bucket", "RoleARN": role,
		},
	})
	require.NoError(t, err)
	result := httptest.NewRecorder()
	handler.ServeAction(result, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(request)), "CreateDeliveryStream")
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
	snapshot, exists := manager.Snapshot("role-at-limit")
	require.True(t, exists)
	require.Equal(t, role, snapshot.Config.RoleARN)
}
