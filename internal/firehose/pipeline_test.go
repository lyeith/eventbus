package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
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

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/require"
)

func partitionConfig(name string) StreamConfig {
	return StreamConfig{Name: name, BucketARN: "arn:aws:s3:::bucket", RoleARN: "arn:aws:iam::000000000000:role/firehose", Prefix: "tenant=!{partitionKeyFromQuery:tenant}/!{timestamp:yyyy/MM/dd/HH/}", ErrorOutputPrefix: "errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH/}", CompressionFormat: "GZIP", CustomTimeZone: "UTC", BufferingHints: BufferingHints{SizeInMBs: 1, IntervalInSeconds: 60}, DynamicPartitioningConfiguration: DynamicPartitioningConfiguration{Enabled: true}, ProcessingConfiguration: ProcessingConfiguration{Enabled: true, Processors: []Processor{{Type: "MetadataExtraction", Parameters: []ProcessorParameter{{ParameterName: "JsonParsingEngine", ParameterValue: "JQ-1.6"}, {ParameterName: "MetadataExtractionQuery", ParameterValue: "{tenant: .customer_id}"}}}, {Type: "AppendDelimiterToRecord"}}}}
}

func TestGoJQMetadataNativeQueryProfileAndExplicitLimits(t *testing.T) {
	extractor := NewGoJQMetadataExtractor()
	query := `(.payload // .) | {tenant: (.customer_id | ascii_downcase), year: (.timestamp | strftime("%Y")), nested: (.values | map(.n) | add | tostring)}`
	require.NoError(t, extractor.Validate(t.Context(), query))
	keys, err := extractor.Extract(t.Context(), query, []byte(`{"payload":{"customer_id":"NORTH","timestamp":1577934245,"values":[{"n":2},{"n":3}]}}`))
	require.NoError(t, err)
	require.Equal(t, map[string]string{"tenant": "north", "year": "2020", "nested": "5"}, keys)
	for _, query := range []string{`{broken:`, `unknown_function(1)`, `import "host" as host; {key:host::read}`, `input`} {
		require.Error(t, extractor.Validate(t.Context(), query))
	}
	for _, query := range []string{`{key:null}`, `{key:[1]}`, `{key:{a:1}}`, `{key:"first"},{key:"second"}`, `empty`, `.customer_id | match("(?<=a)b") | {key:.string}`} {
		_, err := extractor.Extract(t.Context(), query, []byte(`{"customer_id":"ab"}`))
		require.Error(t, err, query)
	}
	for _, record := range []string{`not-json`, `{} {}`} {
		_, err := extractor.Extract(t.Context(), `{key:"x"}`, []byte(record))
		require.Error(t, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	_, err = extractor.Extract(ctx, `def f: f; f`, []byte(`{}`))
	require.Error(t, err)
	require.Error(t, ctx.Err())
	// Go jq's exact integer arithmetic is a documented compatibility difference
	// from jq 1.6's IEEE floating-point arithmetic, not an AWS equivalence claim.
	keys, err = extractor.Extract(t.Context(), `{key:(9007199254740993 + 1 | tostring)}`, []byte(`{}`))
	require.NoError(t, err)
	require.Equal(t, "9007199254740994", keys["key"])
}

func TestPartitionBuffersGZIPAndProcessingErrorPreserveOriginalRecord(t *testing.T) {
	sink := s3MockHandler()
	server := httptest.NewServer(sink)
	t.Cleanup(server.Close)
	manager := NewFirehoseManager("us-east-1", "000000000000", server.URL, "test", "test")
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	config := partitionConfig("partitioned")
	stream, err := manager.CreateConfiguredStream(t.Context(), config)
	require.NoError(t, err)
	records := [][]byte{[]byte(`{"customer_id":"north","id":1}`), []byte(`{"customer_id":"south","id":2}`), []byte(`{"customer_id":"north","id":3}`), []byte(`not-json`)}
	_, err = manager.AcceptRecordBatch(t.Context(), stream, records)
	require.NoError(t, err)
	require.NoError(t, manager.flushReady(t.Context(), stream, false))
	sink.mu.Lock()
	require.Empty(t, sink.objects, "each partition is below its own buffering threshold")
	sink.mu.Unlock()
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	sink.mu.Lock()
	defer sink.mu.Unlock()
	require.Len(t, sink.objects, 3)
	seen := map[string][]string{}
	for key, data := range sink.objects {
		if strings.Contains(key, "/errors/dynamic-partitioning-failed/") {
			var diagnostic map[string]interface{}
			require.NoError(t, json.Unmarshal(bytes.TrimSpace(data), &diagnostic))
			require.Equal(t, base64.StdEncoding.EncodeToString(records[3]), diagnostic["rawData"])
			require.Equal(t, "DynamicPartitioning.MetadataExtractionFailed", diagnostic["errorCode"])
			continue
		}
		require.True(t, strings.HasSuffix(key, ".gz"))
		compressed, err := gzip.NewReader(bytes.NewReader(data))
		require.NoError(t, err)
		body, err := io.ReadAll(compressed)
		require.NoError(t, err)
		require.NoError(t, compressed.Close())
		tenant := "north"
		if strings.Contains(key, "tenant=south/") {
			tenant = "south"
		}
		seen[tenant] = strings.Split(strings.TrimSpace(string(body)), "\n")
	}
	require.Equal(t, []string{string(records[0]), string(records[2])}, seen["north"])
	require.Equal(t, []string{string(records[1])}, seen["south"])
	snapshot, ok := manager.Snapshot(stream.Name)
	require.True(t, ok)
	require.Zero(t, snapshot.BufferedRecords)
	require.Zero(t, snapshot.BufferedBytes)
	config.ProcessingConfiguration.Processors[0].Parameters[1].ParameterValue = "changed"
	require.Equal(t, "{tenant: .customer_id}", snapshot.Config.metadataQuery(), "stream owns deep config")
	snapshot.Config.ProcessingConfiguration.Processors[0].Parameters[1].ParameterValue = "changed again"
	next, _ := manager.Snapshot(stream.Name)
	require.Equal(t, "{tenant: .customer_id}", next.Config.metadataQuery(), "readback cannot mutate stream config")
}

func TestFirehoseRetainsStableObjectKeyAfterAmbiguousS3Failure(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var keys []string
	var bodies [][]byte
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		mu.Lock()
		keys = append(keys, r.URL.Path)
		bodies = append(bodies, data)
		mu.Unlock()
		if calls.Add(1) == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(sink.Close)
	manager := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	stream, err := manager.CreateStream("retry", "bucket", "with space/", "", 100, 3600)
	require.NoError(t, err)
	_, err = manager.PutRecord(stream, []byte("exact-original"))
	require.NoError(t, err)
	require.Error(t, manager.flushBuffer(t.Context(), stream))
	snapshot, _ := manager.Snapshot(stream.Name)
	require.Equal(t, 1, snapshot.BufferedRecords)
	require.Equal(t, 1, snapshot.PendingObjects)
	require.NotEmpty(t, snapshot.LastDeliveryError)
	require.NoError(t, manager.flushBuffer(t.Context(), stream))
	snapshot, _ = manager.Snapshot(stream.Name)
	require.Zero(t, snapshot.BufferedRecords)
	require.Empty(t, snapshot.LastDeliveryError)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, keys, 2)
	require.Equal(t, keys[0], keys[1])
	require.Equal(t, bodies[0], bodies[1])
	require.Equal(t, []byte("exact-original"), bodies[1])
	require.Contains(t, keys[0], "with space/")
}

func TestFirehoseMalformedBatchIsAtomicAndAdmissionFailuresStayOrdered(t *testing.T) {
	manager := newTestFirehoseManager(t)
	stream, err := manager.CreateStream("atomic", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	handler := NewHandler(manager)
	for _, bad := range []string{`{"Data":"%%%"}`, `null`, `123`, `{"Data":12}`} {
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"DeliveryStreamName":"atomic","Records":[{"Data":"YQ=="},`+bad+`]}`))
		response := httptest.NewRecorder()
		handler.ServeAction(response, request, "PutRecordBatch")
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		snapshot, _ := manager.Snapshot(stream.Name)
		require.Zero(t, snapshot.BufferedRecords)
	}
	manager.bufferLimit = 2
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"DeliveryStreamName":"atomic","Records":[{"Data":"YQ=="},{"Data":"YmI="},{"Data":"Yw=="}]}`))
	response := httptest.NewRecorder()
	handler.ServeAction(response, request, "PutRecordBatch")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var output struct {
		FailedPutCount   int
		RequestResponses []struct{ RecordId, ErrorCode, ErrorMessage string }
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &output))
	require.Equal(t, 1, output.FailedPutCount)
	require.Len(t, output.RequestResponses, 3)
	require.NotEmpty(t, output.RequestResponses[0].RecordId)
	require.Equal(t, "ServiceUnavailableException", output.RequestResponses[1].ErrorCode)
	require.Empty(t, output.RequestResponses[1].RecordId)
	require.NotEmpty(t, output.RequestResponses[2].RecordId)
}

func TestNativeFirehoseBatchBudgetAndDestinationReadback(t *testing.T) {
	_, client, manager := setupTestServerWithFirehose(t)
	input := &sdkfirehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("readback"), DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut, ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{RoleARN: aws.String("arn:aws:iam::000000000000:role/test"), BucketARN: aws.String("arn:aws:s3:::bucket"), CompressionFormat: fhtypes.CompressionFormatGzip, BufferingHints: &fhtypes.BufferingHints{SizeInMBs: aws.Int32(5), IntervalInSeconds: aws.Int32(60)}}}
	_, err := client.CreateDeliveryStream(t.Context(), input)
	require.NoError(t, err)
	description, err := client.DescribeDeliveryStream(t.Context(), &sdkfirehose.DescribeDeliveryStreamInput{DeliveryStreamName: input.DeliveryStreamName})
	require.NoError(t, err)
	require.Len(t, description.DeliveryStreamDescription.Destinations, 1)
	destination := description.DeliveryStreamDescription.Destinations[0].ExtendedS3DestinationDescription
	require.Equal(t, "arn:aws:s3:::bucket", *destination.BucketARN)
	require.Equal(t, fhtypes.CompressionFormatGzip, destination.CompressionFormat)
	require.Equal(t, int32(60), *destination.BufferingHints.IntervalInSeconds)
	result, err := client.PutRecordBatch(t.Context(), &sdkfirehose.PutRecordBatchInput{DeliveryStreamName: input.DeliveryStreamName, Records: []fhtypes.Record{{Data: bytes.Repeat([]byte("a"), maxRecordBytes)}, {Data: bytes.Repeat([]byte("b"), maxRecordBytes)}}})
	require.NoError(t, err)
	require.Len(t, result.RequestResponses, 2)
	require.Zero(t, *result.FailedPutCount)
	snapshot, _ := manager.Snapshot("readback")
	require.Equal(t, 2, snapshot.BufferedRecords)
	_, err = client.PutRecordBatch(t.Context(), &sdkfirehose.PutRecordBatchInput{DeliveryStreamName: input.DeliveryStreamName, Records: []fhtypes.Record{{Data: make([]byte, maxRecordBytes+1)}}})
	require.Error(t, err)
	next, _ := manager.Snapshot("readback")
	require.Equal(t, snapshot.BufferedRecords, next.BufferedRecords)
}

func TestFirehoseRejectsUnsupportedConfigurationBeforeMutation(t *testing.T) {
	manager := newTestFirehoseManager(t)
	handler := NewHandler(manager)
	for _, setting := range []string{`"CompressionFormat":"ZIP"`, `"ProcessingConfiguration":{"Enabled":true,"Processors":[{"Type":"Lambda","Parameters":[]}]}`, `"DataFormatConversionConfiguration":{"Enabled":true}`, `"DynamicPartitioningConfiguration":{"Enabled":true}`, `"ProcessingConfiguration":{"Enabled":true,"Extra":true}`, `"BufferingHints":{"SizeInMBs":1.5,"IntervalInSeconds":60}`} {
		body := `{"DeliveryStreamName":"refused","ExtendedS3DestinationConfiguration":{"RoleARN":"arn:aws:iam::000000000000:role/test","BucketARN":"arn:aws:s3:::bucket",` + setting + `}}`
		response := httptest.NewRecorder()
		handler.ServeAction(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)), "CreateDeliveryStream")
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		require.Nil(t, manager.GetStream("refused"))
	}
}
