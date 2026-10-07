//go:build integration

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
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdkfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestNativeSNSFirehosePipelineGZIPPartitionsFiltersErrorsRecoveryAndDrain(t *testing.T) {
	fixture := newNativeS3Fixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// An owned transport fails destination delivery without touching RustFS or
	// its state. After recovery the exact retained objects reach real RustFS.
	target, err := url.Parse(fixture.endpoint)
	require.NoError(t, err)
	forward := httputil.NewSingleHostReverseProxy(target)
	var available atomic.Bool
	failingDestination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(failingDestination.Close)
	manager := NewFirehoseManager("us-east-1", "000000000000", failingDestination.URL, "test", "testtest123")
	require.NoError(t, manager.SetMetadataExtractor(NewGoJQMetadataExtractor()))
	t.Cleanup(func() { _ = manager.Shutdown() })
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	broker.SetFirehoseDelivery(manager)
	emulator := httptest.NewServer(server.New(server.Services{Firehose: NewHandler(manager), Messaging: messaging.NewHandler(broker)}))
	t.Cleanup(emulator.Close)
	sdkConfig, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "testtest123", "")))
	require.NoError(t, err)
	firehoseClient := sdkfirehose.NewFromConfig(sdkConfig, func(options *sdkfirehose.Options) { options.BaseEndpoint = aws.String(emulator.URL) })
	snsClient := sns.NewFromConfig(sdkConfig, func(options *sns.Options) { options.BaseEndpoint = aws.String(emulator.URL) })
	query := `(.Message? // . | if type=="string" then fromjson else . end) | {tenant:.customer_id,day:(.timestamp|strftime("%Y-%m-%d"))}`
	created, err := firehoseClient.CreateDeliveryStream(ctx, &sdkfirehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("sdk-pipeline"), DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut, ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{RoleARN: aws.String("arn:aws:iam::000000000000:role/firehose"), BucketARN: aws.String("arn:aws:s3:::" + fixture.bucket), Prefix: aws.String("tenant=!{partitionKeyFromQuery:tenant}/day=!{partitionKeyFromQuery:day}/!{timestamp:yyyy/MM/dd/HH/}"), ErrorOutputPrefix: aws.String("errors/!{firehose:error-output-type}/!{timestamp:yyyy/MM/dd/HH/}"), CompressionFormat: fhtypes.CompressionFormatGzip, BufferingHints: &fhtypes.BufferingHints{SizeInMBs: aws.Int32(1), IntervalInSeconds: aws.Int32(60)}, DynamicPartitioningConfiguration: &fhtypes.DynamicPartitioningConfiguration{Enabled: aws.Bool(true)}, ProcessingConfiguration: &fhtypes.ProcessingConfiguration{Enabled: aws.Bool(true), Processors: []fhtypes.Processor{{Type: fhtypes.ProcessorType("MetadataExtraction"), Parameters: []fhtypes.ProcessorParameter{{ParameterName: fhtypes.ProcessorParameterName("JsonParsingEngine"), ParameterValue: aws.String("JQ-1.6")}, {ParameterName: fhtypes.ProcessorParameterName("MetadataExtractionQuery"), ParameterValue: aws.String(query)}}}, {Type: fhtypes.ProcessorType("AppendDelimiterToRecord")}}}}})
	require.NoError(t, err)
	topic, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("native-pipeline")})
	require.NoError(t, err)
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("firehose"), Endpoint: created.DeliveryStreamARN, Attributes: map[string]string{"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns-firehose", "RawMessageDelivery": "true", "FilterPolicy": `{"kind":["accepted"]}`}})
	require.NoError(t, err)
	// Subscribe is idempotent per endpoint, so separate topics exercise both
	// raw and native notification-envelope deliveries to the same stream.
	second, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("native-pipeline-envelope")})
	require.NoError(t, err)
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{TopicArn: second.TopicArn, Protocol: aws.String("firehose"), Endpoint: created.DeliveryStreamARN, Attributes: map[string]string{"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns-firehose", "RawMessageDelivery": "false", "FilterPolicy": `{"kind":["accepted"]}`}})
	require.NoError(t, err)
	publish := func(topicARN *string, message, kind string) {
		_, err := snsClient.Publish(ctx, &sns.PublishInput{TopicArn: topicARN, Message: aws.String(message), MessageAttributes: map[string]snstypes.MessageAttributeValue{"kind": {DataType: aws.String("String"), StringValue: aws.String(kind)}}})
		require.NoError(t, err)
	}
	north := `{"customer_id":"north","timestamp":1577934245,"id":1}`
	south := `{"customer_id":"south","timestamp":1577934245,"id":2}`
	publish(topic.TopicArn, north, "accepted")
	publish(second.TopicArn, north, "accepted")
	publish(topic.TopicArn, south, "accepted")
	publish(second.TopicArn, south, "accepted")
	publish(topic.TopicArn, "filtered", "refused")
	publish(second.TopicArn, "filtered", "refused")
	publish(topic.TopicArn, "not-json", "accepted")
	publish(second.TopicArn, "not-json", "accepted")
	snapshot, _ := manager.Snapshot("sdk-pipeline")
	require.Equal(t, 6, snapshot.BufferedRecords)
	stream := manager.GetStream("sdk-pipeline")
	require.Error(t, manager.flushBuffer(ctx, stream))
	snapshot, _ = manager.Snapshot("sdk-pipeline")
	require.Equal(t, 6, snapshot.BufferedRecords)
	require.Equal(t, 3, snapshot.PendingObjects)
	require.NotEmpty(t, snapshot.LastDeliveryError)
	keys, err := fixture.list(ctx)
	require.NoError(t, err)
	require.Empty(t, keys)
	available.Store(true)
	require.NoError(t, manager.flushBuffer(ctx, stream))
	keys, err = fixture.list(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 3)
	nativeEnvelopes, rawRecords, errorRecords := 0, 0, 0
	for _, key := range keys {
		code, data, err := fixture.request(ctx, http.MethodGet, "/"+key, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code)
		if strings.HasPrefix(key, "errors/dynamic-partitioning-failed/") {
			for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
				var diagnostic map[string]interface{}
				require.NoError(t, json.Unmarshal(line, &diagnostic))
				raw, err := base64.StdEncoding.DecodeString(diagnostic["rawData"].(string))
				require.NoError(t, err)
				require.Contains(t, string(raw), "not-json")
				errorRecords++
			}
			continue
		}
		require.Contains(t, key, "/day=2020-01-02/")
		require.True(t, strings.HasSuffix(key, ".gz"))
		reader, err := gzip.NewReader(bytes.NewReader(data))
		require.NoError(t, err)
		content, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		for _, line := range bytes.Split(bytes.TrimSpace(content), []byte{'\n'}) {
			var record map[string]interface{}
			require.NoError(t, json.Unmarshal(line, &record))
			if record["Type"] == "Notification" {
				nativeEnvelopes++
				require.Equal(t, *second.TopicArn, record["TopicArn"])
				require.NoError(t, json.Unmarshal([]byte(record["Message"].(string)), &record))
			} else {
				rawRecords++
			}
			require.Contains(t, key, "tenant="+record["customer_id"].(string)+"/")
		}
	}
	require.Equal(t, 2, rawRecords)
	require.Equal(t, 2, nativeEnvelopes)
	require.Equal(t, 2, errorRecords)
	publish(topic.TopicArn, north, "accepted")
	emulator.Close()
	require.NoError(t, manager.ShutdownContext(ctx))
	after, err := fixture.list(ctx)
	require.NoError(t, err)
	require.Len(t, after, 4)
	require.NoError(t, manager.ShutdownContext(ctx))
	again, err := fixture.list(ctx)
	require.NoError(t, err)
	require.Equal(t, after, again)
}
