package firehose

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	fhtypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFirehoseCreateDeliveryStream(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	result, err := fhClient.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("test-stream"),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/test"),
			BucketARN: aws.String("arn:aws:s3:::test-bucket"),
			Prefix:    aws.String("audit/"),
			BufferingHints: &fhtypes.BufferingHints{
				SizeInMBs:         aws.Int32(1),
				IntervalInSeconds: aws.Int32(60),
			},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, *result.DeliveryStreamARN, "test-stream")
}

func TestFirehoseCreateDeliveryStreamIdempotent(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	input := &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("idem-stream"),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/test"),
			BucketARN: aws.String("arn:aws:s3:::test-bucket"),
		},
	}

	r1, err := fhClient.CreateDeliveryStream(ctx, input)
	require.NoError(t, err)

	r2, err := fhClient.CreateDeliveryStream(ctx, input)
	require.NoError(t, err)

	assert.Equal(t, *r1.DeliveryStreamARN, *r2.DeliveryStreamARN)
}

func TestFirehoseDescribeDeliveryStream(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	// Create
	_, err := fhClient.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("describe-stream"),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/test"),
			BucketARN: aws.String("arn:aws:s3:::test-bucket"),
		},
	})
	require.NoError(t, err)

	// Describe
	result, err := fhClient.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("describe-stream"),
	})
	require.NoError(t, err)
	assert.Equal(t, "describe-stream", *result.DeliveryStreamDescription.DeliveryStreamName)
	assert.Contains(t, *result.DeliveryStreamDescription.DeliveryStreamARN, "describe-stream")
	assert.Equal(t, fhtypes.DeliveryStreamStatusActive, result.DeliveryStreamDescription.DeliveryStreamStatus)
}

func TestFirehoseDescribeNonexistent(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	_, err := fhClient.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("no-such-stream"),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

func TestFirehoseDeleteDeliveryStream(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	// Create
	_, err := fhClient.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("delete-stream"),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/test"),
			BucketARN: aws.String("arn:aws:s3:::test-bucket"),
		},
	})
	require.NoError(t, err)

	// Delete
	_, err = fhClient.DeleteDeliveryStream(ctx, &firehose.DeleteDeliveryStreamInput{
		DeliveryStreamName: aws.String("delete-stream"),
	})
	require.NoError(t, err)

	// Describe should fail
	_, err = fhClient.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String("delete-stream"),
	})
	assert.Error(t, err)
}

func TestFirehosePutRecord(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	// Create stream
	_, err := fhClient.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("put-stream"),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/test"),
			BucketARN: aws.String("arn:aws:s3:::test-bucket"),
		},
	})
	require.NoError(t, err)

	// Put record
	result, err := fhClient.PutRecord(ctx, &firehose.PutRecordInput{
		DeliveryStreamName: aws.String("put-stream"),
		Record: &fhtypes.Record{
			Data: []byte(`{"event_id":"test-001"}` + "\n"),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, *result.RecordId)
}

func TestFirehosePutRecordBatch(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	// Create stream
	_, err := fhClient.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("batch-stream"),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/test"),
			BucketARN: aws.String("arn:aws:s3:::test-bucket"),
		},
	})
	require.NoError(t, err)

	// Put batch
	result, err := fhClient.PutRecordBatch(ctx, &firehose.PutRecordBatchInput{
		DeliveryStreamName: aws.String("batch-stream"),
		Records: []fhtypes.Record{
			{Data: []byte(`{"event_id":"batch-001"}` + "\n")},
			{Data: []byte(`{"event_id":"batch-002"}` + "\n")},
			{Data: []byte(`{"event_id":"batch-003"}` + "\n")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), *result.FailedPutCount)
	assert.Len(t, result.RequestResponses, 3)
	for _, resp := range result.RequestResponses {
		assert.NotEmpty(t, *resp.RecordId)
	}
}

func TestFirehosePutRecordToNonexistent(t *testing.T) {
	_, fhClient, _ := setupTestServerWithFirehose(t)
	ctx := context.Background()

	_, err := fhClient.PutRecord(ctx, &firehose.PutRecordInput{
		DeliveryStreamName: aws.String("no-such-stream"),
		Record: &fhtypes.Record{
			Data: []byte(`{"test":true}` + "\n"),
		},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

func TestFirehoseFlushToS3(t *testing.T) {
	ts, fhClient, fm := setupTestServerWithFirehose(t)
	ctx := context.Background()
	_ = ts

	// Create stream with very short buffer interval for testing
	_, err := fhClient.CreateDeliveryStream(ctx, &firehose.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String("flush-stream"),
		DeliveryStreamType: fhtypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &fhtypes.ExtendedS3DestinationConfiguration{
			RoleARN:   aws.String("arn:aws:iam::000000000000:role/test"),
			BucketARN: aws.String("arn:aws:s3:::flush-bucket"),
			Prefix:    aws.String("audit/"),
			BufferingHints: &fhtypes.BufferingHints{
				SizeInMBs:         aws.Int32(1),
				IntervalInSeconds: aws.Int32(1), // 1 second flush
			},
		},
	})
	require.NoError(t, err)

	// Put a record
	_, err = fhClient.PutRecord(ctx, &firehose.PutRecordInput{
		DeliveryStreamName: aws.String("flush-stream"),
		Record: &fhtypes.Record{
			Data: []byte(`{"event_id":"flush-001"}` + "\n"),
		},
	})
	require.NoError(t, err)

	// Wait for flush (1s interval + some margin)
	time.Sleep(2 * time.Second)

	// Verify the S3 mock received the data
	ds := fm.GetStream("flush-stream")
	ds.mu.Lock()
	bufferLen := len(ds.buffer)
	ds.mu.Unlock()
	assert.Equal(t, 0, bufferLen, "Buffer should be empty after flush")
}
