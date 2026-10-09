package firehose

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const maxRecordBytes = 1000 * 1024
const maxBatchBytes = 4 * 1024 * 1024
const maxBufferedBytes = 64 * 1024 * 1024

type bufferedRecord struct {
	source        []byte
	data          []byte
	originalBytes int
	arrived       time.Time
	group         string
	keys          map[string]string
	errorType     string
	lease         *devRecordLease
}
type deliveryObject struct {
	key           string
	data          []byte
	records       []bufferedRecord
	originalBytes int
	attempts      int
	firstFailure  time.Time
}

type DeliveryStream struct {
	// Legacy direct-store identifiers/configuration are immutable after creation.
	Name, ARN, BucketName, Prefix, ErrorPrefix, Status string
	BufferSizeMB                                       int
	BufferInterval                                     time.Duration
	config                                             StreamConfig
	metadata                                           RecordMetadataExtractor
	buildObject                                        objectBuilder
	created                                            time.Time
	buffer                                             []bufferedRecord
	pending                                            []*deliveryObject
	bufferedBytes                                      int
	// retainedRecords counts accepted buffer and pending records, including
	// preparation/retry intervals; only joined successful delivery decrements it.
	retainedRecords   int
	lastDeliveryError string
	mu                sync.Mutex
	flushSlot         chan struct{}
	stopping          bool
	cancel            context.CancelFunc
	flush             chan struct{}
	done              chan struct{}
}

type StreamSnapshot struct {
	Name, ARN, Status                              string
	Config                                         StreamConfig
	CreatedAt                                      time.Time
	BufferedRecords, BufferedBytes, PendingObjects int
	LastDeliveryError                              string
}

type FirehoseManager struct {
	streams                                                 map[string]*DeliveryStream
	mu                                                      sync.RWMutex
	region, accountID, s3Endpoint, s3AccessKey, s3SecretKey string
	metadata                                                MetadataExtractor
	buildObject                                             objectBuilder
	dev                                                     *devDelivery
	bufferLimit                                             int
	httpClient                                              *http.Client
	stopping                                                bool
	done                                                    chan struct{}
	shutdownErr                                             error
}

func NewFirehoseManager(region, accountID, s3Endpoint, s3AccessKey, s3SecretKey string) *FirehoseManager {
	if region == "" {
		region = "us-east-1"
	}
	if accountID == "" {
		accountID = "000000000000"
	}
	return &FirehoseManager{streams: make(map[string]*DeliveryStream), region: region, accountID: accountID, s3Endpoint: s3Endpoint, s3AccessKey: s3AccessKey, s3SecretKey: s3SecretKey, bufferLimit: maxBufferedBytes, buildObject: makeObject, httpClient: &http.Client{Timeout: 10 * time.Second}, done: make(chan struct{})}
}

// SetMetadataExtractor is a construction-time port, never a mutable per-request
// override of a stream's accepted native processor settings.
func (fm *FirehoseManager) SetMetadataExtractor(extractor MetadataExtractor) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.stopping || len(fm.streams) != 0 {
		return errors.New("metadata runtime must be configured before stream creation")
	}
	fm.metadata = extractor
	return nil
}

// CreateStream preserves the existing embedded-host provisioning entry point,
// including accelerated intervals. Native HTTP creation uses validated config.
func (fm *FirehoseManager) CreateStream(name, bucketName, prefix, errorPrefix string, bufferSizeMB, bufferIntervalSec int) (*DeliveryStream, error) {
	if bufferSizeMB <= 0 {
		bufferSizeMB = 1
	}
	if bufferIntervalSec <= 0 {
		bufferIntervalSec = 60
	}
	config := StreamConfig{Name: name, BucketARN: "arn:aws:s3:::" + bucketName, Prefix: prefix, ErrorOutputPrefix: errorPrefix, CompressionFormat: "UNCOMPRESSED", CustomTimeZone: "UTC", BufferingHints: BufferingHints{SizeInMBs: bufferSizeMB, IntervalInSeconds: bufferIntervalSec}}
	return fm.createStream(config, nil)
}

func (fm *FirehoseManager) CreateConfiguredStream(ctx context.Context, config StreamConfig) (*DeliveryStream, error) {
	fm.mu.RLock()
	extractor := fm.metadata
	fm.mu.RUnlock()
	config = cloneConfig(config)
	if err := validateConfig(config, extractor); err != nil {
		return nil, err
	}
	processor, err := prepareMetadataExtractor(ctx, extractor, config.metadataQuery())
	if err != nil {
		return nil, fmt.Errorf("invalid MetadataExtractionQuery: %w", err)
	}
	return fm.createStream(config, processor)
}

func (fm *FirehoseManager) createStream(config StreamConfig, metadata RecordMetadataExtractor) (*DeliveryStream, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.stopping {
		return nil, errors.New("firehose is stopping")
	}
	if existing := fm.streams[config.Name]; existing != nil {
		if !reflect.DeepEqual(existing.config, config) {
			return nil, errors.New("stream already exists with different configuration")
		}
		return existing, nil
	}
	config = cloneConfig(config)
	ctx, cancel := context.WithCancel(context.Background())
	_, bucket, _ := strings.Cut(config.BucketARN, ":::")
	ds := &DeliveryStream{Name: config.Name, ARN: fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/%s", fm.region, fm.accountID, config.Name), BucketName: bucket, Prefix: config.Prefix, ErrorPrefix: config.ErrorOutputPrefix, BufferSizeMB: config.BufferingHints.SizeInMBs, BufferInterval: time.Duration(config.BufferingHints.IntervalInSeconds) * time.Second, Status: "ACTIVE", config: config, metadata: metadata, buildObject: fm.buildObject, created: time.Now(), cancel: cancel, flush: make(chan struct{}, 1), flushSlot: make(chan struct{}, 1), done: make(chan struct{})}
	fm.streams[ds.Name] = ds
	go fm.flushLoop(ctx, ds)
	return ds, nil
}

func (fm *FirehoseManager) GetStream(name string) *DeliveryStream {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.streams[name]
}

func (fm *FirehoseManager) Snapshot(name string) (StreamSnapshot, bool) {
	fm.mu.RLock()
	ds := fm.streams[name]
	fm.mu.RUnlock()
	if ds == nil {
		return StreamSnapshot{}, false
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	status := ds.Status
	if ds.stopping {
		status = "DELETING"
	}
	return StreamSnapshot{Name: ds.Name, ARN: ds.ARN, Status: status, Config: cloneConfig(ds.config), CreatedAt: ds.created, BufferedRecords: ds.retainedRecords, BufferedBytes: ds.bufferedBytes, PendingObjects: len(ds.pending), LastDeliveryError: ds.lastDeliveryError}, true
}

// PutFirehoseRecord satisfies the consumer-owned SNS delivery port without a
// dependency on messaging. Only this manager's region/account/stream is valid.
func (fm *FirehoseManager) PutFirehoseRecord(ctx context.Context, arn string, data []byte) (string, error) {
	prefix := fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/", fm.region, fm.accountID)
	if !strings.HasPrefix(arn, prefix) || !streamNameRE.MatchString(strings.TrimPrefix(arn, prefix)) {
		return "", errors.New("Firehose endpoint must identify a local stream in this region and account")
	}
	ds := fm.GetStream(strings.TrimPrefix(arn, prefix))
	if ds == nil || ds.ARN != arn {
		return "", errors.New("Firehose stream does not exist")
	}
	results, err := fm.AcceptRecordBatch(ctx, ds, [][]byte{data})
	if err != nil {
		return "", err
	}
	if results[0].ErrorCode != "" {
		return "", errors.New(results[0].ErrorMessage)
	}
	return results[0].RecordID, nil
}

type RecordResult struct{ RecordID, ErrorCode, ErrorMessage string }

func validateRecords(records [][]byte) error {
	if len(records) < 1 || len(records) > 500 {
		return errors.New("Records must contain 1..500 records")
	}
	total := 0
	for _, record := range records {
		if len(record) > maxRecordBytes {
			return errors.New("Record.Data exceeds 1000 KiB")
		}
		total += len(record)
	}
	if total > maxBatchBytes {
		return errors.New("Records exceed 4 MiB")
	}
	return nil
}

func processingFailure(data []byte, arrived time.Time, code string, attempts int) []byte {
	message := "Unable to extract partition keys or evaluate the configured prefix"
	if strings.Contains(code, "DeliveryFailed") {
		message = "Destination delivery retry window was exhausted"
	}
	encoded, _ := json.Marshal(map[string]interface{}{"attemptsMade": attempts, "arrivalTimestamp": arrived.UnixMilli(), "errorCode": code, "errorMessage": message, "lastErrorTimestamp": time.Now().UnixMilli(), "rawData": base64.StdEncoding.EncodeToString(data)})
	return append(encoded, '\n')
}

func prepareRecord(ctx context.Context, config StreamConfig, extractor RecordMetadataExtractor, data []byte, arrived time.Time) bufferedRecord {
	record := bufferedRecord{data: bytes.Clone(data), originalBytes: len(data), arrived: arrived}
	record.source = record.data
	location, _ := time.LoadLocation(config.CustomTimeZone)
	if query := config.metadataQuery(); query != "" {
		keys, err := extractor.Extract(ctx, data)
		if err == nil {
			record.keys = make(map[string]string)
			for key, value := range keys {
				if strings.Contains(config.Prefix, "!{partitionKeyFromQuery:"+key+"}") {
					record.keys[key] = value
				}
			}
			_, err = renderPrefix(config.Prefix, arrived.In(location), keys, "", false, true, false)
		}
		if err != nil {
			record.errorType = "dynamic-partitioning-failed"
			record.data = processingFailure(data, arrived, "DynamicPartitioning.MetadataExtractionFailed", 1)
		}
	}
	prefix := config.Prefix
	if record.errorType != "" {
		prefix = config.ErrorOutputPrefix
	}
	// Random prefixes are object-level decisions, not partition identities.
	groupingPrefix := strings.ReplaceAll(prefix, "!{firehose:random-string}", "__random__")
	record.group, _ = renderPrefix(groupingPrefix, arrived.In(location), record.keys, record.errorType, record.errorType != "", config.DynamicPartitioningConfiguration.Enabled, false)
	if config.appendDelimiter() && record.errorType == "" {
		record.data = append(record.data, '\n')
	}
	return record
}

func (fm *FirehoseManager) AcceptRecordBatch(ctx context.Context, ds *DeliveryStream, records [][]byte) ([]RecordResult, error) {
	// Bound a whole batch, including repeated expensive record-dependent queries.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := validateRecords(records); err != nil {
		return nil, err
	}
	fm.mu.RLock()
	valid := !fm.stopping && ds != nil && fm.streams[ds.Name] == ds
	fm.mu.RUnlock()
	if !valid {
		return nil, errors.New("firehose stream is unavailable")
	}
	prepared := make([]bufferedRecord, len(records))
	arrived := time.Now()
	for index, data := range records {
		prepared[index] = prepareRecord(ctx, ds.config, ds.metadata, data, arrived)
	}
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if fm.stopping || fm.streams[ds.Name] != ds {
		return nil, errors.New("firehose stream is unavailable")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	results := make([]RecordResult, len(records))
	for index, record := range prepared {
		if ds.stopping || ctx.Err() != nil || ds.retainedRecords >= 100000 || ds.bufferedBytes+record.originalBytes > fm.bufferLimit {
			results[index] = RecordResult{ErrorCode: "ServiceUnavailableException", ErrorMessage: "Firehose stream is stopping or its retained delivery buffer is full"}
			continue
		}
		recordID := uuid.NewString()
		if fm.dev != nil {
			lease, err := fm.dev.beginRecord(recordID)
			if err != nil {
				results[index] = RecordResult{ErrorCode: "ServiceUnavailableException", ErrorMessage: "Firehose delivery ownership is unavailable"}
				continue
			}
			record.lease = lease
		}
		ds.buffer = append(ds.buffer, record)
		ds.bufferedBytes += record.originalBytes
		ds.retainedRecords++
		results[index].RecordID = recordID
	}
	select {
	case ds.flush <- struct{}{}:
	default:
	}
	fm.devWake()
	return results, nil
}

func (fm *FirehoseManager) PutRecord(ds *DeliveryStream, data []byte) (string, error) {
	ids, err := fm.PutRecordBatch(ds, [][]byte{data})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}
func (fm *FirehoseManager) PutRecordBatch(ds *DeliveryStream, records [][]byte) ([]string, error) {
	results, err := fm.AcceptRecordBatch(context.Background(), ds, records)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(results))
	var failures []error
	for index, result := range results {
		ids[index] = result.RecordID
		if result.ErrorCode != "" {
			failures = append(failures, errors.New(result.ErrorMessage))
		}
	}
	return ids, errors.Join(failures...)
}

func (fm *FirehoseManager) flushLoop(ctx context.Context, ds *DeliveryStream) {
	defer close(ds.done)
	tick := min(time.Duration(ds.config.BufferingHints.IntervalInSeconds)*time.Second, time.Second)
	if tick <= 0 {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-ds.flush:
		}
		if ctx.Err() != nil {
			return
		}
		if err := fm.flushReady(ctx, ds, false); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Str("stream", ds.Name).Msg("Firehose delivery failed; records retained")
		}
	}
}

func (fm *FirehoseManager) flushBuffer(ctx context.Context, ds *DeliveryStream) error {
	return fm.flushReady(ctx, ds, true)
}

func (fm *FirehoseManager) flushReady(ctx context.Context, ds *DeliveryStream, force bool) error {
	ctx, finishAttempt, err := fm.devAttempt(ctx)
	if err != nil {
		return err
	}
	var acknowledged []*devRecordLease
	acquired := false
	defer func() {
		if acquired {
			<-ds.flushSlot
		}
		// Completions cannot reenter a stream/entity lock or report safe ownership
		// until the serialized attempt and response-body cleanup have joined.
		for _, lease := range acknowledged {
			lease.finish(nil)
		}
		finishAttempt()
	}()
	select {
	case ds.flushSlot <- struct{}{}:
		acquired = true
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ds.mu.Lock()
	if owner, ok := ctx.Value(devForceContextKey{}).(*devDelivery); ok {
		owner.mu.Lock()
		forcing := owner.forcing && !owner.aborted
		owner.mu.Unlock()
		if !forcing {
			ds.mu.Unlock()
			return nil
		}
	}
	// Accepted records remain in the quota-counted buffer while the serialized
	// flush owns an immutable snapshot. Appends can proceed during construction.
	buffer := append([]bufferedRecord(nil), ds.buffer...)
	ds.mu.Unlock()
	objects, selected, err := buildBufferedObjects(ctx, ds, buffer, force)
	if err != nil {
		return err
	}
	ds.mu.Lock()
	if err := ctx.Err(); err != nil {
		ds.mu.Unlock()
		return err
	}
	ds.pending = append(ds.pending, objects...)
	commitBufferedObjects(ds, len(buffer), selected)
	pending := append([]*deliveryObject(nil), ds.pending...)
	ds.mu.Unlock()
	var failures []error
	for _, object := range pending {
		// Dynamic partition retry duration is a delivery window. Once exhausted,
		// records move to the configured error prefix; that object itself stays
		// retained/retryable while the destination remains unavailable.
		ds.mu.Lock()
		retrySeconds := 300
		if options := ds.config.DynamicPartitioningConfiguration.RetryOptions; options != nil {
			retrySeconds = options.DurationInSeconds
		}
		if ds.config.DynamicPartitioningConfiguration.Enabled && !object.firstFailure.IsZero() && object.records[0].errorType == "" && time.Since(object.firstFailure) >= time.Duration(retrySeconds)*time.Second {
			// The flush slot exclusively owns replacement; readers see its old
			// immutable records until the complete replacement is published.
			records, attempts := object.records, object.attempts
			ds.mu.Unlock()
			failed := make([]bufferedRecord, len(records))
			for index, record := range records {
				if err := ctx.Err(); err != nil {
					return err
				}
				record.errorType = "dynamic-partitioning-failed"
				record.data = processingFailure(record.source, record.arrived, "DynamicPartitioning.DeliveryFailed", attempts)
				failed[index] = record
			}
			replacement, prepareErr := ds.buildObject(ctx, ds.config, failed)
			if prepareErr != nil {
				return prepareErr
			}
			ds.mu.Lock()
			if err := ctx.Err(); err != nil {
				ds.mu.Unlock()
				return err
			}
			object.key, object.data, object.records = replacement.key, replacement.data, replacement.records
		}
		ds.mu.Unlock()
		_, bucket, _ := strings.Cut(ds.config.BucketARN, ":::")
		err := fm.putS3Object(ctx, bucket, object.key, object.data)
		ds.mu.Lock()
		object.attempts++
		if err == nil {
			for _, record := range object.records {
				if record.lease != nil {
					acknowledged = append(acknowledged, record.lease)
				}
			}
			for index, current := range ds.pending {
				if current == object {
					ds.pending = slices.Delete(ds.pending, index, index+1)
					break
				}
			}
			ds.bufferedBytes -= object.originalBytes
			ds.retainedRecords -= len(object.records)
			if len(ds.pending) == 0 {
				ds.lastDeliveryError = ""
			}
		} else {
			if object.firstFailure.IsZero() {
				object.firstFailure = time.Now()
			}
			ds.lastDeliveryError = err.Error()
			failures = append(failures, fmt.Errorf("Firehose retained %d records after failed delivery: %w", len(object.records), err))
		}
		ds.mu.Unlock()
	}
	return errors.Join(failures...)
}

func (fm *FirehoseManager) putS3Object(ctx context.Context, bucket, key string, data []byte) error {
	endpoint, err := url.Parse(fm.s3Endpoint)
	if err != nil {
		return err
	}
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + bucket + "/" + key
	endpoint.RawPath = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create Firehose delivery request: %w", err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	hash := sha256.Sum256(data)
	payloadHash := hex.EncodeToString(hash[:])
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: fm.s3AccessKey, SecretAccessKey: fm.s3SecretKey}, request, payloadHash, "s3", fm.region, time.Now(), func(options *v4.SignerOptions) { options.DisableURIPathEscaping = true }); err != nil {
		return fmt.Errorf("sign Firehose delivery: %w", err)
	}
	response, err := fm.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("send Firehose delivery: %w", err)
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	closeErr := response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.Join(fmt.Errorf("Firehose destination returned status %d", response.StatusCode), readErr, closeErr)
	}
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("complete Firehose destination response: %w", err)
	}
	return nil
}

func (fm *FirehoseManager) DeleteStream(ctx context.Context, name string) error {
	fm.mu.Lock()
	ds := fm.streams[name]
	if ds == nil {
		fm.mu.Unlock()
		return nil
	}
	if fm.stopping {
		fm.mu.Unlock()
		return errors.New("firehose is stopping")
	}
	ds.mu.Lock()
	ds.stopping = true
	ds.mu.Unlock()
	fm.mu.Unlock()
	ds.cancel()
	select {
	case <-ds.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := fm.flushBuffer(ctx, ds); err != nil {
		return err
	}
	fm.mu.Lock()
	if fm.streams[name] == ds {
		delete(fm.streams, name)
	}
	fm.mu.Unlock()
	return nil
}
func (fm *FirehoseManager) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return fm.ShutdownContext(ctx)
}
func (fm *FirehoseManager) ShutdownContext(ctx context.Context) error {
	fm.mu.Lock()
	if fm.stopping {
		fm.mu.Unlock()
		select {
		case <-fm.done:
			return fm.shutdownErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	fm.stopping = true
	streams := make([]*DeliveryStream, 0, len(fm.streams))
	for _, ds := range fm.streams {
		streams = append(streams, ds)
		ds.cancel()
	}
	fm.mu.Unlock()
	var failures []error
	if err := fm.devStopForce(ctx); err != nil {
		failures = append(failures, err)
	}
	for _, ds := range streams {
		select {
		case <-ds.done:
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
		}
		if ctx.Err() == nil {
			if err := fm.flushBuffer(ctx, ds); err != nil {
				failures = append(failures, err)
			}
		}
	}
	result := errors.Join(failures...)
	if result == nil && fm.dev != nil {
		fm.httpClient.CloseIdleConnections()
	}
	fm.mu.Lock()
	fm.shutdownErr = result
	close(fm.done)
	fm.mu.Unlock()
	return result
}
