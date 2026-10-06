package firehose

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type DeliveryStream struct {
	Name           string
	ARN            string
	BucketName     string
	Prefix         string
	ErrorPrefix    string
	BufferSizeMB   int
	BufferInterval time.Duration
	Status         string
	buffer         [][]byte
	mu             sync.Mutex
	flushSlot      chan struct{}
	stopping       bool
	cancel         context.CancelFunc
	flush          chan struct{}
	done           chan struct{}
}

type FirehoseManager struct {
	streams     map[string]*DeliveryStream
	mu          sync.RWMutex
	region      string
	accountID   string
	s3Endpoint  string
	s3AccessKey string
	s3SecretKey string
	stopping    bool
	done        chan struct{}
	shutdownErr error
}

func NewFirehoseManager(region, accountID, s3Endpoint, s3AccessKey, s3SecretKey string) *FirehoseManager {
	return &FirehoseManager{
		streams: make(map[string]*DeliveryStream), region: region, accountID: accountID,
		s3Endpoint: s3Endpoint, s3AccessKey: s3AccessKey, s3SecretKey: s3SecretKey,
		done: make(chan struct{}),
	}
}

func (fm *FirehoseManager) CreateStream(name, bucketName, prefix, errorPrefix string, bufferSizeMB, bufferIntervalSec int) (*DeliveryStream, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.stopping {
		return nil, errors.New("firehose is stopping")
	}
	if existing, ok := fm.streams[name]; ok {
		return existing, nil
	}
	if bufferSizeMB <= 0 {
		bufferSizeMB = 1
	}
	if bufferIntervalSec <= 0 {
		bufferIntervalSec = 60
	}
	ctx, cancel := context.WithCancel(context.Background())
	ds := &DeliveryStream{
		Name: name, ARN: fmt.Sprintf("arn:aws:firehose:%s:%s:deliverystream/%s", fm.region, fm.accountID, name),
		BucketName: bucketName, Prefix: prefix, ErrorPrefix: errorPrefix,
		BufferSizeMB: bufferSizeMB, BufferInterval: time.Duration(bufferIntervalSec) * time.Second,
		Status: "ACTIVE", cancel: cancel, flush: make(chan struct{}, 1),
		flushSlot: make(chan struct{}, 1), done: make(chan struct{}),
	}
	fm.streams[name] = ds
	go fm.flushLoop(ctx, ds)
	return ds, nil
}

func (fm *FirehoseManager) GetStream(name string) *DeliveryStream {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.streams[name]
}

// Delete joins the stream's background I/O and reports failed final delivery.
// A failure leaves the buffered stream reachable for explicit retry/shutdown.
func (fm *FirehoseManager) DeleteStream(ctx context.Context, name string) error {
	fm.mu.Lock()
	ds, ok := fm.streams[name]
	if !ok {
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

func (fm *FirehoseManager) PutRecord(ds *DeliveryStream, data []byte) (string, error) {
	ids, err := fm.PutRecordBatch(ds, [][]byte{data})
	if err != nil {
		return "", err
	}
	return ids[0], nil
}

func (fm *FirehoseManager) PutRecordBatch(ds *DeliveryStream, records [][]byte) ([]string, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if fm.stopping || ds == nil || fm.streams[ds.Name] != ds {
		return nil, errors.New("firehose stream is unavailable")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.stopping {
		return nil, errors.New("firehose stream is stopping")
	}
	ids := make([]string, len(records))
	for i, data := range records {
		ds.buffer = append(ds.buffer, bytes.Clone(data))
		ids[i] = uuid.NewString()
	}
	if fm.bufferExceedsSize(ds) {
		select {
		case ds.flush <- struct{}{}:
		default:
		}
	}
	return ids, nil
}

func (fm *FirehoseManager) bufferExceedsSize(ds *DeliveryStream) bool {
	total := 0
	for _, record := range ds.buffer {
		total += len(record)
	}
	return total >= ds.BufferSizeMB*1024*1024
}

// One goroutine owns each stream's scheduled/size-triggered delivery. Writes do
// not launch untracked flush goroutines; stopping cancels and joins this owner.
func (fm *FirehoseManager) flushLoop(ctx context.Context, ds *DeliveryStream) {
	defer close(ds.done)
	ticker := time.NewTicker(ds.BufferInterval)
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
		if err := fm.flushBuffer(ctx, ds); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Str("stream", ds.Name).Msg("Firehose delivery failed; records retained")
		}
	}
}

func (fm *FirehoseManager) flushBuffer(ctx context.Context, ds *DeliveryStream) error {
	// Final Delete requests can overlap. A context deadline must also interrupt
	// waiting for the other request's delivery, not just the eventual HTTP PUT.
	select {
	case ds.flushSlot <- struct{}{}:
		defer func() { <-ds.flushSlot }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ds.mu.Lock()
	records := ds.buffer
	ds.buffer = nil
	ds.mu.Unlock()
	if len(records) == 0 {
		return nil
	}
	var body bytes.Buffer
	for _, record := range records {
		body.Write(record)
	}
	key := fmt.Sprintf("%s%s-%s.json", ds.Prefix, time.Now().UTC().Format("2006/01/02/15"), uuid.NewString())
	if err := fm.putS3Object(ctx, ds.BucketName, key, body.Bytes()); err != nil {
		ds.mu.Lock()
		ds.buffer = append(records, ds.buffer...)
		ds.mu.Unlock()
		return fmt.Errorf("firehose delivery failed (%d buffered records): %w", len(records), err)
	}
	return nil
}

func (fm *FirehoseManager) putS3Object(ctx context.Context, bucket, key string, data []byte) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/%s/%s", fm.s3Endpoint, bucket, key), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create Firehose delivery request: %w", err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	hash := sha256.Sum256(data)
	payloadHash := hex.EncodeToString(hash[:])
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: fm.s3AccessKey, SecretAccessKey: fm.s3SecretKey}, request, payloadHash, "s3", fm.region, time.Now()); err != nil {
		return fmt.Errorf("sign Firehose delivery: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("send Firehose delivery: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return fmt.Errorf("firehose destination returned status %d", response.StatusCode)
	}
	return nil
}

// Shutdown preserves the old convenience entry point, now bounded and observable.
func (fm *FirehoseManager) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return fm.ShutdownContext(ctx)
}

// ShutdownContext requires stopped request/consumer admission. It bars new writes,
// cancels and joins every stream owner, then performs a bounded final flush. Errors
// are terminal and retained; a failed/unknown delivery is never reported as success.
func (fm *FirehoseManager) ShutdownContext(ctx context.Context) error {
	fm.mu.Lock()
	if fm.stopping {
		fm.mu.Unlock()
		select {
		case <-fm.done:
			return fm.shutdownErr
		default:
		}
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
	err := fm.finishShutdown(ctx, streams)
	fm.mu.Lock()
	fm.shutdownErr = err
	close(fm.done)
	fm.mu.Unlock()
	return err
}

func (fm *FirehoseManager) finishShutdown(ctx context.Context, streams []*DeliveryStream) error {
	for _, ds := range streams {
		select {
		case <-ds.done:
		case <-ctx.Done():
			return fmt.Errorf("join Firehose stream: %w", ctx.Err())
		}
	}
	var failures []error
	for _, ds := range streams {
		if err := fm.flushBuffer(ctx, ds); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
