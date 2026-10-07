package eventsource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Options struct {
	Region, AccountID string
	Dev               DevOptions
}

type CreateInput struct {
	EventSourceARN                 string   `json:"EventSourceArn"`
	FunctionName                   string   `json:"FunctionName"`
	BatchSize                      *int     `json:"BatchSize,omitempty"`
	Enabled                        *bool    `json:"Enabled,omitempty"`
	MaximumBatchingWindowInSeconds *int     `json:"MaximumBatchingWindowInSeconds,omitempty"`
	FunctionResponseTypes          []string `json:"FunctionResponseTypes,omitempty"`
}

type Mapping struct {
	UUID                           string   `json:"UUID"`
	EventSourceMappingARN          string   `json:"EventSourceMappingArn"`
	EventSourceARN                 string   `json:"EventSourceArn"`
	FunctionARN                    string   `json:"FunctionArn"`
	BatchSize                      int      `json:"BatchSize"`
	MaximumBatchingWindowInSeconds int      `json:"MaximumBatchingWindowInSeconds"`
	FunctionResponseTypes          []string `json:"FunctionResponseTypes"`
	State                          string   `json:"State"`
	StateTransitionReason          string   `json:"StateTransitionReason"`
	LastModified                   float64  `json:"LastModified"`
	LastProcessingResult           string   `json:"LastProcessingResult"`
}

type APIError struct {
	Code, Message string
	Status        int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }
func invalid(message string) error {
	return &APIError{Code: "InvalidParameterValueException", Status: http.StatusBadRequest, Message: message}
}
func missing(message string) error {
	return &APIError{Code: "ResourceNotFoundException", Status: http.StatusNotFound, Message: message}
}
func unavailable() error {
	return &APIError{Code: "ServiceException", Status: http.StatusServiceUnavailable, Message: "Event source mappings are closing"}
}

type entry struct {
	mapping Mapping
	queue   Queue
	cancel  context.CancelFunc
	done    chan struct{}
}

type Service struct {
	mu              sync.Mutex
	region, account string
	queues          QueueSource
	functions       FunctionInvoker
	entries         map[string]*entry
	dev             DevOptions
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	closing         bool
	done            chan struct{}
	closeErr        error
}

var accountPattern = regexp.MustCompile(`^[0-9]{12}$`)
var regionPattern = regexp.MustCompile(`^[a-z0-9-]+$`)
var queuePattern = regexp.MustCompile(`^arn:aws:sqs:([a-z0-9-]+):([0-9]{12}):([A-Za-z0-9_-]{1,80}(?:\.fifo)?)$`)
var functionPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}(?::(?:[A-Za-z0-9_-]{1,128}|\$LATEST))?$`)
var idPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func New(options Options, queues QueueSource, functions FunctionInvoker) (*Service, error) {
	if !regionPattern.MatchString(options.Region) || !accountPattern.MatchString(options.AccountID) {
		return nil, invalid("Region and twelve-digit AccountID are required")
	}
	if options.Dev.MaxMappings == 0 {
		options.Dev.MaxMappings = 1000
	}
	if options.Dev.EmptyPollDelay == 0 {
		options.Dev.EmptyPollDelay = 100 * time.Millisecond
	}
	if options.Dev.MaxMappings < 1 || options.Dev.EmptyPollDelay < 0 {
		return nil, invalid("Invalid development mapping capacity or empty-poll delay")
	}
	if options.Dev.Clock == nil {
		options.Dev.Clock = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{region: options.Region, account: options.AccountID, queues: queues, functions: functions, entries: make(map[string]*entry), dev: options.Dev, ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

func (s *Service) functionARN(value string) (string, error) {
	if strings.HasPrefix(value, "arn:") {
		prefix := "arn:aws:lambda:" + s.region + ":" + s.account + ":function:"
		if !strings.HasPrefix(value, prefix) {
			return "", invalid("FunctionName must identify a local registered Lambda function")
		}
		value = strings.TrimPrefix(value, prefix)
	} else if account, name, found := strings.Cut(value, ":function:"); found {
		if account != s.account {
			return "", invalid("Partial function ARN must use the local account")
		}
		value = name
	}
	if !functionPattern.MatchString(value) {
		return "", invalid("Invalid FunctionName or qualifier")
	}
	return "arn:aws:lambda:" + s.region + ":" + s.account + ":function:" + value, nil
}

func (s *Service) validate(input CreateInput) (string, error) {
	parts := queuePattern.FindStringSubmatch(input.EventSourceARN)
	if parts == nil || parts[1] != s.region || parts[2] != s.account || len(parts[3]) > 80 {
		return "", invalid("EventSourceArn must identify an owned local SQS queue")
	}
	// Native SQS defaults to 10. Require explicit 1 instead of quietly replacing
	// AWS's unsupported default with a different batch size.
	if input.BatchSize == nil || *input.BatchSize != 1 {
		return "", invalid("Only explicit BatchSize=1 is supported; the AWS default of 10 is not implemented")
	}
	if input.MaximumBatchingWindowInSeconds != nil && *input.MaximumBatchingWindowInSeconds != 0 {
		return "", invalid("Only MaximumBatchingWindowInSeconds=0 is supported")
	}
	if len(input.FunctionResponseTypes) != 0 {
		return "", invalid("Partial batch response types are not supported")
	}
	return s.functionARN(input.FunctionName)
}

func (s *Service) Create(ctx context.Context, input CreateInput) (Mapping, error) {
	if err := ctx.Err(); err != nil {
		return Mapping{}, err
	}
	arn, err := s.validate(input)
	if err != nil {
		return Mapping{}, err
	}
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return Mapping{}, unavailable()
	}
	if s.queues == nil || s.functions == nil {
		return Mapping{}, missing("Local SQS and Lambda runtimes must be configured")
	}
	queue, err := s.queues.ResolveQueue(ctx, input.EventSourceARN)
	if err != nil || queue == nil {
		if ctx.Err() != nil {
			return Mapping{}, ctx.Err()
		}
		return Mapping{}, missing("Source queue is not owned by this emulator")
	}
	info := queue.Info()
	if info.ARN != input.EventSourceARN {
		return Mapping{}, missing("Resolved queue identity does not match EventSourceArn")
	}
	timeout, err := s.functions.ValidateTarget(ctx, arn)
	if err != nil {
		if ctx.Err() != nil {
			return Mapping{}, ctx.Err()
		}
		return Mapping{}, missing("Target is not a registered local Lambda function or alias")
	}
	if timeout <= 0 || timeout > info.VisibilityTimeout {
		return Mapping{}, invalid("Function timeout must be positive and no greater than the queue visibility timeout")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Mapping{}, err
	}
	if s.closing {
		return Mapping{}, unavailable()
	}
	for _, prior := range s.entries {
		if prior.mapping.FunctionARN == arn && prior.mapping.EventSourceARN == input.EventSourceARN {
			return Mapping{}, &APIError{Code: "ResourceConflictException", Status: http.StatusConflict, Message: "A mapping already exists for this source and function"}
		}
	}
	if len(s.entries) >= s.dev.MaxMappings {
		return Mapping{}, &APIError{Code: "TooManyRequestsException", Status: http.StatusTooManyRequests, Message: "Local mapping capacity is full"}
	}
	id := uuid.NewString()
	state := "Enabled"
	if input.Enabled != nil && !*input.Enabled {
		state = "Disabled"
	}
	mapping := Mapping{UUID: id, EventSourceMappingARN: "arn:aws:lambda:" + s.region + ":" + s.account + ":event-source-mapping:" + id, EventSourceARN: input.EventSourceARN, FunctionARN: arn, BatchSize: 1, FunctionResponseTypes: []string{}, State: state, StateTransitionReason: "USER_INITIATED", LastModified: float64(s.dev.Clock().UnixMilli()) / 1000, LastProcessingResult: "No records processed"}
	workerCtx, cancel := context.WithCancel(s.ctx)
	item := &entry{mapping: mapping, queue: queue, cancel: cancel, done: make(chan struct{})}
	s.entries[id] = item
	if state == "Enabled" {
		s.wg.Add(1)
		go s.run(workerCtx, item)
	} else {
		cancel()
		close(item.done)
	}
	return cloneMapping(mapping), nil
}

func cloneMapping(mapping Mapping) Mapping {
	mapping.FunctionResponseTypes = append([]string{}, mapping.FunctionResponseTypes...)
	return mapping
}

func (s *Service) Get(id string) (Mapping, error) {
	if !idPattern.MatchString(id) {
		return Mapping{}, invalid("Invalid mapping UUID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.entries[strings.ToLower(id)]
	if item == nil {
		return Mapping{}, missing("Event source mapping not found")
	}
	return cloneMapping(item.mapping), nil
}

// Delete cancels and joins this mapping's receive and invocation before removal.
// A canceled request still joins cleanup; it never transfers owned work to an
// untracked goroutine or permits queue recreation to rebind the old mapping.
func (s *Service) Delete(ctx context.Context, id string) (Mapping, error) {
	if err := ctx.Err(); err != nil {
		return Mapping{}, err
	}
	if !idPattern.MatchString(id) {
		return Mapping{}, invalid("Invalid mapping UUID")
	}
	id = strings.ToLower(id)
	s.mu.Lock()
	item := s.entries[id]
	if item == nil {
		s.mu.Unlock()
		return Mapping{}, missing("Event source mapping not found")
	}
	if item.mapping.State == "Deleting" {
		s.mu.Unlock()
		return Mapping{}, &APIError{Code: "ResourceConflictException", Status: http.StatusConflict, Message: "Event source mapping is already deleting"}
	}
	item.mapping.State = "Deleting"
	item.mapping.LastModified = float64(s.dev.Clock().UnixMilli()) / 1000
	mapping := cloneMapping(item.mapping)
	item.cancel()
	s.mu.Unlock()
	select {
	case <-item.done:
	case <-ctx.Done():
		<-item.done
	}
	err := ctx.Err()
	s.mu.Lock()
	delete(s.entries, id)
	s.mu.Unlock()
	return mapping, err
}

func (s *Service) setResult(item *entry, result string) {
	s.mu.Lock()
	item.mapping.LastProcessingResult = result
	s.mu.Unlock()
}

func (s *Service) pause(ctx context.Context) bool {
	timer := time.NewTimer(s.dev.EmptyPollDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) run(ctx context.Context, item *entry) {
	defer s.wg.Done()
	defer close(item.done)
	defer item.cancel()
	for ctx.Err() == nil {
		record, err := item.queue.Receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.mu.Lock()
			item.mapping.LastProcessingResult = "Source receive failed"
			if item.mapping.State != "Deleting" {
				item.mapping.State = "Disabled"
				item.mapping.StateTransitionReason = "LAMBDA_INITIATED"
				item.mapping.LastModified = float64(s.dev.Clock().UnixMilli()) / 1000
			}
			s.mu.Unlock()
			return // A deleted bound source is never looked up again by ARN.
		}
		if record == nil {
			// Ports must long poll; this bounded pause also prevents a failed
			// adapter from turning an empty queue into an unbounded CPU loop.
			if !s.pause(ctx) {
				return
			}
			continue
		}
		payload, err := json.Marshal(SQSEvent{Records: []Record{*record}})
		if err == nil {
			err = s.functions.InvokeTarget(ctx, item.mapping.FunctionARN, payload)
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.setResult(item, "Function invocation failed")
			continue // Broker visibility and redrive remain the sole retry owner.
		}
		deleted, err := item.queue.Delete(ctx, record.ReceiptHandle)
		if err != nil || !deleted {
			s.setResult(item, "Source acknowledgment failed")
			continue
		}
		s.setResult(item, "OK")
	}
}

// Close stops new mappings, cancels polling/execution and joins all owned work.
// Call it before closing the Lambda runner or queue owner. The first Close call's
// context governs the terminal shutdown result. One owner publishes that result
// only after the resource barrier joins cleanup; every caller receives it.
func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		s.cancel()
		go func() {
			s.wg.Wait()
			s.mu.Lock()
			// Check after join: work completion and cancellation can both be
			// ready before this goroutine resumes. Publishing the result before
			// done prevents another Close caller from seeing a provisional nil.
			if err := ctx.Err(); err != nil {
				s.closeErr = fmt.Errorf("join event source mappings: %w", err)
			}
			close(s.done)
			s.mu.Unlock()
		}()
	}
	s.mu.Unlock()
	<-s.done // Cancellation never releases the resource barrier early.
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}
