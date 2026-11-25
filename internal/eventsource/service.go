package eventsource

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/devcapture"
)

type Options struct {
	Region, AccountID string
	Dev               DevOptions
}

type ScalingConfig struct {
	MaximumConcurrency *int `json:"MaximumConcurrency,omitempty"`
}

// ProvisionedPollerConfig selects the native SQS limits. Locally, a fixed
// worker set uses the minimum, subject to the separate dev worker cap.
type ProvisionedPollerConfig struct {
	MinimumPollers *int `json:"MinimumPollers,omitempty"`
	MaximumPollers *int `json:"MaximumPollers,omitempty"`
}

type CreateInput struct {
	FilterCriteria                 *FilterCriteria          `json:"FilterCriteria,omitempty"`
	ProvisionedPollerConfig        *ProvisionedPollerConfig `json:"ProvisionedPollerConfig,omitempty"`
	Tags                           map[string]string        `json:"Tags,omitempty"`
	EventSourceARN                 string                   `json:"EventSourceArn"`
	FunctionName                   string                   `json:"FunctionName"`
	BatchSize                      *int                     `json:"BatchSize,omitempty"`
	Enabled                        *bool                    `json:"Enabled,omitempty"`
	MaximumBatchingWindowInSeconds *int                     `json:"MaximumBatchingWindowInSeconds,omitempty"`
	FunctionResponseTypes          []string                 `json:"FunctionResponseTypes,omitempty"`
	ScalingConfig                  *ScalingConfig           `json:"ScalingConfig,omitempty"`
}

type Mapping struct {
	FilterCriteria                 *FilterCriteria          `json:"FilterCriteria,omitempty"`
	ProvisionedPollerConfig        *ProvisionedPollerConfig `json:"ProvisionedPollerConfig,omitempty"`
	UUID                           string                   `json:"UUID"`
	EventSourceMappingARN          string                   `json:"EventSourceMappingArn"`
	EventSourceARN                 string                   `json:"EventSourceArn"`
	FunctionARN                    string                   `json:"FunctionArn"`
	BatchSize                      int                      `json:"BatchSize"`
	MaximumBatchingWindowInSeconds int                      `json:"MaximumBatchingWindowInSeconds"`
	FunctionResponseTypes          []string                 `json:"FunctionResponseTypes"`
	ScalingConfig                  *ScalingConfig           `json:"ScalingConfig,omitempty"`
	State                          string                   `json:"State"`
	StateTransitionReason          string                   `json:"StateTransitionReason"`
	LastModified                   float64                  `json:"LastModified"`
	LastProcessingResult           string                   `json:"LastProcessingResult"`
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
	receive             BatchReceiveOptions
	tags                map[string]string
	updateTarget        string // Nonempty while Update owns this entry.
	stopIntake          context.CancelFunc
	mapping             Mapping
	queue               Queue
	cancel              context.CancelFunc
	done                chan struct{}
	retained            DevQueue
	releaseRegistration func()
	receiptEvidence     ReceiptEvidenceQueue
}

type Service struct {
	mu                  sync.Mutex
	region, account     string
	queues              QueueSource
	functions           FunctionInvoker
	entries             map[string]*entry
	dev                 DevOptions
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	closing             bool
	done                chan struct{}
	closeErr            error
	deliveryCapture     *devcapture.Sink
	deliveryLogPath     string
	deliveryEvidenceErr error
	deliveryClosed      bool
	observedFunctions   ObservedFunctionInvoker
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
	if (options.Dev.Source == nil) != (options.Dev.Activity == nil) {
		return nil, invalid("Development source admission and activity must be configured together")
	}
	if options.Dev.MaxMappings == 0 {
		options.Dev.MaxMappings = 1000
	}
	if options.Dev.MaxWorkersPerMapping == 0 {
		options.Dev.MaxWorkersPerMapping = 32
	}
	if options.Dev.EmptyPollDelay == 0 {
		options.Dev.EmptyPollDelay = 100 * time.Millisecond
	}
	if options.Dev.MaxMappings < 1 || options.Dev.MaxWorkersPerMapping < 1 || options.Dev.EmptyPollDelay < 0 {
		return nil, invalid("Invalid development mapping capacity, worker cap or empty-poll delay")
	}
	if options.Dev.Clock == nil {
		options.Dev.Clock = time.Now
	}
	var observed ObservedFunctionInvoker
	if options.Dev.DeliveryCapture != nil {
		var ok bool
		observed, ok = functions.(ObservedFunctionInvoker)
		if !ok {
			return nil, invalid("Delivery capture requires an observed native Lambda invoker")
		}
		config := *options.Dev.DeliveryCapture
		options.Dev.DeliveryCapture = &config
	}
	capture, logPath, err := openDeliveryCapture(options.Dev.DeliveryCapture)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{region: options.Region, account: options.AccountID, queues: queues, functions: functions, entries: make(map[string]*entry), dev: options.Dev, ctx: ctx, cancel: cancel, done: make(chan struct{}), deliveryCapture: capture, deliveryLogPath: logPath, observedFunctions: observed}, nil
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

func (s *Service) validateSourceARN(value string) error {
	parts := queuePattern.FindStringSubmatch(value)
	if parts == nil || parts[1] != s.region || parts[2] != s.account || len(parts[3]) > 80 {
		return invalid("EventSourceArn must identify an owned local SQS queue")
	}
	return nil
}

func (s *Service) validate(input CreateInput) (string, error) {
	if err := s.validateSourceARN(input.EventSourceARN); err != nil {
		return "", err
	}
	if err := validateMappingTags(input.Tags); err != nil {
		return "", err
	}
	if err := validateBatchConfig(input); err != nil {
		return "", err
	}
	if input.ScalingConfig != nil && input.ScalingConfig.MaximumConcurrency != nil {
		maximum := *input.ScalingConfig.MaximumConcurrency
		if maximum < 2 || maximum > 1000 {
			return "", invalid("ScalingConfig.MaximumConcurrency must be 2 through 1000")
		}
	}
	if len(input.FunctionResponseTypes) > 1 || len(input.FunctionResponseTypes) == 1 && input.FunctionResponseTypes[0] != reportBatchItemFailures {
		return "", invalid("FunctionResponseTypes must be empty or contain only ReportBatchItemFailures")
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
	timeout, err := s.validateBinding(ctx, queue, input.EventSourceARN, arn)
	if err != nil {
		return Mapping{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Mapping{}, err
	}
	if s.closing {
		return Mapping{}, unavailable()
	}
	if s.hasMappingConflict(nil, input.EventSourceARN, arn) {
		return Mapping{}, mappingConflict("A mapping already exists for this source and function")
	}
	if len(s.entries) >= s.dev.MaxMappings {
		return Mapping{}, &APIError{Code: "TooManyRequestsException", Status: http.StatusTooManyRequests, Message: "Local mapping capacity is full"}
	}
	id := uuid.NewString()
	state := "Enabled"
	if input.Enabled != nil && !*input.Enabled {
		state = "Disabled"
	}
	batchSize := 10 // Native SQS default.
	if input.BatchSize != nil {
		batchSize = *input.BatchSize
	}
	scaling := cloneScalingConfig(input.ScalingConfig)
	mapping := Mapping{UUID: id, EventSourceMappingARN: "arn:aws:lambda:" + s.region + ":" + s.account + ":event-source-mapping:" + id, EventSourceARN: input.EventSourceARN, FunctionARN: arn, BatchSize: batchSize, ScalingConfig: scaling, FunctionResponseTypes: append([]string{}, input.FunctionResponseTypes...), State: state, StateTransitionReason: "USER_INITIATED", LastModified: float64(s.dev.Clock().UnixMilli()) / 1000, LastProcessingResult: "No records processed"}
	mapping.MaximumBatchingWindowInSeconds = batchWindowSeconds(input.MaximumBatchingWindowInSeconds)
	mapping.ProvisionedPollerConfig = cloneProvisionedPollerConfig(input.ProvisionedPollerConfig)
	mapping.FilterCriteria = cloneFilterCriteria(input.FilterCriteria)
	item, err := s.prepareEntry(mapping, queue, maps.Clone(input.Tags), timeout)
	if err != nil {
		return Mapping{}, err
	}
	s.entries[id] = item
	s.startEntry(item)
	return cloneMapping(mapping), nil
}

func (s *Service) validateBinding(ctx context.Context, queue Queue, source, function string) (time.Duration, error) {
	info, err := queue.Info()
	if err != nil || info.ARN != source {
		return 0, missing("Original source queue is unavailable")
	}
	timeout, err := s.functions.ValidateTarget(ctx, function)
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if err != nil {
		return 0, missing("Target is not a registered local Lambda function or alias")
	}
	if timeout <= 0 || timeout > info.VisibilityTimeout {
		return 0, invalid("Function timeout must be positive and no greater than the queue visibility timeout")
	}
	return timeout, nil
}

func (s *Service) hasMappingConflict(except *entry, source, function string) bool {
	for _, prior := range s.entries {
		if prior != except && prior.mapping.EventSourceARN == source &&
			(prior.mapping.FunctionARN == function || prior.updateTarget == function) {
			return true
		}
	}
	return false
}

func mappingConflict(message string) error {
	return &APIError{Code: "ResourceConflictException", Status: http.StatusConflict, Message: message}
}

func (s *Service) prepareEntry(mapping Mapping, queue Queue, tags map[string]string, timeout time.Duration) (*entry, error) {
	match, err := compileFilterCriteria(mapping.FilterCriteria)
	if err != nil {
		return nil, err
	}
	item := &entry{mapping: mapping, queue: queue, tags: tags, done: make(chan struct{}),
		receive: BatchReceiveOptions{MaxRecords: mapping.BatchSize,
			Window: time.Duration(mapping.MaximumBatchingWindowInSeconds) * time.Second, InvocationTimeout: timeout, Match: match}}
	if mapping.State == "Disabled" {
		return item, nil
	}
	if s.deliveryCapture != nil {
		receipts, ok := queue.(ReceiptEvidenceQueue)
		if !ok {
			return nil, &APIError{Code: "ServiceException", Status: http.StatusServiceUnavailable, Message: "Canonical receipt evidence is unavailable"}
		}
		item.receiptEvidence = receipts
	}
	if s.dev.Source != nil {
		retained, ok := queue.(DevQueue)
		if !ok {
			return nil, &APIError{Code: "ServiceException", Status: http.StatusServiceUnavailable, Message: "Retained SQS custody is unavailable"}
		}
		release, err := retained.RegisterRetained()
		if err != nil || release == nil {
			return nil, &APIError{Code: "ServiceException", Status: http.StatusServiceUnavailable, Message: "Cannot register retained SQS custody"}
		}
		item.retained, item.releaseRegistration = retained, release
	}
	return item, nil
}

// startEntry publishes the worker owner while holding s.mu. A generation keeps
// its configuration and original queue handle until every worker has joined.
func (s *Service) startEntry(item *entry) {
	execCtx, cancel := context.WithCancel(s.ctx)
	intakeCtx, stopIntake := context.WithCancel(execCtx)
	item.cancel, item.stopIntake = cancel, stopIntake
	if item.mapping.State == "Disabled" {
		cancel()
		close(item.done)
		return
	}
	workers := 1
	if scaling := item.mapping.ScalingConfig; scaling != nil {
		workers = *scaling.MaximumConcurrency
	} else if provisioned := item.mapping.ProvisionedPollerConfig; provisioned != nil {
		workers = *provisioned.MinimumPollers
	}
	workers = min(workers, s.dev.MaxWorkersPerMapping)
	s.wg.Add(1)
	go s.run(execCtx, intakeCtx, item, workers)
}

func cloneScalingConfig(scaling *ScalingConfig) *ScalingConfig {
	if scaling == nil || scaling.MaximumConcurrency == nil {
		return nil // An empty native ScalingConfig selects no concurrency ceiling.
	}
	maximum := *scaling.MaximumConcurrency
	return &ScalingConfig{MaximumConcurrency: &maximum}
}

func cloneMapping(mapping Mapping) Mapping {
	mapping.FunctionResponseTypes = append([]string{}, mapping.FunctionResponseTypes...)
	mapping.ScalingConfig = cloneScalingConfig(mapping.ScalingConfig)
	mapping.ProvisionedPollerConfig = cloneProvisionedPollerConfig(mapping.ProvisionedPollerConfig)
	mapping.FilterCriteria = cloneFilterCriteria(mapping.FilterCriteria)
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
	if item.updateTarget != "" {
		s.mu.Unlock()
		return Mapping{}, &APIError{Code: "ResourceInUseException", Status: http.StatusBadRequest, Message: "Event source mapping is updating"}
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
	if item.mapping.State == "Enabled" || item.mapping.State == "Updating" {
		item.mapping.LastProcessingResult = result
	}
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

// run owns a fixed worker set until every receive and execution has joined.
// A worker never admits a second invocation before its current batch settles.
func (s *Service) run(ctx, intakeCtx context.Context, item *entry, count int) {
	defer s.wg.Done()
	defer close(item.done)
	defer item.cancel()
	if item.releaseRegistration != nil {
		defer item.releaseRegistration()
	}
	var workers sync.WaitGroup
	workers.Add(count)
	for range count {
		go func() {
			defer workers.Done()
			s.poll(ctx, intakeCtx, item)
		}()
	}
	workers.Wait()
}

func (s *Service) disableSource(item *entry, result string) {
	s.mu.Lock()
	if item.mapping.State != "Deleting" && item.mapping.State != "Updating" {
		item.mapping.State = "Disabled"
		item.mapping.StateTransitionReason = "LAMBDA_INITIATED"
		item.mapping.LastModified = float64(s.dev.Clock().UnixMilli()) / 1000
	}
	item.mapping.LastProcessingResult = result
	item.cancel() // All peer receives and actual executions share this owner.
	s.mu.Unlock()
}

func (s *Service) poll(ctx, intakeCtx context.Context, item *entry) {
	for intakeCtx.Err() == nil {
		if s.deliveryCapture != nil && s.EvidenceErr() != nil {
			s.disableSource(item, "Delivery evidence unavailable")
			return // Never lease fresh messages against failed required evidence.
		}
		empty, keepPolling := s.pollBatch(ctx, intakeCtx, item)
		if !keepPolling {
			return
		}
		// The lease is complete before this empty-poll guard. Fenced workers
		// subsequently park on source/queue changes without owning empty retries.
		if empty && !s.pause(intakeCtx) {
			return
		}
	}
}

func (s *Service) pollBatch(ctx, intakeCtx context.Context, item *entry) (empty, keepPolling bool) {
	batch, complete, err := s.receive(intakeCtx, item)
	var evidenceErr error
	if complete != nil {
		defer func() { complete(evidenceErr) }() // Ordinary business failure retains queue custody.
	}
	if ctx.Err() != nil || intakeCtx.Err() != nil && len(batch.Records) == 0 {
		return false, false
	}
	if err != nil {
		s.disableSource(item, "Source receive failed")
		return false, false // A deleted bound source is never looked up again.
	}
	records := batch.Records
	if len(records) == 0 {
		return true, true
	}
	if len(records) > item.mapping.BatchSize || len(batch.Payload) > MaxBatchPayloadBytes || batch.Payload == "" {
		s.disableSource(item, "Source batch exceeds the supported count or payload limit")
		return false, false // Invalid adapter leases remain unacknowledged.
	}
	payload := []byte(batch.Payload) // Invocation owns mutable transport bytes, not the admitted string.
	if s.deliveryCapture != nil {
		keepPolling, evidenceErr = s.deliverObserved(ctx, item, records, payload)
		return false, keepPolling
	}
	response, err := s.functions.InvokeTarget(ctx, item.mapping.FunctionARN, payload)
	if ctx.Err() != nil {
		return false, false
	}
	if err != nil {
		s.setResult(item, "Function invocation failed")
		return false, true // Queue visibility/redrive remain the sole retry owner.
	}
	failures, err := batchFailures(item.mapping.FunctionResponseTypes, records, response)
	if err != nil {
		s.setResult(item, "Invalid batch response")
		return false, true
	}
	acknowledged := true
	for _, record := range records {
		if failures.includes(record) {
			continue
		}
		settled, err := item.queue.Delete(ctx, record.ReceiptHandle)
		if err != nil || !settled {
			acknowledged = false
		}
		if ctx.Err() != nil {
			return false, false
		}
	}
	s.setResult(item, batchProcessingResult(failures, acknowledged))
	return false, true
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
			if s.deliveryCapture != nil {
				s.deliveryEvidenceErr = errors.Join(s.deliveryEvidenceErr, s.deliveryCapture.Close())
				s.deliveryClosed = true
			}
			s.closeErr = errors.Join(s.closeErr, s.deliveryEvidenceErr)
			// Check after join: work completion and cancellation can both be
			// ready before this goroutine resumes. Publishing the result before
			// done prevents another Close caller from seeing a provisional nil.
			if err := ctx.Err(); err != nil {
				s.closeErr = errors.Join(s.closeErr, fmt.Errorf("join event source mappings: %w", err))
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
