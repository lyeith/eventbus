package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"
)

const maxEventPayload = 1 << 20
const maxEventAge = 6 * time.Hour

// AsyncRecord is redacted local evidence, never a claim that 202 completed the
// handler. No input, function output, error message, credentials or logs enter
// this record. Completed records are bounded by the explicit harness history.
type AsyncRecord struct {
	SchemaVersion string    `json:"schema_version"`
	RequestID     string    `json:"request_id"`
	FunctionName  string    `json:"function_name"`
	State         string    `json:"state"`
	Attempts      int       `json:"attempts"`
	QueuedAt      time.Time `json:"queued_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	ErrorType     string    `json:"error_type,omitempty"`
}
type asyncTask struct {
	entry        executableFunction
	input        invocation
	record       AsyncRecord
	readyAt      time.Time
	release      func(error)
	ownershipErr error
}

// Admit transfers a private copy into this instance's bounded in-memory queue.
// Capacity includes queued, retry-waiting and running tasks. Accepted events
// outlive caller cancellation; no persistent/restart guarantee is implied.
func (service *Service) Admit(ctx context.Context, input InvokeInput) (Admission, error) {
	if err := ctx.Err(); err != nil {
		return Admission{}, err
	}
	entry, name, err := service.resolveTarget(input.FunctionName, input.Qualifier)
	if err != nil {
		return Admission{}, err
	}
	if err := validateExecution(input, maxEventPayload); err != nil {
		return Admission{}, err
	}
	input.Payload = append([]byte(nil), input.Payload...)
	input.ClientContext = "" // Native Event never passes ClientContext to handlers.
	now := time.Now().UTC()
	requestID := uuid.NewString()
	task := &asyncTask{entry: entry, input: prepareInvocation(entry, name, input, requestID), readyAt: now, record: AsyncRecord{SchemaVersion: "eventbus.lambda.async.v1", RequestID: requestID, FunctionName: name, State: "queued", QueuedAt: now, UpdatedAt: now}}
	// Async transitions serialize durable capture and publication independently
	// of the shared invocation/registry mutex. Always take transitionMu before mu.
	service.asyncTransitionMu.Lock()
	defer service.asyncTransitionMu.Unlock()
	service.mu.Lock()
	if err := ctx.Err(); err != nil {
		service.mu.Unlock()
		return Admission{}, err
	}
	if service.closed || service.asyncClosed {
		service.mu.Unlock()
		return Admission{}, invocationError(http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
	}
	if service.asyncEvidenceErr != nil {
		service.mu.Unlock()
		return Admission{}, invocationError(http.StatusInternalServerError, "ServiceException", "Lambda async evidence is unavailable")
	}
	if service.asyncOutstanding >= service.asyncCapacity {
		service.mu.Unlock()
		return Admission{}, invocationError(http.StatusTooManyRequests, "TooManyRequestsException", "Lambda async queue capacity is exhausted")
	}
	release, err := service.beginActivityLocked("lambda_async", requestID)
	if err != nil {
		service.mu.Unlock()
		return Admission{}, err
	}
	task.release = release
	// A reservation wins admission against a later Close and counts capacity
	// and retained work, but is neither executable nor a published queued record.
	service.asyncOutstanding++
	service.asyncAdmissions.Add(1)
	service.mu.Unlock()
	reserved := true
	defer service.asyncAdmissions.Done()
	defer func() {
		if reserved {
			// Propagate a borrowed writer's panic/Goexit without orphaning this
			// unpublished reservation or treating interrupted evidence as healthy.
			service.mu.Lock()
			service.rollbackAsyncAdmissionLocked(task, errors.New("Lambda async admission capture was interrupted"))
			service.mu.Unlock()
		}
	}()
	captureErr := service.asyncCapture.Append(task.record)
	service.mu.Lock()
	reserved = false
	if captureErr != nil {
		service.rollbackAsyncAdmissionLocked(task, captureErr)
		service.mu.Unlock()
		return Admission{}, invocationError(http.StatusInternalServerError, "ServiceException", "Cannot capture Lambda async admission")
	}
	service.asyncTasks[requestID] = task
	aborted := service.asyncAborted
	if !aborted {
		service.asyncQueue = append(service.asyncQueue, task)
	}
	service.wakeAsyncLocked()
	service.mu.Unlock()
	if aborted {
		// The reservation preceded shutdown, so successful queued capture still
		// returns native acceptance. Actual abort owns a canceled terminal record
		// and never launches this event.
		service.finishAsync(task, "canceled", "ServiceShutdown")
	}
	return Admission{RequestID: requestID}, nil
}

// The caller owns mu and the unpublished reservation's transition.
func (service *Service) rollbackAsyncAdmissionLocked(task *asyncTask, err error) {
	if service.asyncEvidenceErr == nil {
		service.asyncEvidenceErr = err
	}
	service.asyncOutstanding--
	service.wakeAsyncLocked()
	if task.release != nil {
		task.release(err)
		task.release = nil
	}
}

func (service *Service) wakeAsyncLocked() {
	close(service.asyncWake)
	service.asyncWake = make(chan struct{})
}

// AsyncSnapshot returns value copies. Terminal records may age out of this
// bounded diagnostic history; an optional dev_async.log_path retains JSONL.
func (service *Service) AsyncSnapshot() []AsyncRecord {
	service.mu.Lock()
	records := make([]AsyncRecord, len(service.asyncHistory), len(service.asyncHistory)+len(service.asyncTasks))
	copy(records, service.asyncHistory)
	for _, task := range service.asyncTasks {
		records = append(records, task.record)
	}
	service.mu.Unlock()
	sort.Slice(records, func(i, j int) bool {
		if records[i].QueuedAt.Equal(records[j].QueuedAt) {
			return records[i].RequestID < records[j].RequestID
		}
		return records[i].QueuedAt.Before(records[j].QueuedAt)
	})
	return records
}

// nextAsyncRecord copies an immutable proposed transition. The task's published
// state changes only after its capture attempt has completed.
func nextAsyncRecord(record AsyncRecord, state, errorType string) AsyncRecord {
	record.State = state
	record.ErrorType = errorType
	record.UpdatedAt = time.Now().UTC()
	if state == "succeeded" || state == "failed" || state == "canceled" {
		record.CompletedAt = record.UpdatedAt
	}
	return record
}

// recordAsync and finishAsync require asyncTransitionMu, with mu released. All
// filesystem/borrowed-writer I/O stays outside the shared service mutex.
func (service *Service) captureAsyncRecord(record AsyncRecord) {
	err := service.asyncCapture.Append(record)
	service.mu.Lock()
	firstFailure := err != nil && service.asyncEvidenceErr == nil
	if firstFailure {
		service.asyncEvidenceErr = err
	}
	service.mu.Unlock()
	if firstFailure {
		log.Printf("Lambda async evidence failed request_id=%s function=%s state=%s", record.RequestID, record.FunctionName, record.State)
	}
}

func (service *Service) recordAsync(task *asyncTask, record AsyncRecord) {
	service.captureAsyncRecord(record)
	service.mu.Lock()
	task.record = record
	service.mu.Unlock()
}

func (service *Service) finishAsync(task *asyncTask, state, errorType string) {
	service.mu.Lock()
	record := nextAsyncRecord(task.record, state, errorType)
	record.NextAttemptAt = time.Time{}
	service.mu.Unlock()
	service.captureAsyncRecord(record)
	service.mu.Lock()
	defer service.mu.Unlock()
	task.record = record
	delete(service.asyncTasks, task.record.RequestID)
	service.asyncOutstanding--
	service.appendAsyncHistoryLocked(task.record)
	service.wakeAsyncLocked()
	if task.release != nil {
		task.release(errors.Join(task.ownershipErr, service.asyncEvidenceErr))
		task.release = nil
	}
}

// appendAsyncHistoryLocked retains the most recently completed records without
// shifting the entire bounded history on every completion. Snapshot ordering is
// independent of this physical ring layout and is applied to detached copies.
func (service *Service) appendAsyncHistoryLocked(record AsyncRecord) {
	if service.asyncHistoryLimit <= 0 {
		return
	}
	if len(service.asyncHistory) < service.asyncHistoryLimit {
		service.asyncHistory = append(service.asyncHistory, record)
		return
	}
	service.asyncHistory[service.asyncHistoryNext] = record
	service.asyncHistoryNext = (service.asyncHistoryNext + 1) % service.asyncHistoryLimit
}

// pollAsync owns one selection/capture transaction. A nil wake with no task
// means an expired event was settled and the worker should select again.
func (service *Service) pollAsync() (*asyncTask, <-chan struct{}, time.Time, bool) {
	service.asyncTransitionMu.Lock()
	defer service.asyncTransitionMu.Unlock()
	service.mu.Lock()
	if service.asyncAborted || service.asyncClosed && service.asyncOutstanding == 0 {
		service.mu.Unlock()
		return nil, nil, time.Time{}, true
	}
	now := time.Now()
	selected := -1
	var earliest time.Time
	for index, task := range service.asyncQueue {
		if earliest.IsZero() || task.readyAt.Before(earliest) {
			earliest = task.readyAt
			selected = index
		}
	}
	if selected >= 0 && !now.Before(earliest) {
		task := service.asyncQueue[selected]
		copy(service.asyncQueue[selected:], service.asyncQueue[selected+1:])
		service.asyncQueue[len(service.asyncQueue)-1] = nil
		service.asyncQueue = service.asyncQueue[:len(service.asyncQueue)-1]
		if now.Sub(task.record.QueuedAt) >= maxEventAge {
			service.mu.Unlock()
			service.finishAsync(task, "failed", "EventAgeExceeded")
			return nil, nil, time.Time{}, false
		}
		record := nextAsyncRecord(task.record, "running", "")
		record.Attempts++
		record.NextAttemptAt = time.Time{}
		service.mu.Unlock()
		service.recordAsync(task, record)
		return task, nil, time.Time{}, false
	}
	wake := service.asyncWake
	service.mu.Unlock()
	return nil, wake, earliest, false
}

func (service *Service) takeAsync() *asyncTask {
	for {
		task, wake, earliest, stopped := service.pollAsync()
		if stopped || task != nil {
			return task
		}
		if wake == nil {
			continue
		}
		if earliest.IsZero() {
			select {
			case <-wake:
			case <-service.asyncContext.Done():
				return nil
			}
		} else {
			timer := time.NewTimer(time.Until(earliest))
			select {
			case <-wake:
			case <-timer.C:
			case <-service.asyncContext.Done():
				timer.Stop()
				return nil
			}
			timer.Stop()
		}
	}
}

func asyncErrorType(result invocationResult) string {
	var value struct {
		Type string `json:"errorType"`
	}
	_ = json.Unmarshal(result.payload, &value)
	switch value.Type {
	case "Sandbox.Timedout", "Runtime.ExitError", "Runtime.InvalidResponse", "Runtime.InvalidEntrypoint", "Runtime.InternalError", "Function.ResponseSizeTooLarge":
		return value.Type
	default:
		return "FunctionError"
	}
}

func (service *Service) asyncWorker() {
	defer service.asyncWorkers.Done()
	for {
		task := service.takeAsync()
		if task == nil {
			return
		}
		input := task.input
		input.attempt = task.record.Attempts
		result, err := service.invokeOwned(service.asyncContext, task.entry, input, true)
		service.completeAsync(task, result, err)
	}
}

// The native attempt has actually joined before its terminal/retry transaction.
func (service *Service) completeAsync(task *asyncTask, result invocationResult, err error) {
	service.asyncTransitionMu.Lock()
	defer service.asyncTransitionMu.Unlock()
	service.mu.Lock()
	task.ownershipErr = errors.Join(task.ownershipErr, result.ownershipErr)
	if result.ownershipErr != nil && service.asyncOwnershipErr == nil {
		service.asyncOwnershipErr = result.ownershipErr
	}
	aborted := service.asyncAborted
	record := task.record
	service.mu.Unlock()
	switch {
	case aborted || errors.Is(err, context.Canceled):
		service.finishAsync(task, "canceled", "ServiceShutdown")
	case err != nil:
		service.finishAsync(task, "failed", "ServiceError")
	case !result.functionError:
		service.finishAsync(task, "succeeded", "")
	case record.Attempts <= 2:
		readyAt := time.Now().Add(service.asyncRetryDelays[record.Attempts-1])
		record = nextAsyncRecord(record, "retrying", asyncErrorType(result))
		record.NextAttemptAt = readyAt.UTC()
		service.recordAsync(task, record)
		service.mu.Lock()
		aborted = service.asyncAborted
		if !aborted {
			task.readyAt = readyAt
			service.asyncQueue = append(service.asyncQueue, task)
			service.wakeAsyncLocked()
		}
		service.mu.Unlock()
		if aborted {
			service.finishAsync(task, "canceled", "ServiceShutdown")
		}
	default:
		service.finishAsync(task, "failed", asyncErrorType(result))
	}
}

// joinAsync waits accepted children and admission reservations before settling
// aborted queued events. No new reservation can be added after workers stop:
// shutdown fenced admission under mu before making worker exit possible.
func (service *Service) joinAsync() {
	service.asyncWorkers.Wait()
	service.asyncAdmissions.Wait()
	service.asyncTransitionMu.Lock()
	defer service.asyncTransitionMu.Unlock()
	for {
		service.mu.Lock()
		if len(service.asyncQueue) == 0 {
			close(service.asyncDrainDone)
			service.mu.Unlock()
			return
		}
		task := service.asyncQueue[0]
		service.asyncQueue[0] = nil
		service.asyncQueue = service.asyncQueue[1:]
		service.mu.Unlock()
		service.finishAsync(task, "canceled", "ServiceShutdown")
	}
}

// DrainAsync quiesces Event admission while synchronous Execute/HTTP Invoke and
// backing AWS listeners remain usable by accepted handlers. It joins all async
// work and workers; Close later owns synchronous cleanup and sink closure.
// A deadline cancels accepted async work only and retains a non-nil result.
// Completed async ownership uncertainty remains a drain error across retries;
// synchronous execution uncertainty belongs only to final Close.
func (service *Service) DrainAsync(ctx context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	if !service.asyncClosed {
		service.asyncClosed = true
		service.wakeAsyncLocked()
	}
	service.mu.Unlock()
	select {
	case <-service.asyncDrainDone:
	case <-ctx.Done():
		service.mu.Lock()
		select {
		case <-service.asyncDrainDone:
			service.mu.Unlock()
			return service.asyncDrainError()
		default:
		}
		service.abortAsyncLocked(ctx.Err())
		service.mu.Unlock()
		<-service.asyncDrainDone
	}
	return service.asyncDrainError()
}

func (service *Service) asyncDrainError() error {
	service.mu.Lock()
	defer service.mu.Unlock()
	return errors.Join(service.asyncAbortErr, service.asyncEvidenceErr, service.asyncOwnershipErr)
}

// The caller owns mu. Fencing/cancellation never waits for capture. joinAsync
// owns queued terminal capture after workers and pending admissions join.
// Synchronous invocations retain their caller contexts and final Close owner.
func (service *Service) abortAsyncLocked(reason error) {
	if service.asyncAborted {
		return
	}
	service.asyncClosed = true
	service.asyncAborted = true
	service.asyncAbortErr = reason
	service.asyncCancel(errServiceCancellation)
	for _, owner := range service.active {
		if owner.asynchronous {
			owner.cancel()
		}
	}
	service.wakeAsyncLocked()
}
