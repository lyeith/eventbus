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
	service.mu.Lock()
	defer service.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Admission{}, err
	}
	if service.closed || service.asyncClosed {
		return Admission{}, invocationError(http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
	}
	if service.asyncEvidenceErr != nil {
		return Admission{}, invocationError(http.StatusInternalServerError, "ServiceException", "Lambda async evidence is unavailable")
	}
	if service.asyncOutstanding >= service.asyncCapacity {
		return Admission{}, invocationError(http.StatusTooManyRequests, "TooManyRequestsException", "Lambda async queue capacity is exhausted")
	}
	release, err := service.beginActivityLocked("lambda_async", requestID)
	if err != nil {
		return Admission{}, err
	}
	task.release = release
	// The small evidence append and publication are atomic with service closing.
	// Capture writers are owned file/stdio sinks, never application callbacks.
	if err := service.asyncCapture.Append(task.record); err != nil {
		service.asyncEvidenceErr = err
		if task.release != nil {
			task.release(err)
		}
		return Admission{}, invocationError(http.StatusInternalServerError, "ServiceException", "Cannot capture Lambda async admission")
	}
	service.asyncOutstanding++
	service.asyncTasks[requestID] = task
	service.asyncQueue = append(service.asyncQueue, task)
	service.wakeAsyncLocked()
	return Admission{RequestID: requestID}, nil
}

func (service *Service) wakeAsyncLocked() {
	close(service.asyncWake)
	service.asyncWake = make(chan struct{})
}

// AsyncSnapshot returns value copies. Terminal records may age out of this
// bounded diagnostic history; an optional dev_async.log_path retains JSONL.
func (service *Service) AsyncSnapshot() []AsyncRecord {
	service.mu.Lock()
	defer service.mu.Unlock()
	records := append([]AsyncRecord(nil), service.asyncHistory...)
	for _, task := range service.asyncTasks {
		records = append(records, task.record)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].QueuedAt.Equal(records[j].QueuedAt) {
			return records[i].RequestID < records[j].RequestID
		}
		return records[i].QueuedAt.Before(records[j].QueuedAt)
	})
	return records
}

func (service *Service) recordAsyncLocked(task *asyncTask, state, errorType string) {
	task.record.State = state
	task.record.ErrorType = errorType
	task.record.UpdatedAt = time.Now().UTC()
	if state == "succeeded" || state == "failed" || state == "canceled" {
		task.record.CompletedAt = task.record.UpdatedAt
	}
	if err := service.asyncCapture.Append(task.record); err != nil && service.asyncEvidenceErr == nil {
		service.asyncEvidenceErr = err
		log.Printf("Lambda async evidence failed request_id=%s function=%s state=%s", task.record.RequestID, task.record.FunctionName, state)
	}
}
func (service *Service) finishAsyncLocked(task *asyncTask, state, errorType string) {
	task.record.NextAttemptAt = time.Time{}
	service.recordAsyncLocked(task, state, errorType)
	delete(service.asyncTasks, task.record.RequestID)
	service.asyncOutstanding--
	service.asyncHistory = append(service.asyncHistory, task.record)
	if excess := len(service.asyncHistory) - service.asyncHistoryLimit; excess > 0 {
		copy(service.asyncHistory, service.asyncHistory[excess:])
		service.asyncHistory = service.asyncHistory[:service.asyncHistoryLimit]
	}
	service.wakeAsyncLocked()
	if task.release != nil {
		task.release(errors.Join(task.ownershipErr, service.asyncEvidenceErr))
		task.release = nil
	}
}

func (service *Service) takeAsync() *asyncTask {
	for {
		service.mu.Lock()
		if service.asyncAborted || service.asyncClosed && service.asyncOutstanding == 0 {
			service.mu.Unlock()
			return nil
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
				service.finishAsyncLocked(task, "failed", "EventAgeExceeded")
				service.mu.Unlock()
				continue
			}
			task.record.Attempts++
			task.record.NextAttemptAt = time.Time{}
			service.recordAsyncLocked(task, "running", "")
			service.mu.Unlock()
			return task
		}
		wake := service.asyncWake
		service.mu.Unlock()
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
		service.mu.Lock()
		task.ownershipErr = errors.Join(task.ownershipErr, result.ownershipErr)
		if result.ownershipErr != nil && service.asyncOwnershipErr == nil {
			service.asyncOwnershipErr = result.ownershipErr
		}
		switch {
		case service.asyncAborted || errors.Is(err, context.Canceled):
			service.finishAsyncLocked(task, "canceled", "ServiceShutdown")
		case err != nil:
			service.finishAsyncLocked(task, "failed", "ServiceError")
		case !result.functionError:
			service.finishAsyncLocked(task, "succeeded", "")
		case task.record.Attempts <= 2:
			task.readyAt = time.Now().Add(service.asyncRetryDelays[task.record.Attempts-1])
			task.record.NextAttemptAt = task.readyAt.UTC()
			service.recordAsyncLocked(task, "retrying", asyncErrorType(result))
			service.asyncQueue = append(service.asyncQueue, task)
			service.wakeAsyncLocked()
		default:
			service.finishAsyncLocked(task, "failed", asyncErrorType(result))
		}
		service.mu.Unlock()
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

// The caller owns mu. Synchronous invocations are intentionally independent;
// their caller contexts and final Close own cancellation.
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
	for _, task := range service.asyncQueue {
		service.finishAsyncLocked(task, "canceled", "ServiceShutdown")
	}
	service.asyncQueue = nil
	service.wakeAsyncLocked()
}
