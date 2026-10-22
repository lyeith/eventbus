package lambda

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func drainSummaryOwner(tasks int, reservations int) *Service {
	service := &Service{
		asyncTasks: make(map[string]*asyncTask), asyncOutstanding: tasks + reservations,
		asyncWake: make(chan struct{}), asyncCancel: func(error) {},
	}
	nextAttemptAt := time.Now().UTC().Add(2 * time.Minute)
	for index := 0; index < tasks; index++ {
		id := fmt.Sprintf("%08d-0000-4000-8000-%012d", index, index)
		service.asyncTasks[id] = &asyncTask{record: AsyncRecord{
			RequestID: id, FunctionName: "processor:live", State: "retrying", Attempts: 2,
			NextAttemptAt: nextAttemptAt.Add(time.Duration(index) * time.Second),
		}}
	}
	return service
}
func abortForSummary(service *Service, reason error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.abortAsyncLocked(reason)
}

func TestDevAsyncDrainSummaryCapturesBeforeAbortAndRemainsDetached(t *testing.T) {
	service := drainSummaryOwner(2, 1)
	var retry *asyncTask
	for _, task := range service.asyncTasks {
		retry = task
		break
	}
	wantRetry := retry.record.NextAttemptAt
	retry.input.payload = []byte("private request payload")
	retry.entry.environment = map[string]string{"PRIVATE_TOKEN": "private environment value"}
	retry.record.ErrorType = "private handler error detail"
	retry.ownershipErr = errors.New("private ownership error detail")
	asyncCanceled, syncCanceled := 0, 0
	service.active = map[uint64]invocationOwner{
		1: {asynchronous: true, cancel: func() { asyncCanceled++ }},
		2: {asynchronous: false, cancel: func() { syncCanceled++ }},
	}
	capturedBeforeCancel := false
	service.asyncCancel = func(error) {
		capturedBeforeCancel = service.devAsyncDrainSummary != nil
		// The actual abort next causes native terminal transitions to clear
		// future retry metadata. The frozen evidence must retain the old value.
		retry.record.NextAttemptAt = time.Time{}
		retry.record.State = "canceled"
	}
	if got := service.DevAsyncDrainSummary(); !got.CapturedAt.IsZero() || got.Outstanding != 0 {
		t.Fatalf("unaborted owner invented evidence:%+v", got)
	}
	reason := errors.New("first native abort")
	abortForSummary(service, reason)
	summary := service.DevAsyncDrainSummary()
	if !capturedBeforeCancel || asyncCanceled != 1 || syncCanceled != 0 {
		t.Fatal("snapshot/cancel order or native sync isolation changed")
	}
	if summary.CapturedAt.IsZero() || summary.Outstanding != 3 || summary.PendingAdmissions != 1 || summary.Shown != 2 || summary.Omitted != 0 {
		t.Fatalf("summary totals:%+v", summary)
	}
	found := false
	for _, entry := range summary.Invocations {
		if entry.RequestID == retry.record.RequestID {
			found = true
			if entry.State != "retrying" || entry.Attempts != 2 || !entry.NextAttemptAt.Equal(wantRetry) {
				t.Fatalf("abort destroyed retry metadata:%+v", entry)
			}
		}
	}
	if !found {
		t.Fatal("native published identity omitted")
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private request payload", "PRIVATE_TOKEN", "private environment value", "private handler error detail", "private ownership error detail", "payload", "environment", "ownership_error", "error_type"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("summary leaked private field/detail:%s", encoded)
		}
	}
	summary.Invocations[0].FunctionName = "mutated getter"
	delete(service.asyncTasks, retry.record.RequestID)
	repeated := service.DevAsyncDrainSummary()
	if repeated.Invocations[0].FunctionName != "processor:live" || repeated.CapturedAt != summary.CapturedAt || len(repeated.Invocations) != 2 {
		t.Fatalf("snapshot aliases mutable state:%+v", repeated)
	}
	abortForSummary(service, errors.New("later abort"))
	if !errors.Is(service.asyncAbortErr, reason) || service.DevAsyncDrainSummary().CapturedAt != repeated.CapturedAt || asyncCanceled != 1 {
		t.Fatal("repeated abort replaced original error/evidence")
	}
}

func TestDevAsyncDrainSummaryIsBoundedOrderedAndCountsUnpublishedAdmissions(t *testing.T) {
	service := drainSummaryOwner(35, 3)
	abortForSummary(service, errors.New("abort"))
	summary := service.DevAsyncDrainSummary()
	if summary.Outstanding != 38 || summary.PendingAdmissions != 3 || summary.Shown != 16 || summary.Omitted != 19 || len(summary.Invocations) != 16 {
		t.Fatalf("bounded counts:%+v", summary)
	}
	if summary.Outstanding != summary.PendingAdmissions+summary.Shown+summary.Omitted {
		t.Fatal("summary cannot account for owned reservations")
	}
	for index, entry := range summary.Invocations {
		want := fmt.Sprintf("%08d-0000-4000-8000-%012d", index, index)
		if entry.RequestID != want {
			t.Fatalf("unstable selection index%d got%s want%s", index, entry.RequestID, want)
		}
	}
	if cap(service.devAsyncDrainSummary.Invocations) != 16 {
		t.Fatal("stored summary retained unbounded backing entries")
	}
	service = drainSummaryOwner(0, 2)
	abortForSummary(service, errors.New("abort"))
	summary = service.DevAsyncDrainSummary()
	if summary.Outstanding != 2 || summary.PendingAdmissions != 2 || summary.Shown != 0 || summary.Omitted != 0 || len(summary.Invocations) != 0 {
		t.Fatalf("unpublished reservations invented identities:%+v", summary)
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "\"invocations\":[]") || strings.Contains(string(encoded), "request_id") {
		t.Fatalf("reservation-only evidence:%s", encoded)
	}
	var absent *Service
	if got := absent.DevAsyncDrainSummary(); !got.CapturedAt.IsZero() {
		t.Fatal("nil owner invented evidence")
	}
}

func TestDevAsyncDrainSummaryOmitsUnknownRetryTime(t *testing.T) {
	service := drainSummaryOwner(1, 0)
	for _, task := range service.asyncTasks {
		task.record.State = "running"
		task.record.Attempts = 1
		task.record.NextAttemptAt = time.Time{}
	}
	abortForSummary(service, errors.New("abort"))
	encoded, err := json.Marshal(service.DevAsyncDrainSummary())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "next_attempt_at") || !strings.Contains(string(encoded), "\"state\":\"running\"") {
		t.Fatalf("invented active retry timestamp:%s", encoded)
	}
}
