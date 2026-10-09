package lambda

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
)

func TestAsyncHistoryRetainsLatestCompletionAndDetachedOrderedSnapshot(t *testing.T) {
	var capture bytes.Buffer
	sink := devcapture.NewWriter(&capture, "history")
	defer sink.Close()
	service := &Service{asyncHistoryLimit: 3, asyncTasks: make(map[string]*asyncTask), asyncWake: make(chan struct{}), asyncCapture: sink}
	base := time.Unix(1700000000, 0).UTC()
	active := &asyncTask{record: AsyncRecord{RequestID: "active", QueuedAt: base, State: "running"}}
	service.asyncTasks["active"] = active
	service.asyncOutstanding = 1
	releases := 0
	for index := 0; index < 11; index++ {
		record := AsyncRecord{RequestID: fmt.Sprintf("completed-%02d", index), QueuedAt: base.Add(time.Duration(11-index) * time.Second), State: "running"}
		task := &asyncTask{record: record, release: func(err error) {
			if err != nil {
				t.Fatal(err)
			}
			releases++
		}}
		service.asyncTransitionMu.Lock()
		service.mu.Lock()
		service.asyncTasks[record.RequestID] = task
		service.asyncOutstanding++
		service.mu.Unlock()
		service.finishAsync(task, "succeeded", "")
		service.asyncTransitionMu.Unlock()
		if task.release != nil {
			t.Fatal("terminal owner not released")
		}
	}
	records := service.AsyncSnapshot()
	var ids []string
	for _, record := range records {
		ids = append(ids, record.RequestID)
	}
	if !reflect.DeepEqual(ids, []string{"active", "completed-10", "completed-09", "completed-08"}) {
		t.Fatalf("retention / queued ordering: %v", ids)
	}
	if releases != 11 || service.asyncOutstanding != 1 || len(service.asyncHistory) != 3 {
		t.Fatalf("terminal ownership releases=%d outstanding=%d history=%d", releases, service.asyncOutstanding, len(service.asyncHistory))
	}
	records[0].State = "changed"
	records[1].RequestID = "changed"
	if reflect.DeepEqual(records, service.AsyncSnapshot()) || active.record.State != "running" {
		t.Fatal("snapshot exposes shared record state")
	}
	decoder := json.NewDecoder(&capture)
	seen := 0
	for {
		var record AsyncRecord
		if err := decoder.Decode(&record); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if record.State != "succeeded" || record.CompletedAt.IsZero() {
			t.Fatalf("terminal evidence: %+v", record)
		}
		seen++
	}
	if seen != 11 {
		t.Fatalf("capture retains all completed transitions: %d", seen)
	}
}

func TestAsyncHistoryConcurrentSnapshots(t *testing.T) {
	service := &Service{asyncHistoryLimit: 3, asyncTasks: make(map[string]*asyncTask)}
	var joined sync.WaitGroup
	joined.Add(2)
	go func() {
		defer joined.Done()
		for index := 0; index < 1000; index++ {
			service.mu.Lock()
			service.appendAsyncHistoryLocked(AsyncRecord{RequestID: fmt.Sprint(index), QueuedAt: time.Unix(int64(index), 0)})
			service.mu.Unlock()
		}
	}()
	go func() {
		defer joined.Done()
		for index := 0; index < 1000; index++ {
			records := service.AsyncSnapshot()
			for previous := 0; previous+1 < len(records); previous++ {
				if records[previous+1].QueuedAt.Before(records[previous].QueuedAt) {
					t.Error("snapshot order")
				}
			}
		}
	}()
	joined.Wait()
	if len(service.AsyncSnapshot()) != 3 {
		t.Fatal("concurrent history bound")
	}
}
