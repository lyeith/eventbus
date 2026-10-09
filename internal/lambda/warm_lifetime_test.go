package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type warmTestActivity struct {
	mu     sync.Mutex
	counts map[string]int
	errors []error
}

func (activity *warmTestActivity) BeginActivity(kind, id string) (func(error), error) {
	activity.mu.Lock()
	activity.counts[kind]++
	activity.mu.Unlock()
	return func(err error) {
		activity.mu.Lock()
		activity.counts[kind]--
		activity.errors = append(activity.errors, err)
		activity.mu.Unlock()
	}, nil
}
func (activity *warmTestActivity) count(kind string) int {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	return activity.counts[kind]
}

func TestWarmWorkerLeaseRetainedUntilDrainAndFreshSnapshotPolicy(t *testing.T) {
	directory := t.TempDir()
	function := warmTestFunction(t, "python", directory)
	activity := &warmTestActivity{counts: make(map[string]int)}
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevActivity: activity, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	first := warmTestExecute(t, service, `{}`)
	if activity.count("lambda_invoke") != 0 || activity.count("lambda_warm_worker") != 1 {
		t.Fatalf("successful invocation released worker lifetime: %+v", activity.counts)
	}
	if err := service.DevBeginWarmDrain(); err != nil {
		t.Fatal(err)
	}
	warmWaitRetired(t, service)
	if activity.count("lambda_warm_worker") != 0 {
		t.Fatal("retired worker lease retained")
	}
	fresh1 := warmTestExecute(t, service, `{}`)
	fresh2 := warmTestExecute(t, service, `{}`)
	if fresh1["pid"] == fresh2["pid"] || fresh1["pid"] == first["pid"] || fresh1["count"] != float64(1) || fresh2["count"] != float64(1) || activity.count("lambda_warm_worker") != 0 {
		t.Fatalf("draining requests retained state %v %v", fresh1, fresh2)
	}
	if err := service.DevResumeWarm(); err != nil {
		t.Fatal(err)
	}
	warmTestExecute(t, service, `{}`)
	if activity.count("lambda_warm_worker") != 1 {
		t.Fatal("resumed mode did not retain worker")
	}
}

func TestWarmReloadPreservesAlreadyAcceptedEventSnapshots(t *testing.T) {
	directory := t.TempDir()
	function := warmTestFunction(t, "python", directory)
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{Workers: 1, LogWriter: io.Discard}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	started, record := filepath.Join(directory, "accepted-start"), filepath.Join(directory, "results.jsonl")
	if _, err := service.Admit(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(fmt.Sprintf(`{"started":%q,"sleep":0.2,"record":%q}`, started, record))}); err != nil {
		t.Fatal(err)
	}
	warmWaitFile(t, started)
	if _, err := service.Admit(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(fmt.Sprintf(`{"record":%q}`, record))}); err != nil {
		t.Fatal(err)
	}
	function.Environment = map[string]string{"SETTING": "new"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := service.RegisterFunction(ctx, "warm", function); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("join should be bounded after publication: %v", err)
	}
	if err := service.DrainAsync(context.Background()); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(record)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var old []map[string]any
	for {
		var result map[string]any
		if err := decoder.Decode(&result); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		old = append(old, result)
	}
	if len(old) != 2 || old[0]["setting"] != "old" || old[1]["setting"] != "old" || old[1]["count"] != float64(1) || old[0]["pid"] == old[1]["pid"] {
		t.Fatalf("accepted work changed generation %v", old)
	}
	if body := warmTestExecute(t, service, `{}`); body["setting"] != "new" || body["count"] != float64(1) {
		t.Fatalf("new work wrong generation %v", body)
	}
}
