//go:build linux || darwin

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/lyeith/eventbus/internal/localexec"
)

// A real child process implements either the command response channel or native
// Runtime API. It records writes before blocking/exiting, without emulator hooks.
func TestWorkerCrashProcess(t *testing.T) {
	mode := os.Getenv("EVENTBUS_CRASH_FIXTURE")
	if mode == "" {
		return
	}
	var payload []byte
	id := os.Getenv("EVENTBUS_LAMBDA_REQUEST_ID")
	base := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + runtimePrefix
	if mode == "provided" {
		response, err := http.Get(base + "invocation/next")
		if err != nil {
			os.Exit(9)
		}
		payload, _ = io.ReadAll(response.Body)
		_ = response.Body.Close()
		id = response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	} else {
		payload, _ = io.ReadAll(os.Stdin)
	}
	var event struct {
		Label      string
		Wait, Exit bool
	}
	_ = json.Unmarshal(payload, &event)
	path := filepath.Join(os.Getenv("DIRECTORY"), event.Label)
	_ = os.WriteFile(path+"-started", []byte(strconv.Itoa(os.Getpid())), 0600)
	if event.Exit {
		os.Exit(23)
	}
	for event.Wait {
		if _, err := os.Stat(path + "-release"); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	_ = os.WriteFile(path+"-done", []byte(id), 0600)
	if mode == "provided" {
		response, err := http.Post(base+"invocation/"+id+"/response", "application/json", bytes.NewReader([]byte("{}")))
		if err != nil {
			os.Exit(10)
		}
		_ = response.Body.Close()
	} else {
		fmt.Print("{}")
	}
	os.Exit(0)
}

func crashLifecycleFunction(t *testing.T, runtime, directory string) Function {
	t.Helper()
	function := Function{Runtime: runtime, Timeout: 5 * time.Second, Environment: map[string]string{"DIRECTORY": directory}}
	if runtime == "command" || runtime == "provided" {
		command, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		function.Command = []string{command, "-test.run=^TestWorkerCrashProcess$"}
		function.Environment["EVENTBUS_CRASH_FIXTURE"] = runtime
		return function
	}
	command := "node"
	filename := "crash.mjs"
	source := `import fs from 'node:fs'; export async function handler(event,context) {
 const path=process.env.DIRECTORY+'/'+event.Label;
 fs.writeFileSync(path+'-started',String(process.pid));
 if(event.Exit) process.exit(23);
 while(event.Wait && !fs.existsSync(path+'-release')) await new Promise(r=>setTimeout(r,2));
 fs.writeFileSync(path+'-done',context.awsRequestId);
 return {ok:true};
}`
	if runtime == "python" {
		command = "python3"
		filename = "crash.py"
		source = `import os,time
def handler(event,context):
 path=os.environ['DIRECTORY']+'/'+event['Label']
 with open(path+'-started','w') as f: f.write(str(os.getpid()))
 if event.get('Exit'): os._exit(23)
 while event.get('Wait') and not os.path.exists(path+'-release'): time.sleep(.002)
 with open(path+'-done','w') as f: f.write(context.aws_request_id)
 return {'ok':True}
`
	}
	executable, err := exec.LookPath(command)
	if err != nil {
		t.Skipf("%s unavailable", command)
	}
	function.Command = []string{executable}
	function.Handler = filename + "#handler"
	if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	return function
}

type crashLifecycleResult struct {
	outcome InvocationOutcome
	err     error
}

func crashObservedCall(service *Service, label string, wait, exit bool) (<-chan InvocationMetadata, <-chan crashLifecycleResult) {
	metadata := make(chan InvocationMetadata, 1)
	done := make(chan crashLifecycleResult, 1)
	body, _ := json.Marshal(map[string]any{"Label": label, "Wait": wait, "Exit": exit})
	go func() {
		outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "processor:live", Payload: body}, func(identity InvocationMetadata) error { metadata <- identity; return nil })
		done <- crashLifecycleResult{outcome, err}
	}()
	return metadata, done
}
func crashAwait(t *testing.T, done <-chan crashLifecycleResult) crashLifecycleResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("actual runtime did not join")
	}
	return crashLifecycleResult{}
}
func crashStartedPID(t *testing.T, directory, label string) int {
	t.Helper()
	filename := filepath.Join(directory, label+"-started")
	warmWaitFile(t, filename)
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}
func crashLifecycleService(t *testing.T, runtime string, warm bool) (*Service, string, string) {
	t.Helper()
	directory := t.TempDir()
	function := crashLifecycleFunction(t, runtime, directory)
	path := filepath.Join(directory, "diagnostics.jsonl")
	config := &Config{Functions: map[string]Function{"processor:live": function}, DevWorkerCrash: &DevWorkerCrashConfig{Enabled: true}, DevDiagnostics: &DevDiagnosticsConfig{LogPath: path}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}
	if warm {
		config.DevWarm = &DevWarmConfig{MaxWorkers: 2, MaxConcurrency: 2}
	}
	service, err := NewService(config, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return service, directory, path
}
func TestWorkerCrashActualRuntimeSelectionAndOverlap(t *testing.T) {
	for _, scenario := range []struct {
		runtime string
		warm    bool
	}{{"command", false}, {"provided", false}, {"node", false}, {"python", false}, {"node", true}, {"python", true}} {
		t.Run(fmt.Sprintf("%s-warm%v", scenario.runtime, scenario.warm), func(t *testing.T) {
			service, directory, path := crashLifecycleService(t, scenario.runtime, scenario.warm)
			if scenario.warm {
				_, done := crashObservedCall(service, "prime", false, false)
				if result := crashAwait(t, done); result.err != nil || result.outcome.Output.FunctionError {
					t.Fatal(result)
				}
			}
			selectedIdentity, selectedDone := crashObservedCall(service, "selected", true, false)
			selected := <-selectedIdentity
			pid := crashStartedPID(t, directory, "selected")
			_, peerDone := crashObservedCall(service, "peer", true, false)
			peerPID := crashStartedPID(t, directory, "peer")
			if pid == peerPID {
				t.Fatal("busy runtime shared with peer")
			}
			if scenario.warm && pid != crashStartedPID(t, directory, "prime") {
				t.Fatal("selected worker did not exercise retained reuse")
			}
			control, err := service.DevArmWorkerCrash(context.Background(), selected.FunctionName, selected.RequestID, selected.Attempt)
			if err != nil || control.Armed || !control.Fired {
				t.Fatalf("active select:%+v %v", control, err)
			}
			result := crashAwait(t, selectedDone)
			if result.err != nil || result.outcome.State != InvocationFailed || !result.outcome.Output.FunctionError || result.outcome.OwnershipErr != nil || result.outcome.CompletionScope != CompletionProcess {
				t.Fatalf("crashed result:%+v", result)
			}
			var failure struct {
				Type string `json:"errorType"`
			}
			_ = json.Unmarshal(result.outcome.Output.Payload, &failure)
			if failure.Type != "Runtime.ExitError" || processAlive(pid) {
				t.Fatalf("native death:%s alive%v", result.outcome.Output.Payload, processAlive(pid))
			}
			if _, err := os.Stat(filepath.Join(directory, "selected-done")); !os.IsNotExist(err) {
				t.Fatal("killed handler completed")
			}
			if _, err := os.Stat(filepath.Join(directory, "selected-started")); err != nil {
				t.Fatal("prior external write lost")
			}
			if !processAlive(peerPID) {
				t.Fatal("unrelated worker killed")
			}
			if err := os.WriteFile(filepath.Join(directory, "peer-release"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			peer := crashAwait(t, peerDone)
			if peer.err != nil || peer.outcome.Output.FunctionError {
				t.Fatalf("overlapping native call:%+v", peer)
			}
			var injected *invocationDiagnosticRecord
			for _, record := range readDiagnosticRecords(t, path) {
				if record.RequestID == selected.RequestID {
					copy := record
					injected = &copy
				}
				if record.RequestID == peer.outcome.Metadata.RequestID && record.InjectedWorkerCrash {
					t.Fatal("peer labeled as injected")
				}
			}
			if injected == nil || !injected.InjectedWorkerCrash || injected.TerminationCause != "injected_worker_crash" || !injected.OwnershipConfirmed || injected.State != InvocationFailed {
				t.Fatalf("crash diagnostic:%+v", injected)
			}
			if err := service.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if processAlive(peerPID) {
				t.Fatal("peer ownership not joined")
			}
		})
	}
}

func TestWorkerCrashNaturalExitFencesBeforeHeldCleanup(t *testing.T) {
	for _, scenario := range []struct {
		runtime string
		warm    bool
	}{{"provided", false}, {"node", true}, {"python", true}} {
		t.Run(scenario.runtime, func(t *testing.T) {
			service, directory, path := crashLifecycleService(t, scenario.runtime, scenario.warm)
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(proceed) })
			service.processCleanup = func(command *exec.Cmd) error {
				err := localexec.Cleanup(command)
				close(entered)
				<-proceed
				return err
			}
			identity, done := crashObservedCall(service, "natural", false, true)
			selected := <-identity
			pid := crashStartedPID(t, directory, "natural")
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("natural exit cleanup not reached")
			}
			if processAlive(pid) {
				t.Fatal("fixture did not reach actual process exit")
			}
			_, err := service.DevArmWorkerCrash(context.Background(), selected.FunctionName, selected.RequestID, selected.Attempt)
			requireWorkerCrashError(t, err, 409)
			if len(service.DevWorkerInvocations().Invocations) != 0 {
				t.Fatal("dead runtime still advertised")
			}
			once.Do(func() { close(proceed) })
			result := crashAwait(t, done)
			if result.err != nil || result.outcome.State != InvocationFailed || result.outcome.OwnershipErr != nil {
				t.Fatalf("natural exit:%+v", result)
			}
			for _, record := range readDiagnosticRecords(t, path) {
				if record.InjectedWorkerCrash || record.TerminationCause == "injected_worker_crash" {
					t.Fatalf("natural exit labeled injected:%+v", record)
				}
			}
		})
	}
}

func TestWorkerCrashPendingBeforeLaunchStillStartsRealRuntime(t *testing.T) {
	for _, scenario := range []struct {
		runtime string
		warm    bool
	}{{"command", false}, {"provided", false}, {"node", false}, {"python", false}, {"node", true}, {"python", true}} {
		t.Run(fmt.Sprintf("%s-warm%v", scenario.runtime, scenario.warm), func(t *testing.T) {
			service, directory, _ := crashLifecycleService(t, scenario.runtime, scenario.warm)
			var cleanedPID int
			service.processCleanup = func(command *exec.Cmd) error {
				cleanedPID = command.Process.Pid
				return localexec.Cleanup(command)
			}
			if scenario.warm {
				_, done := crashObservedCall(service, "prime", false, false)
				if result := crashAwait(t, done); result.err != nil || result.outcome.Output.FunctionError {
					t.Fatal(result)
				}
			}
			outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "processor:live", Payload: []byte("{\"Label\":\"armed\",\"Wait\":true}")}, func(identity InvocationMetadata) error {
				control, err := service.DevArmWorkerCrash(context.Background(), identity.FunctionName, identity.RequestID, identity.Attempt)
				if err == nil && (!control.Armed || control.Fired) {
					return fmt.Errorf("prelaunch control %+v", control)
				}
				return err
			})
			if err != nil || !outcome.Output.FunctionError || outcome.State != InvocationFailed || outcome.OwnershipErr != nil {
				t.Fatalf("prelaunch result:%+v %v", outcome, err)
			}
			if cleanedPID <= 0 || processAlive(cleanedPID) {
				t.Fatalf("prelaunch arm skipped real owned launch:pid%d", cleanedPID)
			}
			if scenario.warm && cleanedPID != crashStartedPID(t, directory, "prime") {
				t.Fatal("warm pending selection killed other retained worker")
			}
		})
	}
}

type heldCrashDiagnosticWriter struct {
	entered chan struct{}
	proceed chan struct{}
	once    sync.Once
	mu      sync.Mutex
	records []invocationDiagnosticRecord
}

func (writer *heldCrashDiagnosticWriter) Write(data []byte) (int, error) {
	var record invocationDiagnosticRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return 0, err
	}
	if record.InvocationType == "Event" && record.State == InvocationNotStarted {
		writer.once.Do(func() { close(writer.entered); <-writer.proceed })
	}
	writer.mu.Lock()
	writer.records = append(writer.records, record)
	writer.mu.Unlock()
	return len(data), nil
}
func TestWorkerCrashPendingClearDuringHeldNonlaunchDiagnostics(t *testing.T) {
	service, directory, _ := crashLifecycleService(t, "node", true)
	if _, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{1, 1}); err != nil {
		t.Fatal(err)
	}
	writer := &heldCrashDiagnosticWriter{entered: make(chan struct{}), proceed: make(chan struct{})}
	var once sync.Once
	defer once.Do(func() { close(writer.proceed) })
	if err := service.diagnosticCapture.Close(); err != nil {
		t.Fatal(err)
	}
	service.diagnosticCapture = devcapture.NewWriter(writer, "held crash diagnostics")
	_, activeDone := crashObservedCall(service, "held", true, false)
	pid := crashStartedPID(t, directory, "held")
	admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "processor:live", Payload: []byte("{\"Label\":\"queued\",\"Wait\":false}")})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native unlaunched throttle diagnostics not held")
	}
	control, err := service.DevArmWorkerCrash(context.Background(), "processor:live", admission.RequestID, 1)
	if err != nil || !control.Armed || control.Fired {
		t.Fatalf("deferred task arm:%+v %v", control, err)
	}
	for index := 0; index < 2; index++ {
		control, err = service.DevClearWorkerCrash(context.Background(), "processor:live", admission.RequestID, 1)
		if err != nil || control.Armed || control.Fired {
			t.Fatalf("pending clear through joined diagnostics:%+v %v", control, err)
		}
	}
	if err := os.WriteFile(filepath.Join(directory, "held-release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(writer.proceed) })
	record := waitAsyncState(t, service, admission.RequestID, "succeeded")
	if record.Attempts != 1 || record.InjectedWorkerCrash {
		t.Fatalf("cleared fault changed native retry:%+v", record)
	}
	if result := crashAwait(t, activeDone); result.err != nil || result.outcome.Output.FunctionError {
		t.Fatalf("peer result:%+v", result)
	}
	if crashStartedPID(t, directory, "queued") != pid || !processAlive(pid) {
		t.Fatal("cleared arm killed retained environment")
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	for _, record := range writer.records {
		if record.InjectedWorkerCrash || record.TerminationCause == "injected_worker_crash" {
			t.Fatal("pending clear left an injected failure")
		}
	}
}
