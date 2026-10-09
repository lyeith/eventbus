//go:build performance && linux

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/ses"
	"github.com/lyeith/eventbus/internal/testperf"
)

// Real installed SDK imports, module-owned clients and native SES capture are
// identical in both modes. No artificial initialization sleep or speed assertion.
const performanceWarmPython = `import boto3, json, os
count = 0
client = boto3.client('sesv2', endpoint_url=os.environ['SES_ENDPOINT'], region_name='us-east-1', aws_access_key_id='fixture', aws_secret_access_key='fixture')
def handler(event, context):
    global count
    count += 1
    reply = client.send_email(FromEmailAddress='sender@eventbus.test', Destination={'ToAddresses':['recipient@eventbus.test']}, Content={'Simple':{'Subject':{'Data':'warm performance'},'Body':{'Text':{'Data':json.dumps({'id':context.aws_request_id,'auth':event['auth']})}}}})
    with open('/proc/self/status') as f:
        rss = next(int(line.split()[1])*1024 for line in f if line.startswith('VmRSS:'))
    return {'pid':os.getpid(),'rss':rss,'count':count,'id':context.aws_request_id,'message_id':reply['MessageId']}
`
const performanceWarmNode = `import fs from 'node:fs'; import {createRequire} from 'node:module';
const require=createRequire(process.env.SDK_PACKAGE);
const {SESv2Client,SendEmailCommand}=require('@aws-sdk/client-sesv2');
let count=0;
const client=new SESv2Client({endpoint:process.env.SES_ENDPOINT,region:'us-east-1',credentials:{accessKeyId:'fixture',secretAccessKey:'fixture'},maxAttempts:1});
export async function handler(event,context) {
  count++;
  const reply=await client.send(new SendEmailCommand({FromEmailAddress:'sender@eventbus.test',Destination:{ToAddresses:['recipient@eventbus.test']},Content:{Simple:{Subject:{Data:'warm performance'},Body:{Text:{Data:JSON.stringify({id:context.awsRequestId,auth:event.auth})}}}}}));
  const rss=Number(fs.readFileSync('/proc/self/status','utf8').split('\n').find(line=>line.startsWith('VmRSS:')).trim().split(/\s+/)[1])*1024;
  return {pid:process.pid,rss,count,id:context.awsRequestId,message_id:reply.MessageId};
}
`

func TestPerformanceWarmNativeSDKCalls(t *testing.T) {
	const samples = 8
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			var capture bytes.Buffer
			manager := ses.NewSESManager(ses.SESFixtures{}, ses.NewSESCapture(&capture))
			server := httptest.NewServer(ses.NewHandler(manager))
			defer server.Close()
			defer manager.Close()
			directory := t.TempDir()
			source, filename := performanceWarmNode, "sdk.mjs"
			command := []string{"node"}
			if runtime == "python" {
				source, filename = performanceWarmPython, "sdk.py"
				command = []string{performanceReviewInterpreter(t)}
			} else if _, err := exec.LookPath("node"); err != nil {
				t.Skip(err)
			}
			packagePath := filepath.Join(root, "tests/sdk/javascript/package.json")
			if runtime == "node" {
				if _, err := os.Stat(filepath.Join(filepath.Dir(packagePath), "node_modules/@aws-sdk/client-sesv2/package.json")); err != nil {
					t.Skip("install existing pinned JavaScript SDK lane first")
				}
			}
			if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			identities := make(map[string]bool)
			for _, mode := range []string{"fresh", "warm"} {
				t.Run(mode, func(t *testing.T) {
					config := &Config{Functions: map[string]Function{"sdk": {Runtime: runtime, Command: command, Handler: filename + "#handler", Environment: map[string]string{"SES_ENDPOINT": server.URL, "SDK_PACKAGE": packagePath}, Timeout: 5 * time.Second}}, DevAsync: &DevAsyncConfig{Workers: 1, LogWriter: io.Discard}, DevDiagnostics: &DevDiagnosticsConfig{LogPath: filepath.Join(directory, mode+".jsonl")}}
					if mode == "warm" {
						config.DevWarm = &DevWarmConfig{MaxWorkers: 1}
					}
					service, err := NewService(config, directory)
					if err != nil {
						t.Fatal(err)
					}
					defer service.Close(context.Background())
					var wall, rss, idle []float64
					pids := make(map[int]bool)
					for index := 0; index < samples; index++ {
						started := time.Now()
						out, err := service.Execute(context.Background(), InvokeInput{FunctionName: "sdk", Payload: []byte(fmt.Sprintf(`{"auth":%q}`, fmt.Sprintf("fixture-%s-%d", mode, index)))})
						elapsed := time.Since(started)
						if err != nil || out.FunctionError {
							t.Fatalf("native SDK call %s %v", out.Payload, err)
						}
						var reply struct {
							PID       int    `json:"pid"`
							RSS       int64  `json:"rss"`
							Count     int    `json:"count"`
							ID        string `json:"id"`
							MessageID string `json:"message_id"`
						}
						if err := json.Unmarshal(out.Payload, &reply); err != nil {
							t.Fatal(err)
						}
						if reply.ID != out.RequestID || reply.MessageID == "" || identities[reply.ID] {
							t.Fatalf("native effect identity %+v", reply)
						}
						identities[reply.ID] = true
						want := 1
						if mode == "warm" {
							want = index + 1
						}
						if reply.Count != want {
							t.Fatalf("module reuse count %d want%d", reply.Count, want)
						}
						pids[reply.PID] = true
						wall = append(wall, float64(elapsed)/float64(time.Millisecond))
						rss = append(rss, float64(reply.RSS))
						retained := float64(0)
						if mode == "warm" {
							status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", reply.PID))
							if err != nil {
								t.Fatal(err)
							}
							for _, line := range strings.Split(string(status), "\n") {
								if strings.HasPrefix(line, "VmRSS:") {
									kib, err := strconv.ParseInt(strings.Fields(line)[1], 10, 64)
									if err != nil {
										t.Fatal(err)
									}
									retained = float64(kib * 1024)
								}
							}
						}
						idle = append(idle, retained)
						t.Logf("WARM_NATIVE_SAMPLE runtime=%s mode=%s index=%d wall_ms=%.3f pid=%d handler_rss=%d retained_idle_rss=%.0f", runtime, mode, index, float64(elapsed)/float64(time.Millisecond), reply.PID, reply.RSS, retained)
					}
					if mode == "warm" && len(pids) != 1 || mode == "fresh" && len(pids) != samples {
						t.Fatalf("process reuse count %d mode%s", len(pids), mode)
					}
					if err := service.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					file, err := os.Open(filepath.Join(directory, mode+".jsonl"))
					if err != nil {
						t.Fatal(err)
					}
					defer file.Close()
					var init, invoke []float64
					decoder := json.NewDecoder(file)
					for {
						var record invocationDiagnosticRecord
						if err := decoder.Decode(&record); err == io.EOF {
							break
						} else if err != nil {
							t.Fatal(err)
						}
						if !record.OwnershipConfirmed || len(record.ExecutionPhases) != 1 {
							t.Fatalf("phase evidence %+v", record)
						}
						init = append(init, record.ExecutionPhases[0].InitMS)
						invoke = append(invoke, record.ExecutionPhases[0].InvokeMS)
					}
					if len(init) != samples {
						t.Fatalf("phase count %d", len(init))
					}
					for _, metric := range []struct {
						name   string
						values []float64
					}{{"joined_wall_ms", wall}, {"init_ms", init}, {"invoke_ms", invoke}, {"handler_rss_bytes", rss}, {"retained_idle_rss_bytes", idle}} {
						testperf.Report(t, runtime+"_"+mode, metric.name, metric.values)
					}
				})
			}
			decoder := json.NewDecoder(bytes.NewReader(capture.Bytes()))
			count := 0
			for {
				var record map[string]any
				if err := decoder.Decode(&record); err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				count++
				if record["operation"] != "SendEmail" || record["outcome"].(map[string]any)["http_status"] != float64(200) {
					t.Fatalf("native SES outcome %v", record)
				}
			}
			if count != samples*2 {
				t.Fatalf("native SES capture count%d", count)
			}
		})
	}
}
