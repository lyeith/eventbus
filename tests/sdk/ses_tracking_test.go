//go:build sdksmoke && (linux || darwin)

package sdk

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/app"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Exercise the actual CLI without a separate build. The native SDK scenario
// can also select an explicitly owned packaged binary for release acceptance.
func TestSESTrackingCLIProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_SES_TRACKING_CLI_CHILD") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		os.Exit(2)
	}
	os.Args = append([]string{"eventbus"}, os.Args[separator+1:]...)
	if err := app.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestSESTrackingPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	for _, enabled := range []bool{true, false} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			directory := t.TempDir()
			output := filepath.Join(directory, "observations.jsonl")
			capture := filepath.Join(directory, "ses.jsonl")
			functions := lambda.Config{Functions: map[string]lambda.Function{"ses-pixel:live": {
				Runtime: "python", Command: []string{python, "-E", "-s"},
				Handler: fixturePath("python", "ses_events_smoke.py") + "#handler", Timeout: 5 * time.Second,
				Environment: map[string]string{"SES_EVENTS_OUTPUT": output, "SES_EVENTS_GATE": filepath.Join(directory, "unused-bounce-gate")},
			}}, DevAsync: &lambda.DevAsyncConfig{Workers: 1, Capacity: 32, RetryDelays: []time.Duration{0, 0}, LogPath: filepath.Join(directory, "async.jsonl")}}
			recipe, err := yaml.Marshal(functions)
			require.NoError(t, err)
			functionsPath := filepath.Join(directory, "functions.yaml")
			require.NoError(t, os.WriteFile(functionsPath, recipe, 0600))
			script := filepath.Join(directory, "ses_tracking.py")
			require.NoError(t, os.WriteFile(script, []byte(sesTrackingSDKScenario), 0600))
			// Keep SES v1 management on the unchanged installed model. Only the
			// existing SESv2 compatibility model supplies its newer Tracking field.
			models := filepath.Join(directory, "models")
			modelDirectory := filepath.Join(models, "sesv2", "2019-09-27")
			require.NoError(t, os.MkdirAll(modelDirectory, 0700))
			model, err := os.ReadFile(fixturePath("aws_models", "sesv2", "2019-09-27", "service-2.json"))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(modelDirectory, "service-2.json"), model, 0600))
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			port := listener.Addr().(*net.TCPAddr).Port
			endpoint := "http://" + listener.Addr().String()
			require.NoError(t, listener.Close())
			args := []string{"--port", strconv.Itoa(port), "--work-dir", directory, "--issuer-base", endpoint,
				"--cognito-db", filepath.Join(directory, "cognito.db"), "--ses-log", capture,
				"--sns-log", filepath.Join(directory, "sns.jsonl"), "--cognito-log", filepath.Join(directory, "notifications.jsonl"),
				"--lambda-functions", functionsPath, "--s3-endpoint", "http://127.0.0.1:1"}
			if enabled {
				args = append(args, "--ses-tracking")
			}
			executable, err := os.Executable()
			require.NoError(t, err)
			command := exec.Command(executable, append([]string{"-test.run=^TestSESTrackingCLIProcess$", "--"}, args...)...)
			if binary := os.Getenv("EVENTBUS_SMOKE_BINARY"); binary != "" {
				require.True(t, filepath.IsAbs(binary), "EVENTBUS_SMOKE_BINARY must select an absolute owned executable")
				info, err := os.Stat(binary)
				require.NoError(t, err)
				require.False(t, info.IsDir())
				command = exec.Command(binary, args...)
				t.Logf("packaged SES tracking proof: %s", binary)
			}
			command.Dir = directory
			command.Env = append(sdkEnvironment(directory, endpoint, "", ""), "EVENTBUS_SES_TRACKING_CLI_CHILD=1")
			logPath := filepath.Join(directory, "cli.log")
			log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			require.NoError(t, err)
			t.Cleanup(func() { _ = log.Close() })
			command.Stdout, command.Stderr = log, log
			command.WaitDelay = time.Second
			require.NoError(t, command.Start())
			done := make(chan struct{})
			var waitErr error
			go func() { waitErr = command.Wait(); close(done) }()
			var stopOnce sync.Once
			stop := func() {
				stopOnce.Do(func() {
					_ = command.Process.Signal(syscall.SIGTERM)
					select {
					case <-done:
						if waitErr != nil {
							t.Errorf("the real CLI must join native handlers and captures: %v", waitErr)
						}
					case <-time.After(35 * time.Second):
						_ = command.Process.Kill()
						<-done
						t.Error("owned SES tracking CLI did not join graceful shutdown")
					}
				})
			}
			t.Cleanup(func() {
				stop()
				if t.Failed() {
					data, _ := os.ReadFile(logPath)
					t.Logf("owned SES tracking CLI output:\n%s", data)
				}
			})
			client := &http.Client{Timeout: time.Second}
			t.Cleanup(client.CloseIdleConnections)
			require.Eventually(t, func() bool {
				response, err := client.Get(endpoint + "/health")
				if err != nil {
					return false
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				return response.StatusCode == http.StatusOK
			}, 10*time.Second, 10*time.Millisecond, "owned CLI never reached readiness")
			environment := append(sdkEnvironment(directory, endpoint, "", ""),
				"SES_TRACKING_ENDPOINT="+endpoint, "SES_TRACKING_ENABLED="+strconv.FormatBool(enabled),
				"SES_TRACKING_OUTPUT="+output, "SES_TRACKING_CAPTURE="+capture, "AWS_DATA_PATH="+models)
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			logs, err := runSDKProcess(ctx, python, script, environment)
			t.Logf("real SES tracking flag=%t + SNS/native Python Lambda:\n%s", enabled, logs)
			require.NoError(t, err)
			stop()
		})
	}
}

const sesTrackingSDKScenario = `
import base64
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import os
from pathlib import Path
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.config import Config

endpoint = os.environ["SES_TRACKING_ENDPOINT"]
enabled = os.environ["SES_TRACKING_ENABLED"] == "true"
output = Path(os.environ["SES_TRACKING_OUTPUT"])
capture = Path(os.environ["SES_TRACKING_CAPTURE"])
options = dict(endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="owned-sdk",
               aws_secret_access_key="owned-sdk-secret",
               config=Config(connect_timeout=2, read_timeout=8, retries={"total_max_attempts": 1}))
ses, sesv2, sns = (boto3.client(service, **options) for service in ("ses", "sesv2", "sns"))
suffix = uuid.uuid4().hex[:12]
configuration, barrier_configuration, unrelated = (prefix + suffix for prefix in ("pixel-", "barrier-", "other-"))
topic = subscription = None
sets = []
expected_opens = {}
originals = {}


def records():
    if not output.exists():
        return []
    return [json.loads(line) for line in output.read_text().splitlines(keepends=True) if line.endswith("\n")]


def matches(message_id, kind):
    return [row for row in records() if row["phase"] == "success" and row["event"]["eventType"] == kind
            and row["event"]["mail"]["messageId"] == message_id]


def wait_event(message_id, kind, count=1):
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        found = matches(message_id, kind)
        if len(found) >= count:
            assert len(found) == count, ("duplicate native event", message_id, kind, found)
            return found
        time.sleep(0.02)
    raise AssertionError(("missing native event", message_id, kind, records()))


def destination(types=("send", "open"), active=True):
    return {"Name": "pixel", "Enabled": active, "MatchingEventTypes": list(types), "SNSDestination": {"TopicARN": topic}}


def update(types=("send", "open"), active=True):
    ses.update_configuration_set_event_destination(ConfigurationSetName=configuration, EventDestination=destination(types, active))


def send(html=None, selected=configuration):
    body = {"Text": {"Data": "native SDK SES tracking fixture"}} if html is None else {"Html": {"Data": html, "Charset": "UTF-8"}}
    result = ses.send_email(Source="sender@example.test", Destination={"ToAddresses": ["reader@example.test"]},
                            ConfigurationSetName=selected, Message={"Subject": {"Data": "pixel SDK proof"}, "Body": body})
    assert set(result) == {"MessageId", "ResponseMetadata"}, result
    return result["MessageId"]


def barrier():
    # One native worker processes the earlier pixel admissions before this Send;
    # a missing Open is never concluded from an arbitrary quiet interval.
    wait_event(send(selected=barrier_configuration), "Send")
    for message_id, count in expected_opens.items():
        assert len(matches(message_id, "Open")) == count, (message_id, count, records())


def http(path, method="GET", status=200, agent="EventBus tracking SDK reader"):
    request = urllib.request.Request(endpoint + path, method=method,
                                     headers={"User-Agent": agent, "X-Forwarded-For": "203.0.113.99"})
    try:
        response = urllib.request.urlopen(request, timeout=8)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        assert response.status == status, (path, method, response.status, body)
        return body, response.headers


def discover(message_id, status=200):
    body, headers = http("/__eventbus/dev/ses/messages/" + message_id, status=status)
    if status != 200:
        return None
    value = json.loads(body)
    assert set(value) == {"local", "message_id", "view_url", "tracking_url", "expires_at"}, value
    assert value["local"] is True and value["message_id"] == message_id, value
    for name in ("view_url", "tracking_url"):
        parsed = urllib.parse.urlsplit(value[name])
        assert value[name].startswith("/") and not parsed.scheme and not parsed.netloc, value
    expires = datetime.fromisoformat(value["expires_at"].replace("Z", "+00:00"))
    assert datetime.now(timezone.utc) < expires < datetime.now(timezone.utc) + timedelta(hours=25), value
    assert headers["Cache-Control"] == "no-store", headers
    return value


def view(metadata, snippet, charset="utf-8"):
    body, headers = http(metadata["view_url"])
    assert headers.get_content_type() == "text/html", headers
    assert headers.get_content_charset().lower() == charset, headers
    assert "sandbox" in headers["Content-Security-Policy"], headers
    assert headers["Cache-Control"] == "no-store" and headers["X-Content-Type-Options"] == "nosniff", headers
    assert headers["Referrer-Policy"] == "no-referrer", headers
    html = body.decode(charset)
    assert snippet in html and "{{ses:openTracker}}" not in html, html
    assert html.count(metadata["tracking_url"]) == 1, html
    return html


def pixel(metadata):
    message_id = metadata["message_id"]
    count = expected_opens.get(message_id, 0) + 1
    agent = "EventBus actual tracking SDK GET/" + str(count)
    before = datetime.now(timezone.utc)
    body, headers = http(metadata["tracking_url"], agent=agent)
    after = datetime.now(timezone.utc)
    assert headers.get_content_type() == "image/gif" and body[:6] in (b"GIF87a", b"GIF89a"), (headers, body)
    assert struct.unpack("<HH", body[6:10]) == (1, 1), body
    assert headers["Cache-Control"] == "no-store", headers
    row = wait_event(message_id, "Open", count)[-1]
    assert row["sns"]["TopicArn"] == topic and row["sns"]["MessageId"] != message_id, row
    assert row["aws_request_id"], row
    opened = row["event"]["open"]
    assert ipaddress.ip_address(opened["ipAddress"]).is_loopback and opened["ipAddress"] == "127.0.0.1", row
    assert opened["userAgent"] == agent, row
    stamp = datetime.fromisoformat(opened["timestamp"].replace("Z", "+00:00"))
    assert before - timedelta(seconds=1) <= stamp <= after + timedelta(seconds=1), row
    assert row["event"]["mail"] == wait_event(message_id, "Send")[0]["event"]["mail"], row
    expected_opens[message_id] = count
    return row


try:
    topic = sns.create_topic(Name="pixel-" + suffix)["TopicArn"]
    subscription = sns.subscribe(TopicArn=topic, Protocol="lambda",
                                 Endpoint="arn:aws:lambda:us-east-1:000000000000:function:ses-pixel:live")["SubscriptionArn"]
    for name in (configuration, barrier_configuration, unrelated):
        ses.create_configuration_set(ConfigurationSet={"Name": name})
        sets.append(name)
    ses.create_configuration_set_event_destination(ConfigurationSetName=configuration, EventDestination=destination())
    ses.create_configuration_set_event_destination(ConfigurationSetName=barrier_configuration, EventDestination=destination(("send",)))
    html = "<html><body><p>first message</p>{{ses:openTracker}}<p>after marker</p>{{ses:openTracker}}</body></html>"
    first = send(html)
    raw_html = "<html><body><p>Ol\u00e1 raw message</p></body></html>"
    raw = ("From: sender@example.test\r\nTo: reader@example.test\r\nSubject: raw pixel\r\n"
           "X-SES-CONFIGURATION-SET: " + configuration + "\r\nMIME-Version: 1.0\r\n"
           "Content-Type: multipart/alternative; boundary=pixel-boundary\r\n\r\n"
           "--pixel-boundary\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nplain alternative\r\n"
           "--pixel-boundary\r\nContent-Type: text/html; charset=iso-8859-1\r\nContent-Transfer-Encoding: base64\r\n\r\n"
           + base64.b64encode(raw_html.encode("iso-8859-1")).decode() + "\r\n--pixel-boundary--\r\n").encode()
    raw_id = ses.send_raw_email(RawMessage={"Data": raw})["MessageId"]
    v2_html = "<body><p>v2 message</p></body>"
    v2_id = sesv2.send_email(FromEmailAddress="sender@example.test", Destination={"ToAddresses": ["reader@example.test"]},
                            ConfigurationSetName=configuration, Content={"Simple": {"Subject": {"Data": "v2 pixel"},
                            "Body": {"Html": {"Data": v2_html}}}})["MessageId"]
    originals = {first: html, raw_id: raw, v2_id: v2_html}
    for message_id in originals:
        wait_event(message_id, "Send")
        expected_opens[message_id] = 0
    before_reads = capture.read_bytes()
    if not enabled:
        for message_id in originals:
            discover(message_id, 404)
        http("/__eventbus/dev/ses/tracking/unknown.gif", status=404)
        assert capture.read_bytes() == before_reads
        barrier()
        assert not [row for row in records() if row["event"]["eventType"] == "Open"], records()
    else:
        first_meta, raw_meta, v2_meta = (discover(message_id) for message_id in originals)
        assert len({item["tracking_url"] for item in (first_meta, raw_meta, v2_meta)}) == 3
        rendered = view(first_meta, "first message")
        assert rendered.index("first message") < rendered.index(first_meta["tracking_url"]) < rendered.index("after marker"), rendered
        raw_view = view(raw_meta, "Ol\u00e1 raw message", "iso-8859-1")
        assert raw_view.startswith(raw_html) and raw_view.index(raw_meta["tracking_url"]) > raw_view.index("</html>"), raw_view
        v2_view = view(v2_meta, "v2 message")
        assert v2_view.startswith(v2_html) and v2_view.index(v2_meta["tracking_url"]) > v2_view.index("</body>"), v2_view
        assert discover(first) == first_meta
        for path in (first_meta["view_url"], first_meta["tracking_url"], "/__eventbus/dev/ses/messages/" + first):
            http(path, method="HEAD", status=405)
        discover("never-accepted", 404)
        for path in ("/__eventbus/dev/ses/tracking/unknown.gif", "/__eventbus/dev/ses/tracking/malformed"):
            http(path, status=404)
        assert capture.read_bytes() == before_reads, "discovery/view/invalid requests changed native submission capture"
        barrier()
        assert not [row for row in records() if row["event"]["eventType"] == "Open"], records()
        opened_first = pixel(first_meta)
        pixel(raw_meta)
        pixel(v2_meta)
        resent = send(html)
        originals[resent] = html
        wait_event(resent, "Send")
        assert resent not in (first, raw_id, v2_id), resent
        resent_meta = discover(resent)
        assert resent_meta["tracking_url"] != first_meta["tracking_url"]
        pixel(resent_meta)
        older_open = pixel(first_meta)
        repeated_open = pixel(first_meta)
        assert older_open["event"]["mail"]["messageId"] == first and repeated_open["event"]["mail"]["messageId"] == first
        assert len({row["sns"]["MessageId"] for row in (opened_first, older_open, repeated_open)}) == 3
        http(first_meta["tracking_url"], method="HEAD", status=405)
        barrier()

        # Native current destination state gates the existing old pixel. New
        # untracked messages never gain a resource just because config changes.
        update(active=False)
        http(first_meta["tracking_url"])
        disabled_id = send(html)
        discover(disabled_id, 404)
        barrier()
        update(("send",))
        http(raw_meta["tracking_url"])
        nonmatching_id = send(html)
        discover(nonmatching_id, 404)
        barrier()
        update()
        discover(disabled_id, 404)
        discover(nonmatching_id, 404)
        unconfigured = send(html, selected=unrelated)
        discover(unconfigured, 404)
        text_id = send()
        discover(text_id, 404)
        suppressed_id = sesv2.send_email(FromEmailAddress="sender@example.test", Destination={"ToAddresses": ["reader@example.test"]},
            ConfigurationSetName=configuration, ConfigurationOverrides={"Tracking": {"OpenTrackingEnabled": "DISABLED"}},
            Content={"Simple": {"Subject": {"Data": "suppressed pixel"}, "Body": {"Html": {"Data": html}}}})["MessageId"]
        discover(suppressed_id, 404)
        barrier()
        ses.delete_configuration_set_event_destination(ConfigurationSetName=configuration, EventDestinationName="pixel")
        http(first_meta["tracking_url"])
        barrier()
        ses.delete_configuration_set(ConfigurationSetName=configuration)
        sets.remove(configuration)
        ses.create_configuration_set(ConfigurationSet={"Name": configuration})
        sets.append(configuration)
        ses.create_configuration_set_event_destination(ConfigurationSetName=configuration, EventDestination=destination())
        http(first_meta["tracking_url"])
        assert discover(first) == first_meta
        barrier()
        fresh = send(html)
        wait_event(fresh, "Send")
        pixel(discover(fresh))
        barrier()
        assert not matches(disabled_id, "Open") and not matches(nonmatching_id, "Open") and not matches(suppressed_id, "Open"), records()

    captured = [json.loads(line) for line in capture.read_text().splitlines() if line.strip()]
    by_id = {email["message_id"]: (record, email) for record in captured for email in record.get("emails") or [] if "message_id" in email}
    for message_id, original in originals.items():
        record, email = by_id[message_id]
        assert record["schema_version"] == 1, record
        if isinstance(original, bytes):
            assert base64.b64decode(record["request"]["RawMessage"]["Data"]) == original
            assert "ConfigurationSetName" not in record["request"], record
            assert email["configuration_set"] == configuration, email
        else:
            assert email["html"] == original, email
    assert "/__eventbus/dev/ses/tracking/" not in capture.read_text(), "native original capture contains rewritten HTML"
    barrier()
    all_opens = [row for row in records() if row["event"]["eventType"] == "Open"]
    assert len(all_opens) == sum(expected_opens.values()), (expected_opens, all_opens)
    print(json.dumps({"tracking_enabled": enabled, "status": "passed", "open_events": len(all_opens), "native_observations": len(records())}))
finally:
    if subscription is not None:
        sns.unsubscribe(SubscriptionArn=subscription)
    for name in reversed(sets):
        ses.delete_configuration_set(ConfigurationSetName=name)
    if topic is not None:
        sns.delete_topic(TopicArn=topic)
print("PASS")
`
