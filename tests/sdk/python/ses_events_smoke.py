#!/usr/bin/env python3
"""Real SES/SNS provisioning, sends and explicit outcomes through native Lambda."""
from __future__ import annotations

import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request
import uuid


def handler(event, context):
    """Generic native SNS observer; no consuming application's state or policy."""
    output = Path(os.environ["SES_EVENTS_OUTPUT"])
    gate = Path(os.environ["SES_EVENTS_GATE"])
    for record in event["Records"]:
        assert record["EventSource"] == "aws:sns", record
        envelope = record["Sns"]
        assert envelope["Type"] == "Notification", envelope
        message = json.loads(envelope["Message"])
        observed = {"phase": "success", "sns": envelope, "event": message,
                    "aws_request_id": context.aws_request_id}
        # One actual handler failure exercises SNS's existing retry owner. The
        # marker is owned by this fixture, not an application retry substitute.
        if message["eventType"] == "Bounce":
            try:
                descriptor = os.open(gate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                pass
            else:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    stream.write(message["mail"]["messageId"])
                observed["phase"] = "failure"
                with output.open("a", encoding="utf-8") as stream:
                    stream.write(json.dumps(observed, separators=(",", ":")) + "\n")
                raise RuntimeError("owned SES/SNS retry proof")
        with output.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(observed, separators=(",", ":")) + "\n")
    return {"observed": True}


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError

    endpoint = os.environ["SES_EVENTS_ENDPOINT"]
    output = Path(os.environ["SES_EVENTS_OUTPUT"])
    capture = Path(os.environ["SES_EVENTS_CAPTURE"])
    function_arn = os.environ["SES_EVENTS_FUNCTION_ARN"]
    config = Config(connect_timeout=2, read_timeout=8, retries={"total_max_attempts": 1})
    credentials = dict(endpoint_url=endpoint, region_name="us-east-1",
                       aws_access_key_id="sdk-test", aws_secret_access_key="sdk-secret", config=config)
    ses, sesv2, sns = (boto3.client(service, **credentials) for service in ("ses", "sesv2", "sns"))
    suffix = uuid.uuid4().hex[:12]
    configuration, unrelated, topic_name = ("events-" + suffix, "other-" + suffix, "ses-events-" + suffix)
    topic_arn = None
    subscription_arn = None
    sets = []
    checks = 0
    accepted_ids = set()

    def records():
        if not output.exists():
            return []
        return [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines(keepends=True) if line.endswith("\n")]

    def wait_event(message_id, event_type, *, phase="success", count=1):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            found = [item for item in records() if item["phase"] == phase and item["event"]["eventType"] == event_type
                     and item["event"]["mail"]["messageId"] == message_id]
            if len(found) >= count:
                return found
            time.sleep(0.03)
        raise AssertionError(("missing native event", message_id, event_type, phase, records()))

    def successes(message_id, event_type):
        return [item for item in records() if item["phase"] == "success" and item["event"]["eventType"] == event_type
                and item["event"]["mail"]["messageId"] == message_id]

    def expect_error(operation, code, **kwargs):
        nonlocal checks
        try:
            operation(**kwargs)
        except ClientError as exc:
            assert exc.response["Error"]["Code"] == code, exc.response
            assert exc.response["ResponseMetadata"]["HTTPStatusCode"] == 400, exc.response
            # The unchanged SDK must select the modeled exception class too.
            assert isinstance(exc, ses.exceptions.from_code(code)), type(exc)
        else:
            raise AssertionError(("expected error", code, kwargs))
        checks += 1

    def destination(enabled=True, matching_types=None):
        return {"Name": "tracking", "Enabled": enabled,
                "MatchingEventTypes": matching_types or ["send", "open", "bounce"],
                "SNSDestination": {"TopicARN": topic_arn}}

    def update(enabled=True, matching_types=None):
        return ses.update_configuration_set_event_destination(
            ConfigurationSetName=configuration, EventDestination=destination(enabled, matching_types))

    def send(*, selected=None):
        selected = configuration if selected is None else selected
        kwargs = dict(Source="SDK Sender <sender@example.test>",
                      Destination={"ToAddresses": ["recipient@example.test"], "CcAddresses": ["copy@example.test"]},
                      Message={"Subject": {"Data": "SES event SDK proof"}, "Body": {"Text": {"Data": "captured only"}}})
        if selected:
            kwargs["ConfigurationSetName"] = selected
        result = ses.send_email(**kwargs)
        accepted_ids.add(result["MessageId"])
        return result["MessageId"]

    def outcome(message_id, event_type, *, expected_status=200, **extra):
        data = json.dumps({"message_id": message_id, "event_type": event_type, **extra}).encode()
        request = urllib.request.Request(endpoint + "/__eventbus/dev/ses/outcomes", data=data,
                                         headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=8) as response:
                status, result = response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            status, result = exc.code, json.load(exc)
        assert status == expected_status, (status, result)
        if expected_status == 200:
            assert result["local"] is True, result
            assert result["message_id"] == message_id and result["event_type"] == event_type, result
        return result

    try:
        topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]
        subscription_arn = sns.subscribe(TopicArn=topic_arn, Protocol="lambda", Endpoint=function_arn)["SubscriptionArn"]
        for name in (configuration, unrelated):
            ses.create_configuration_set(ConfigurationSet={"Name": name})
            sets.append(name)
        expect_error(ses.create_configuration_set, "ConfigurationSetAlreadyExists", ConfigurationSet={"Name": configuration})
        expect_error(ses.create_configuration_set_event_destination, "ConfigurationSetDoesNotExist",
                     ConfigurationSetName="absent-" + suffix, EventDestination=destination())
        bad = destination()
        bad["SNSDestination"] = {"TopicARN": topic_arn + "-absent"}
        expect_error(ses.create_configuration_set_event_destination, "InvalidSNSDestination",
                     ConfigurationSetName=configuration, EventDestination=bad)
        ses.create_configuration_set_event_destination(ConfigurationSetName=configuration, EventDestination=destination())
        expect_error(ses.create_configuration_set_event_destination, "EventDestinationAlreadyExists",
                     ConfigurationSetName=configuration, EventDestination=destination())
        described = ses.describe_configuration_set(ConfigurationSetName=configuration, ConfigurationSetAttributeNames=["eventDestinations"])
        assert described["ConfigurationSet"] == {"Name": configuration}, described
        assert described["EventDestinations"] == [destination()], described
        checks += 1

        message_id = send()
        send_event = wait_event(message_id, "Send")[0]
        mail = send_event["event"]["mail"]
        assert mail["destination"] == ["recipient@example.test", "copy@example.test"], mail
        assert mail["tags"]["ses:configuration-set"] == [configuration], mail
        assert send_event["sns"]["TopicArn"] == topic_arn, send_event
        assert send_event["sns"]["MessageId"] != message_id, send_event
        assert send_event["event"]["send"] == {}, send_event
        assert not successes(message_id, "Open") and not successes(message_id, "Bounce"), records()
        checks += 1

        result = outcome(message_id, "Open", ip_address="192.0.2.12", user_agent="EventBus SDK explicit proof")
        assert len(result["destinations"]) == 1, result
        opened = wait_event(message_id, "Open")[0]
        assert opened["event"]["open"]["ipAddress"] == "192.0.2.12", opened
        assert opened["event"]["mail"] == mail, opened
        checks += 1

        outcome(message_id, "Bounce", recipients=["recipient@example.test"], bounce_type="Permanent", bounce_sub_type="NoEmail")
        failed = wait_event(message_id, "Bounce", phase="failure")[0]
        bounced = wait_event(message_id, "Bounce")[0]
        assert failed["sns"]["MessageId"] == bounced["sns"]["MessageId"], (failed, bounced)
        assert bounced["event"]["bounce"]["bouncedRecipients"] == [{"emailAddress": "recipient@example.test"}], bounced
        assert bounced["event"]["bounce"]["bounceSubType"] == "NoEmail", bounced
        assert bounced["event"]["mail"] == mail, bounced
        checks += 1

        # A body-based SNS filter sees the native event JSON; refused Open is not
        # replaced by a test's direct Publish or an application-shaped payload.
        sns.set_subscription_attributes(SubscriptionArn=subscription_arn, AttributeName="FilterPolicyScope", AttributeValue="MessageBody")
        sns.set_subscription_attributes(SubscriptionArn=subscription_arn, AttributeName="FilterPolicy", AttributeValue=json.dumps({"eventType": ["Send"]}))
        outcome(message_id, "Open")
        marker = send()
        wait_event(marker, "Send")  # FIFO async owner joins earlier admissions.
        assert len(successes(message_id, "Open")) == 1, records()
        sns.set_subscription_attributes(SubscriptionArn=subscription_arn, AttributeName="FilterPolicy", AttributeValue="{}")
        checks += 1

        # Disabled/nonmatching/unrelated rules are checked at native admission,
        # so a later matching Send marker bounds the asynchronous observation.
        update(False)
        disabled_id = send()
        assert outcome(disabled_id, "Open")["destinations"] == [], records()
        update(True, ["bounce"])
        nonmatching_id = send()
        assert outcome(nonmatching_id, "Open")["destinations"] == [], records()
        unrelated_id = send(selected=unrelated)
        update()
        marker = send()
        wait_event(marker, "Send")
        assert not successes(disabled_id, "Send") and not successes(disabled_id, "Open"), records()
        assert not successes(nonmatching_id, "Send") and not successes(nonmatching_id, "Open"), records()
        assert not successes(unrelated_id, "Send"), records()
        checks += 1

        # API selection wins a conflicting MIME header. Header-only selection
        # also publishes while original request bytes remain unchanged in JSONL.
        raw = ("From: sender@example.test\r\nTo: recipient@example.test\r\nSubject: raw\r\n"
               "X-SES-CONFIGURATION-SET: " + unrelated + "\r\n\r\nraw body").encode()
        api_raw_id = ses.send_raw_email(RawMessage={"Data": raw}, ConfigurationSetName=configuration)["MessageId"]
        accepted_ids.add(api_raw_id)
        assert wait_event(api_raw_id, "Send")[0]["event"]["mail"]["tags"]["ses:configuration-set"] == [configuration]
        header_raw = raw.replace(unrelated.encode(), configuration.encode())
        header_raw_id = ses.send_raw_email(RawMessage={"Data": header_raw})["MessageId"]
        accepted_ids.add(header_raw_id)
        assert wait_event(header_raw_id, "Send")[0]["event"]["mail"]["tags"]["ses:configuration-set"] == [configuration]
        v2_id = sesv2.send_email(FromEmailAddress="sender@example.test", Destination={"ToAddresses": ["recipient@example.test"]},
                               ConfigurationSetName=configuration, Content={"Simple": {"Subject": {"Data": "v2 event"},
                               "Body": {"Text": {"Data": "capture only"}}}})["MessageId"]
        accepted_ids.add(v2_id)
        wait_event(v2_id, "Send")
        checks += 3

        captured = [json.loads(line) for line in capture.read_text(encoding="utf-8").splitlines() if line.strip()]
        requests_by_id = {email["message_id"]: record for record in captured for email in record.get("emails") or [] if "message_id" in email}
        assert accepted_ids <= requests_by_id.keys(), (accepted_ids, requests_by_id.keys())
        import base64
        assert base64.b64decode(requests_by_id[api_raw_id]["request"]["RawMessage"]["Data"]) == raw
        assert base64.b64decode(requests_by_id[header_raw_id]["request"]["RawMessage"]["Data"]) == header_raw
        assert "ConfigurationSetName" not in requests_by_id[header_raw_id]["request"]
        assert all(item["schema_version"] == 1 for item in captured), captured
        checks += 1

        ses.delete_configuration_set_event_destination(ConfigurationSetName=configuration, EventDestinationName="tracking")
        assert outcome(message_id, "Open")["destinations"] == [], records()
        expect_error(ses.delete_configuration_set_event_destination, "EventDestinationDoesNotExist",
                     ConfigurationSetName=configuration, EventDestinationName="tracking")
        expect_error(ses.update_configuration_set_event_destination, "EventDestinationDoesNotExist",
                     ConfigurationSetName=configuration, EventDestination=destination())
        ses.delete_configuration_set(ConfigurationSetName=configuration)
        sets.remove(configuration)
        expect_error(ses.describe_configuration_set, "ConfigurationSetDoesNotExist", ConfigurationSetName=configuration)
        # Recreated same-name resources cannot capture old-message outcomes.
        ses.create_configuration_set(ConfigurationSet={"Name": configuration})
        sets.append(configuration)
        ses.create_configuration_set_event_destination(ConfigurationSetName=configuration, EventDestination=destination())
        assert outcome(message_id, "Open")["destinations"] == [], records()
        outcome("never-accepted", "Open", expected_status=404)
        outcome(message_id, "Bounce", recipients=["foreign@example.test"], expected_status=400)
        checks += 1
        print(json.dumps({"status": "passed", "checks": checks, "accepted_messages": len(accepted_ids),
                          "native_handler_observations": len(records())}))
    finally:
        if subscription_arn is not None:
            sns.unsubscribe(SubscriptionArn=subscription_arn)
        for name in reversed(sets):
            ses.delete_configuration_set(ConfigurationSetName=name)
        if topic_arn is not None:
            sns.delete_topic(TopicArn=topic_arn)


if __name__ == "__main__":
    main()
    print("PASS")
