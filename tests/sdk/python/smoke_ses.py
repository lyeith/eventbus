#!/usr/bin/env python3
"""Exercise all nine SES sends through frozen boto3 and current AWS model fixtures."""

from __future__ import annotations

import base64
import datetime as dt
import json
import os
from pathlib import Path
import sys
import traceback

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ["SES_ENDPOINT_URL"]
CAPTURE = Path(os.environ["SES_CAPTURE_LOG"])
MODE = os.environ.get("SES_SMOKE_MODE", "full")
SENDER = "SDK Sender <sender@example.test>"
DESTINATION = {"ToAddresses": ["recipient@example.test"], "CcAddresses": ["cc@example.test"], "BccAddresses": ["bcc@example.test"]}
IDENTITY_ARN = "arn:aws:ses:us-east-1:000000000000:identity/example.test"
TEMPLATE_ARN = "arn:aws:ses:us-east-1:000000000000:template/welcome"
RAW = (
    b"From: SDK Sender <sender@example.test>\r\n"
    b"To: recipient@example.test\r\n"
    b"Cc: cc@example.test\r\n"
    b"Subject: SDK raw capture\r\n"
    b"MIME-Version: 1.0\r\n"
    b"Content-Type: multipart/mixed; boundary=ses-sdk-boundary\r\n\r\n"
    b"--ses-sdk-boundary\r\n"
    b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
    b"Raw body + & < >.\r\n"
    b"--ses-sdk-boundary\r\n"
    b"Content-Type: application/octet-stream\r\n"
    b"Content-Disposition: attachment; filename=payload.bin\r\n"
    b"Content-Transfer-Encoding: base64\r\n\r\n"
    b"AAH+/w==\r\n"
    b"--ses-sdk-boundary--\r\n"
)
ATTACHMENT = {
    "RawContent": b"%PDF-1.7\n\x00\x01\xfe\xff",
    "FileName": "capture.pdf",
    "ContentDisposition": "ATTACHMENT",
    "ContentDescription": "SDK binary capture",
    "ContentId": "sdk-pdf",
    "ContentTransferEncoding": "BASE64",
    "ContentType": "application/pdf",
}
MESSAGE = {
    "Subject": {"Data": "SDK subject \u03c0 + & < >", "Charset": "UTF-8"},
    "Body": {
        "Text": {"Data": "SDK text \u03c0\r\n\u4e8c + & < >.", "Charset": "UTF-8"},
        "Html": {"Data": "<p>SDK HTML \u03c0 &amp; body.</p>", "Charset": "UTF-8"},
    },
}
SEEN: set[tuple[str, str]] = set()
MESSAGE_IDS: set[str] = set()
CHECKS = 0


def make_client(service: str):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="sdk-test",
        aws_secret_access_key="sdk-secret",
        # The endpoint is local. Explicit SigV4 avoids requiring AWS CRT for
        # global EndpointId test fields while retaining real SDK serialization.
        config=Config(signature_version="v4", connect_timeout=2, read_timeout=5, retries={"total_max_attempts": 1}),
    )


def normalized(value):
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: normalized(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalized(item) for item in value]
    return value


def records() -> list[dict]:
    with CAPTURE.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def invoke(client, api: str, operation: str, params: dict, *, error: str | None = None, status: int = 400):
    global CHECKS
    before = len(records())
    method = client.meta.method_to_api_mapping
    name = next(key for key, value in method.items() if value == operation)
    try:
        result = getattr(client, name)(**params)
    except ClientError as exc:
        assert error is not None, (api, operation, exc.response)
        if error == "ConfigurationSetDoesNotExist":
            assert isinstance(exc, client.exceptions.ConfigurationSetDoesNotExistException), type(exc)
        result = exc.response
        assert result["Error"]["Code"] == error, result
        assert result["ResponseMetadata"]["HTTPStatusCode"] == status, result
    else:
        assert error is None, (api, operation, result)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200, result
        SEEN.add((api, operation))
    captured = records()
    assert len(captured) == before + 1, (api, operation, before, captured)
    record = captured[-1]
    assert record["schema_version"] == 1, record
    assert record["api"] == api and record["operation"] == operation, record
    assert record["api_version"] == ("2010-12-01" if api == "ses" else "2019-09-27"), record
    assert record["request_id"], record
    dt.datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
    assert record["request"] == normalized(params), (record["request"], normalized(params))
    assert record["outcome"]["http_status"] == (status if error else 200), record
    if error:
        assert record["outcome"]["error"]["code"] == error, record
    else:
        response = {key: item for key, item in result.items() if key != "ResponseMetadata"}
        assert record["outcome"]["response"] == response, (record, response)
        assert record["emails"], record
        ids = [response["MessageId"]] if "MessageId" in response else [
            item["MessageId"] for item in response.get("Status", response.get("BulkEmailEntryResults", [])) if item.get("MessageId")
        ]
        for message_id in ids:
            assert message_id and message_id not in MESSAGE_IDS, (api, operation, response)
            MESSAGE_IDS.add(message_id)
        request_id = result["ResponseMetadata"].get("RequestId")
        assert request_id == record["request_id"], (result, record)
    assert "sdk-secret" not in json.dumps(record), record
    assert "Authorization" not in record["request"], record
    CHECKS += 1
    return result, record


def raw_configuration_suite(v1):
    # Real frozen boto3 bytes serialization, with no application/header rewrite.
    # The API selector wins when both selectors are supplied (AWS SES team):
    # https://aws.amazon.com/blogs/messaging-and-targeting/introducing-sending-metrics/
    cases = [
        (b"X-SES-CONFIGURATION-SET: sdk-header-config\r\n", {}, "sdk-header-config", None),
        (b"x-sEs-cOnFiGuRaTiOn-SeT: sdk-header-config\r\n", {}, "sdk-header-config", None),
        (b"X-SES-CONFIGURATION-SET:\r\n\tsdk-header-config\r\n", {}, "sdk-header-config", None),
        (b"", {}, "", None),
        (b"", {"ConfigurationSetName": "sdk-config"}, "sdk-config", None),
        (b"X-SES-CONFIGURATION-SET: sdk-header-config\r\n", {"ConfigurationSetName": "sdk-config"}, "sdk-config", None),
        (b"X-SES-CONFIGURATION-SET: absent-header\r\n", {"ConfigurationSetName": "sdk-config"}, "sdk-config", None),
        (b"X-SES-CONFIGURATION-SET: absent-header\r\n", {}, None, "ConfigurationSetDoesNotExist"),
        (b"X-SES-CONFIGURATION-SET: sdk-header-config\r\n", {"ConfigurationSetName": "absent-api"}, None, "ConfigurationSetDoesNotExist"),
    ]
    for header, selector, effective, error in cases:
        submitted = header + RAW
        params = {"RawMessage": {"Data": submitted}, **selector}
        _, record = invoke(v1, "ses", "SendRawEmail", params, error=error)
        assert base64.b64decode(record["request"]["RawMessage"]["Data"]) == submitted, record
        assert ("ConfigurationSetName" in record["request"]) == ("ConfigurationSetName" in selector), record
        if error:
            assert not record["emails"], record
        else:
            email = record["emails"][0]
            assert email["configuration_set"] == effective, record
            assert email["request_content_path"] == "RawMessage.Data", record
            if header:
                assert email["headers"]["X-Ses-Configuration-Set"], record


def full_suite(v1, v2):
    simple = {
        "Source": SENDER,
        "Destination": DESTINATION,
        "Message": MESSAGE,
        "ReplyToAddresses": ["reply@example.test"],
        "ReturnPath": "feedback@example.test",
        "SourceArn": IDENTITY_ARN,
        "ReturnPathArn": IDENTITY_ARN,
        "Tags": [{"Name": "suite", "Value": "sdk"}],
        "ConfigurationSetName": "sdk-config",
    }
    invoke(v1, "ses", "SendEmail", simple)
    # AWS Query represents explicitly empty optional lists with empty form values.
    # Preserve the arrays in the normalized capture rather than string values.
    invoke(v1, "ses", "SendEmail", {
        **simple,
        "Destination": {"ToAddresses": ["recipient@example.test"], "CcAddresses": [], "BccAddresses": []},
        "ReplyToAddresses": [],
        "Tags": [],
    })
    raw_params = {
        "Source": SENDER,
        "Destinations": ["recipient@example.test", "cc@example.test"],
        "RawMessage": {"Data": RAW},
        "FromArn": IDENTITY_ARN,
        "SourceArn": IDENTITY_ARN,
        "ReturnPathArn": IDENTITY_ARN,
        "Tags": [{"Name": "suite", "Value": "sdk-raw"}],
        "ConfigurationSetName": "sdk-config",
    }
    _, record = invoke(v1, "ses", "SendRawEmail", raw_params)
    assert base64.b64decode(record["request"]["RawMessage"]["Data"]) == RAW
    assert record["emails"][0]["configuration_set"] == "sdk-config", record
    raw_configuration_suite(v1)
    template = {key: item for key, item in simple.items() if key != "Message"}
    template.update(Template="welcome", TemplateArn=TEMPLATE_ARN, TemplateData=json.dumps({"name": "A \u03c0 & B"}))
    invoke(v1, "ses", "SendTemplatedEmail", template)
    # Missing personalization is an accepted request, not a synchronous rendering error.
    invoke(v1, "ses", "SendTemplatedEmail", {**template, "TemplateData": "{}"})
    bulk = {
        "Source": SENDER,
        "SourceArn": IDENTITY_ARN,
        "ReplyToAddresses": ["reply@example.test"],
        "ReturnPath": "feedback@example.test",
        "ReturnPathArn": IDENTITY_ARN,
        "ConfigurationSetName": "sdk-config",
        "DefaultTags": [{"Name": "suite", "Value": "sdk-bulk"}],
        "Template": "welcome",
        "TemplateArn": TEMPLATE_ARN,
        "DefaultTemplateData": '{"name":"Default"}',
        "Destinations": [
            {"Destination": DESTINATION, "ReplacementTags": [{"Name": "entry", "Value": "first"}], "ReplacementTemplateData": '{"name":"First"}'},
            {"Destination": {"ToAddresses": ["not-an-email"]}, "ReplacementTemplateData": '{"name":"Bad"}'},
            {"Destination": {"ToAddresses": ["last@example.test"]}},
        ],
    }
    result, _ = invoke(v1, "ses", "SendBulkTemplatedEmail", bulk)
    assert [item["Status"] for item in result["Status"]] == ["Success", "InvalidParameterValue", "Success"], result
    assert result["Status"][1].get("Error") and not result["Status"][1].get("MessageId"), result
    verification = {"EmailAddress": "verify-recipient@example.test", "TemplateName": "verify", "ConfigurationSetName": "sdk-config"}
    invoke(v1, "ses", "SendCustomVerificationEmail", verification)
    invoke(v1, "ses", "SendBounce", {
        "OriginalMessageId": "sdk-inbound",
        "BounceSender": "sender@example.test",
        "BounceSenderArn": IDENTITY_ARN,
        "Explanation": "SDK bounce capture",
        "MessageDsn": {
            "ReportingMta": "dns; sdk.example.test",
            "ArrivalDate": dt.datetime(2026, 10, 1, 1, 2, 3, tzinfo=dt.timezone.utc),
            "ExtensionFields": [{"Name": "X-SDK", "Value": "message"}],
        },
        "BouncedRecipientInfoList": [{
            "Recipient": "sender@example.test",
            "RecipientArn": IDENTITY_ARN,
            "RecipientDsnFields": {
                "FinalRecipient": "sender@example.test",
                "Action": "failed",
                "RemoteMta": "dns; receiver.example.test",
                "Status": "5.1.1",
                "DiagnosticCode": "smtp; 550 SDK fixture",
                "LastAttemptDate": dt.datetime(2026, 10, 1, 1, 2, 4, tzinfo=dt.timezone.utc),
                "ExtensionFields": [{"Name": "X-SDK", "Value": "recipient"}],
            },
        }],
    })

    invoke(v1, "ses", "SendBounce", {
        "OriginalMessageId": "sdk-inbound",
        "BounceSender": "sender@example.test",
        "BouncedRecipientInfoList": [{"Recipient": "sender@example.test", "BounceType": "DoesNotExist"}],
    })

    envelope = {
        "FromEmailAddress": SENDER,
        "FromEmailAddressIdentityArn": IDENTITY_ARN,
        "Destination": DESTINATION,
        "ReplyToAddresses": ["reply@example.test"],
        "FeedbackForwardingEmailAddress": "feedback@example.test",
        "FeedbackForwardingEmailAddressIdentityArn": IDENTITY_ARN,
        "EmailTags": [{"Name": "suite", "Value": "sdk-v2"}],
        "ConfigurationSetName": "sdk-config",
        "EndpointId": "sdk.us-east-1",
        "TenantName": "sdk-tenant",
        "ListManagementOptions": {"ContactListName": "sdk-contacts", "TopicName": "updates"},
        "ConfigurationOverrides": {"Tracking": {"OpenTrackingEnabled": "DISABLED", "ClickTrackingEnabled": "ENABLED"}},
    }
    content = {**MESSAGE, "Headers": [{"Name": "X-SDK-Capture", "Value": "exact optional headers"}], "Attachments": [ATTACHMENT]}
    _, record = invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Simple": content}})
    assert base64.b64decode(record["request"]["Content"]["Simple"]["Attachments"][0]["RawContent"]) == ATTACHMENT["RawContent"]
    _, record = invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Raw": {"Data": RAW}}})
    assert base64.b64decode(record["request"]["Content"]["Raw"]["Data"]) == RAW
    # Raw From/recipient header fallback must work without explicit envelope addresses.
    invoke(v2, "sesv2", "SendEmail", {"Content": {"Raw": {"Data": RAW}}})
    inline = {
        "TemplateContent": {"Subject": "Inline {{name}}", "Text": "Hi {{name}}", "Html": "<p>{{name}}</p>"},
        "TemplateData": '{"name":"Inline"}',
        "Headers": [{"Name": "X-SDK-Template", "Value": "inline"}],
        "Attachments": [ATTACHMENT],
    }
    invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Template": inline}})
    stored = {"TemplateName": "welcome", "TemplateArn": TEMPLATE_ARN, "TemplateData": '{"name":"Stored"}', "Headers": [{"Name": "X-SDK-Template", "Value": "stored"}], "Attachments": [ATTACHMENT]}
    invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Template": stored}})
    invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Template": {**stored, "TemplateData": "{}"}}})
    bulk2 = {key: item for key, item in envelope.items() if key not in {"Destination", "EmailTags", "ListManagementOptions"}}
    bulk2.update(
        DefaultEmailTags=[{"Name": "suite", "Value": "sdk-bulk-v2"}],
        DefaultContent={"Template": inline},
        BulkEmailEntries=[
            {"Destination": DESTINATION, "ReplacementTags": [{"Name": "entry", "Value": "first"}], "ReplacementEmailContent": {"ReplacementTemplate": {"ReplacementTemplateData": '{"name":"First"}'}}, "ReplacementHeaders": [{"Name": "X-SDK-Entry", "Value": "first"}]},
            {"Destination": {"ToAddresses": ["not-an-email"]}, "ReplacementEmailContent": {"ReplacementTemplate": {"ReplacementTemplateData": '{"name":"Bad"}'}}},
            {"Destination": {"ToAddresses": ["last@example.test"]}},
        ],
    )
    result, _ = invoke(v2, "sesv2", "SendBulkEmail", bulk2)
    assert [item["Status"] for item in result["BulkEmailEntryResults"]] == ["SUCCESS", "INVALID_PARAMETER", "SUCCESS"], result
    assert result["BulkEmailEntryResults"][1].get("Error") and not result["BulkEmailEntryResults"][1].get("MessageId"), result
    invoke(v2, "sesv2", "SendCustomVerificationEmail", verification)

    invoke(v1, "ses", "SendEmail", {**simple, "ConfigurationSetName": "absent"}, error="ConfigurationSetDoesNotExist")
    invoke(v1, "ses", "SendTemplatedEmail", {**template, "Template": "absent", "TemplateArn": "arn:aws:ses:us-east-1:000000000000:template/absent"}, error="TemplateDoesNotExist")
    invoke(v1, "ses", "SendCustomVerificationEmail", {**verification, "TemplateName": "absent"}, error="CustomVerificationEmailTemplateDoesNotExist")
    invoke(v1, "ses", "SendEmail", {**simple, "Source": "unverified@example.invalid"}, error="MessageRejected")
    invoke(v1, "ses", "SendBounce", {"OriginalMessageId": "absent", "BounceSender": "sender@example.test", "BouncedRecipientInfoList": [{"Recipient": "sender@example.test", "BounceType": "DoesNotExist"}]}, error="MessageRejected")
    invoke(v1, "ses", "SendBounce", {"OriginalMessageId": "sdk-expired", "BounceSender": "sender@example.test", "BouncedRecipientInfoList": [{"Recipient": "sender@example.test", "BounceType": "DoesNotExist"}]}, error="MessageRejected")
    invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Simple": content}, "ConfigurationSetName": "absent"}, error="NotFoundException", status=404)
    invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Template": {"TemplateName": "absent", "TemplateData": "{}"}}}, error="NotFoundException", status=404)
    invoke(v2, "sesv2", "SendCustomVerificationEmail", {**verification, "TemplateName": "absent"}, error="NotFoundException", status=404)
    invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Simple": content}, "FromEmailAddress": "unverified@example.invalid"}, error="MessageRejected")
    invoke(v2, "sesv2", "SendEmail", {**envelope, "Content": {"Simple": content, "Raw": {"Data": RAW}}}, error="BadRequestException")
    assert SEEN == {
        ("ses", "SendEmail"), ("ses", "SendRawEmail"), ("ses", "SendTemplatedEmail"),
        ("ses", "SendBulkTemplatedEmail"), ("ses", "SendCustomVerificationEmail"), ("ses", "SendBounce"),
        ("sesv2", "SendEmail"), ("sesv2", "SendBulkEmail"), ("sesv2", "SendCustomVerificationEmail"),
    }, SEEN


def disabled_suite(v1, v2):
    invoke(v1, "ses", "SendEmail", {"Source": SENDER, "Destination": DESTINATION, "Message": MESSAGE}, error="AccountSendingPausedException")
    invoke(v2, "sesv2", "SendEmail", {"FromEmailAddress": SENDER, "Destination": DESTINATION, "Content": {"Simple": MESSAGE}}, error="SendingPausedException")


def main() -> int:
    assert ENDPOINT.startswith("http://127.0.0.1:") or ENDPOINT.startswith("http://[::1]:"), ENDPOINT
    assert os.environ.get("AWS_DATA_PATH"), "the pinned current AWS models must be selected"
    v1, v2 = make_client("ses"), make_client("sesv2")
    assert set(v1.meta.service_model.operation_names) == {"SendEmail", "SendRawEmail", "SendTemplatedEmail", "SendBulkTemplatedEmail", "SendCustomVerificationEmail", "SendBounce"}
    assert set(v2.meta.service_model.operation_names) == {"SendEmail", "SendBulkEmail", "SendCustomVerificationEmail"}
    assert "ConfigurationOverrides" in v2.meta.service_model.operation_model("SendEmail").input_shape.members
    if MODE == "disabled":
        disabled_suite(v1, v2)
    else:
        assert MODE == "full", MODE
        full_suite(v1, v2)
    print(f"SES SDK mode={MODE}: {CHECKS} captured requests checked; {len(MESSAGE_IDS)} unique accepted message IDs")
    print("PASS")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        print("FAIL")
        sys.exit(1)
