# SES SDK compatibility models

These fixtures contain only the SES sending operations and the transitive closure
of their input, output, error and nested member shapes. Documentation strings
were removed; protocol metadata, constraints, wrappers and wire error codes are
preserved. The source models contain 71 SES v1 operations and 116 SES v2
operations; the supported send scope is six v1 and three v2 operations.

| Fixture | Source revision | Operations | Shapes |
| --- | --- | --- | --- |
| `ses/2010-12-01/service-2.json` | [c196d5f32d7d6412592ed95af8c56b3116def468](https://github.com/boto/botocore/blob/c196d5f32d7d6412592ed95af8c56b3116def468/botocore/data/ses/2010-12-01/service-2.json) (2025-08-28) | 6 | 64 |
| `sesv2/2019-09-27/service-2.json` | [9931e9741f7943efde511662929ce938f220f686](https://github.com/boto/botocore/blob/9931e9741f7943efde511662929ce938f220f686/botocore/data/sesv2/2019-09-27/service-2.json) (2026-09-29) | 3 | 69 |

The files derive from the AWS-authored models distributed with
[botocore](https://github.com/boto/botocore), licensed under the
[Apache License 2.0](https://github.com/boto/botocore/blob/9931e9741f7943efde511662929ce938f220f686/LICENSE.txt).
The upstream [NOTICE](https://github.com/boto/botocore/blob/9931e9741f7943efde511662929ce938f220f686/NOTICE)
identifies copyright Amazon.com, Inc. or its affiliates. These are reduced model
fixtures, not an unmodified copy of the complete upstream models.

The existing frozen boto3/botocore environment loads these files through
`AWS_DATA_PATH`. The Go SDK smoke runner sets this path to this owned directory
and supplies an OS-assigned loopback endpoint. No package or lockfile update is
needed to test newer optional fields such as ConfigurationOverrides.

Run the SDK lane using the commands in [../README.md](../README.md).
It exercises all nine sends and checks response/error parsing and complete
captured requests, including binary round trips. These tests establish the
supported SDK wire contract; they do not execute against AWS or prove Internet
email delivery, DNS verification, or IAM policy enforcement.
