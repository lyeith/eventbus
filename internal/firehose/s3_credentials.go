package firehose

import "github.com/lyeith/eventbus/internal/s3delivery"

// S3Credentials are explicit static credentials for this manager's configured
// S3 endpoint. SessionToken is optional.
type S3Credentials = s3delivery.Credentials
