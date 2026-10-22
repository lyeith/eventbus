package firehose

// S3Credentials are explicit static credentials for this manager's S3 delivery
// endpoint. SessionToken is optional. Values are copied at construction and are
// never loaded from environment variables or a default AWS credentials chain.
type S3Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}
