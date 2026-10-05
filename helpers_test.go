package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

func setupTestServer(t *testing.T) (*Broker, *httptest.Server, *sns.Client, *sqs.Client) {
	t.Helper()

	broker := NewBroker("us-east-1", "000000000000", 0)
	fm := NewFirehoseManager("us-east-1", "000000000000", "http://localhost:9000", "test", "test")
	server := NewServer(broker, fm, NewSSMStore(), NewSecretsStore("us-east-1", "000000000000"))
	ts := httptest.NewServer(server)
	t.Cleanup(ts.Close)

	// Update broker port so queue URLs point at the test server
	u, err := url.Parse(ts.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(u.Port())
	require.NoError(t, err)
	broker.port = port

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	snsClient := sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
	})
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
	})

	return broker, ts, snsClient, sqsClient
}

func setupTestServerWithFirehose(t *testing.T) (*httptest.Server, *firehose.Client, *FirehoseManager) {
	t.Helper()

	// Start a minimal S3 mock for firehose flush (just accept PUTs)
	s3mock := httptest.NewServer(s3MockHandler())
	t.Cleanup(s3mock.Close)

	broker := NewBroker("us-east-1", "000000000000", 0)
	fm := NewFirehoseManager("us-east-1", "000000000000", s3mock.URL, "test", "test")
	server := NewServer(broker, fm, NewSSMStore(), NewSecretsStore("us-east-1", "000000000000"))
	ts := httptest.NewServer(server)
	t.Cleanup(func() {
		ts.Close()
		require.NoError(t, fm.Shutdown())
	})

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	fhClient := firehose.NewFromConfig(cfg, func(o *firehose.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
	})

	return ts, fhClient, fm
}

// s3MockHandler returns an HTTP handler that accepts PUT requests (simulates S3).
func s3MockHandler() *s3Mock {
	return &s3Mock{objects: make(map[string][]byte)}
}

type s3Mock struct {
	objects map[string][]byte
	mu      sync.Mutex
}

func (m *s3Mock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := r.URL.Path
	switch r.Method {
	case "PUT":
		body, _ := io.ReadAll(r.Body)
		m.objects[key] = body
		w.WriteHeader(200)
	case "GET":
		if data, ok := m.objects[key]; ok {
			_, _ = w.Write(data)
		} else {
			w.WriteHeader(404)
		}
	default:
		w.WriteHeader(405)
	}
}
