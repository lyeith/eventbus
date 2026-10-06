package firehose
import ("context"; "io"; "net/http"; "net/http/httptest"; "strings"; "sync"; "testing"
"github.com/aws/aws-sdk-go-v2/aws"; "github.com/aws/aws-sdk-go-v2/config"; "github.com/aws/aws-sdk-go-v2/credentials"; sdkfirehose "github.com/aws/aws-sdk-go-v2/service/firehose"; "github.com/stretchr/testify/require")
func setupTestServerWithFirehose(t *testing.T) (*httptest.Server, *sdkfirehose.Client, *FirehoseManager) {
	t.Helper()

	// Start a minimal S3 mock for firehose flush (just accept PUTs)
	s3mock := httptest.NewServer(s3MockHandler())
	t.Cleanup(s3mock.Close)

	fm := NewFirehoseManager("us-east-1", "000000000000", s3mock.URL, "test", "test")
	handler := NewHandler(fm)
    ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        target := r.Header.Get("X-Amz-Target")
        action := target[strings.LastIndex(target,".")+1:]
        handler.ServeAction(w,r,action)
    }))
	t.Cleanup(func() {
		ts.Close()
		require.NoError(t, fm.Shutdown())
	})

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	fhClient := sdkfirehose.NewFromConfig(cfg, func(o *sdkfirehose.Options) {
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
