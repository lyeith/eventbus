package firehose

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFirehoseManagerUnit tests the manager directly without HTTP
func TestFirehoseManagerUnit(t *testing.T) {
	t.Run("create and get stream", func(t *testing.T) {
		fm := newTestFirehoseManager(t)
		ds, err := fm.CreateStream("test", "bucket", "prefix/", "errors/", 1, 60)
		require.NoError(t, err)
		assert.Equal(t, "test", ds.Name)
		assert.Equal(t, "ACTIVE", ds.Status)
		assert.Contains(t, ds.ARN, "test")

		got := fm.GetStream("test")
		assert.Equal(t, ds, got)

		_ = fm.Shutdown()
	})

	t.Run("put record buffers", func(t *testing.T) {
		fm := newTestFirehoseManager(t)
		ds, _ := fm.CreateStream("buf-test", "bucket", "", "", 100, 3600) // large buffer, long interval

		id, err := fm.PutRecord(ds, []byte(`{"test":true}`+"\n"))
		require.NoError(t, err)
		assert.NotEmpty(t, id)

		ds.mu.Lock()
		assert.Len(t, ds.buffer, 1)
		ds.mu.Unlock()

		_ = fm.Shutdown()
	})

	t.Run("put record batch buffers all", func(t *testing.T) {
		fm := newTestFirehoseManager(t)
		ds, _ := fm.CreateStream("batch-buf", "bucket", "", "", 100, 3600)

		ids, err := fm.PutRecordBatch(ds, [][]byte{
			[]byte(`{"a":1}` + "\n"),
			[]byte(`{"b":2}` + "\n"),
			[]byte(`{"c":3}` + "\n"),
		})
		require.NoError(t, err)
		assert.Len(t, ids, 3)

		ds.mu.Lock()
		assert.Len(t, ds.buffer, 3)
		ds.mu.Unlock()

		_ = fm.Shutdown()
	})

	t.Run("delete flushes and removes", func(t *testing.T) {
		fm := newTestFirehoseManager(t)
		_, err := fm.CreateStream("del-test", "bucket", "", "", 100, 3600)
		require.NoError(t, err)
		assert.NotNil(t, fm.GetStream("del-test"))

		require.NoError(t, fm.DeleteStream(context.Background(), "del-test"))
		assert.Nil(t, fm.GetStream("del-test"))

		_ = fm.Shutdown()
	})
}

func newTestFirehoseManager(t *testing.T) *FirehoseManager {
	t.Helper()
	sink := httptest.NewServer(s3MockHandler())
	t.Cleanup(sink.Close)
	manager := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	t.Cleanup(func() { require.NoError(t, manager.Shutdown()) })
	return manager
}
