package sqsevent

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssembleBatchPreservesNativeWireAndOwnsEncodedBytes(t *testing.T) {
	for _, count := range []int{0, 1, 2} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			records := make([]Record, count)
			encoded := make([][]byte, count)
			for index := range records {
				text := "line\n<>&"
				records[index] = Record{
					MessageID: "native-message", ReceiptHandle: "current-receipt", Body: text,
					Attributes: map[string]string{"ApproximateReceiveCount": "1"},
					MessageAttributes: map[string]MessageAttribute{
						"binary": {DataType: "Binary", BinaryValue: []byte{0, 1, 255}, StringListValues: []string{}, BinaryListValues: [][]byte{}},
						"text":   {DataType: "String", StringValue: &text, StringListValues: []string{}, BinaryListValues: [][]byte{}},
					},
					EventSource: "aws:sqs", EventSourceARN: "arn:aws:sqs:us-east-1:000000000000:owned", AWSRegion: "us-east-1",
				}
				var err error
				encoded[index], err = json.Marshal(records[index])
				require.NoError(t, err)
			}
			batch := AssembleBatch(records, encoded)
			expected, err := json.Marshal(Event{Records: records})
			require.NoError(t, err)
			require.Equal(t, string(expected), batch.Payload)
			require.Equal(t, records, batch.Records)
			if count == 0 {
				require.Equal(t, EmptyBatchPayload, batch.Payload)
				return
			}
			// Neither the encoded temporary buffers nor mutable detached record
			// fields alias the immutable admission/transport representation.
			encoded[0][0] = 'x'
			records[0].Body = "changed body"
			records[0].Attributes["ApproximateReceiveCount"] = "99"
			records[0].MessageAttributes["binary"].BinaryValue[0] = 42
			require.Equal(t, string(expected), batch.Payload)
			require.True(t, json.Valid([]byte(batch.Payload)))
		})
	}
}
