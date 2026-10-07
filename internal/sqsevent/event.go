// Package sqsevent owns the native SQS-to-Lambda event schema shared by native
// event-source mappings and development consumer recipes. It owns no queue state.
package sqsevent

type Event struct {
	Records []Record `json:"Records"`
}

type Record struct {
	MessageID         string                      `json:"messageId"`
	ReceiptHandle     string                      `json:"receiptHandle"`
	Body              string                      `json:"body"`
	Attributes        map[string]string           `json:"attributes"`
	MessageAttributes map[string]MessageAttribute `json:"messageAttributes"`
	MD5OfBody         string                      `json:"md5OfBody"`
	EventSource       string                      `json:"eventSource"`
	EventSourceARN    string                      `json:"eventSourceARN"`
	AWSRegion         string                      `json:"awsRegion"`
}

type MessageAttribute struct {
	DataType         string   `json:"dataType"`
	StringValue      *string  `json:"stringValue,omitempty"`
	BinaryValue      []byte   `json:"binaryValue,omitempty"`
	StringListValues []string `json:"stringListValues"`
	BinaryListValues [][]byte `json:"binaryListValues"`
}
