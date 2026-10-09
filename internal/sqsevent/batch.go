package sqsevent

import "strings"

// Batch transfers detached native receive records and their exact invocation
// wire representation together. Payload is immutable: later edits to a record's
// maps or binary snapshots cannot change the admitted/dispatched JSON.
// Queue owners encode each record before issuing its lease; consumers use
// Payload for invocation and Records for that delivery's settlement.
type Batch struct {
	Records []Record
	Payload string
}

const EmptyBatchPayload = `{"Records":[]}`

// AssembleBatch frames already-encoded native records without encoding them a
// second time. recordJSON must be the corresponding admission-time json.Marshal
// output for every record. The queue owner retains selection and byte admission.
func AssembleBatch(records []Record, recordJSON [][]byte) Batch {
	if len(records) != len(recordJSON) {
		panic("SQS batch records and encoded records differ")
	}
	size := len(EmptyBatchPayload)
	for index, encoded := range recordJSON {
		size += len(encoded)
		if index != 0 {
			size++
		}
	}
	var payload strings.Builder
	payload.Grow(size)
	payload.WriteString(`{"Records":[`)
	for index, encoded := range recordJSON {
		if index != 0 {
			payload.WriteByte(',')
		}
		payload.Write(encoded)
	}
	payload.WriteString(`]}`)
	return Batch{Records: records, Payload: payload.String()}
}
