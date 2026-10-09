// Stream-owned object construction and buffer publication. The flush slot
// serializes builders; immutable snapshots keep admission available during builds.
package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
)

// objectBuilder is the private object-preparation dependency. Stream creation
// freezes it alongside native configuration; it owns no admission or settlement.
type objectBuilder func(context.Context, StreamConfig, *time.Location, []bufferedRecord) (*deliveryObject, error)

func buildBufferedObjects(ctx context.Context, ds *DeliveryStream, buffer []bufferedRecord, force bool) ([]*deliveryObject, map[string]bool, error) {
	groups := make(map[string][]bufferedRecord)
	var order []string
	for _, record := range buffer {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if _, exists := groups[record.group]; !exists {
			order = append(order, record.group)
		}
		groups[record.group] = append(groups[record.group], record)
	}
	selected := make(map[string]bool)
	var objects []*deliveryObject
	for _, group := range order {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		records := groups[group]
		size := 0
		for _, record := range records {
			size += len(record.data)
		}
		if !force && size < ds.config.BufferingHints.SizeInMBs*1024*1024 && time.Since(records[0].arrived) < time.Duration(ds.config.BufferingHints.IntervalInSeconds)*time.Second {
			continue
		}
		object, err := ds.buildObject(ctx, ds.config, ds.location, records)
		if err != nil {
			return nil, nil, fmt.Errorf("prepare Firehose destination: %w", err)
		}
		objects = append(objects, object)
		selected[group] = true
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return objects, selected, nil
}

// Caller holds ds.mu and the flush slot. Only the captured prefix can move to
// pending objects; concurrent appends, even in a selected partition, stay queued.
// Count/bytes do not change until destination response cleanup succeeds.
func commitBufferedObjects(ds *DeliveryStream, captured int, selected map[string]bool) {
	remaining := ds.buffer[:0]
	for index, record := range ds.buffer {
		if index >= captured || !selected[record.group] {
			remaining = append(remaining, record)
		}
	}
	clear(ds.buffer[len(remaining):])
	ds.buffer = remaining
}

func makeObject(ctx context.Context, config StreamConfig, location *time.Location, records []bufferedRecord) (*deliveryObject, error) {
	oldest := records[0]
	prefix := config.Prefix
	if oldest.errorType != "" {
		prefix = config.ErrorOutputPrefix
	}
	evaluated, err := renderPrefix(prefix, oldest.arrived.In(location), oldest.keys, oldest.errorType, oldest.errorType != "", config.DynamicPartitioningConfiguration.Enabled, false)
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	var writer io.Writer = &body
	var compressor *gzip.Writer
	extension := config.FileExtension
	if config.CompressionFormat == "GZIP" && oldest.errorType == "" {
		compressor = gzip.NewWriter(&body)
		writer = compressor
		if extension == "" {
			extension = ".gz"
		}
	}
	originalBytes := 0
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := writer.Write(record.data); err != nil {
			return nil, err
		}
		originalBytes += record.originalBytes
	}
	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &deliveryObject{key: evaluated + config.Name + "-1-" + oldest.arrived.UTC().Format("2006-01-02-15-04-05") + "-" + uuid.NewString() + extension, data: body.Bytes(), records: records, originalBytes: originalBytes}, nil
}
