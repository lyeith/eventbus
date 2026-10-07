package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/itchyny/gojq"
)

// GoJQMetadataExtractor implements the supported inline-query profile with Go
// jq. It is not an exact jq 1.6 runtime: integer arithmetic, regular expressions,
// object ordering and some time functions differ as documented by gojq.
// Unsupported functions/regexes fail explicitly; host modules, input streams
// and environment variables are never supplied to the interpreter.
type GoJQMetadataExtractor struct{}

func NewGoJQMetadataExtractor() *GoJQMetadataExtractor { return &GoJQMetadataExtractor{} }

func compileMetadataQuery(ctx context.Context, query string) (*gojq.Code, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(query) == 0 || len(query) > 5120 {
		return nil, fmt.Errorf("MetadataExtractionQuery must contain 1..5120 bytes")
	}
	parsed, err := gojq.Parse(query)
	if err != nil {
		return nil, err
	}
	return gojq.Compile(parsed)
}
func (extractor *GoJQMetadataExtractor) Validate(ctx context.Context, query string) error {
	_, err := compileMetadataQuery(ctx, query)
	return err
}
func (extractor *GoJQMetadataExtractor) Extract(ctx context.Context, query string, record []byte) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	code, err := compileMetadataQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("metadata query compilation failed")
	}
	decoder := json.NewDecoder(bytes.NewReader(record))
	var input interface{}
	if err := decoder.Decode(&input); err != nil {
		return nil, fmt.Errorf("metadata extraction requires one JSON record")
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("metadata extraction requires exactly one JSON record")
	}
	iterator := code.RunWithContext(ctx, input)
	value, ok := iterator.Next()
	if !ok {
		return nil, fmt.Errorf("metadata query must return one object")
	}
	if _, isError := value.(error); isError {
		return nil, fmt.Errorf("metadata extraction failed")
	}
	values, ok := value.(map[string]interface{})
	if !ok || values == nil {
		return nil, fmt.Errorf("metadata query must return one object")
	}
	_, more := iterator.Next()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if more {
		return nil, fmt.Errorf("metadata query must return exactly one object")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Bound retained metadata before it is admitted to a stream partition.
	encoded, err := json.Marshal(values)
	if err != nil || len(encoded) > 64<<10 {
		return nil, fmt.Errorf("metadata result exceeds the processing limit")
	}
	keys := make(map[string]string, len(values))
	for key, value := range values {
		if key == "" {
			return nil, fmt.Errorf("partition key names must be nonempty")
		}
		switch typed := value.(type) {
		case string:
			keys[key] = typed
		case bool:
			keys[key] = fmt.Sprint(typed)
		case int, float64:
			encoded, err := json.Marshal(typed)
			if err != nil {
				return nil, fmt.Errorf("partition keys must be finite scalar values")
			}
			keys[key] = string(encoded)
		case *big.Int:
			keys[key] = typed.String()
		default:
			return nil, fmt.Errorf("partition keys must be non-null scalar values")
		}
	}
	return keys, nil
}
