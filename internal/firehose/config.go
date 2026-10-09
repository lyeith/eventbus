package firehose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// MetadataExtractor is the processing boundary. App composition selects the
// implementation; Firehose owns native configuration and processing policy.
type MetadataExtractor interface {
	Validate(context.Context, string) error
	Extract(context.Context, string, []byte) (map[string]string, error)
}

// RecordMetadataExtractor owns one immutable query. Each call must own its
// execution state and honor cancellation; a stream may process concurrent puts.
type RecordMetadataExtractor interface {
	Extract(context.Context, []byte) (map[string]string, error)
}

// PreparedMetadataExtractor is an optional construction-time extension. Existing
// injected MetadataExtractors retain Validate/Extract through a bound adapter.
type PreparedMetadataExtractor interface {
	Prepare(context.Context, string) (RecordMetadataExtractor, error)
}

type boundMetadataExtractor struct {
	extractor MetadataExtractor
	query     string
}

func (bound boundMetadataExtractor) Extract(ctx context.Context, record []byte) (map[string]string, error) {
	return bound.extractor.Extract(ctx, bound.query, record)
}

func prepareMetadataExtractor(ctx context.Context, extractor MetadataExtractor, query string) (RecordMetadataExtractor, error) {
	if query == "" {
		return nil, nil
	}
	if prepared, ok := extractor.(PreparedMetadataExtractor); ok {
		processor, err := prepared.Prepare(ctx, query)
		if err != nil {
			return nil, err
		}
		if processor == nil {
			return nil, fmt.Errorf("metadata extractor did not prepare the query")
		}
		return processor, nil
	}
	if err := extractor.Validate(ctx, query); err != nil {
		return nil, err
	}
	return boundMetadataExtractor{extractor: extractor, query: query}, nil
}

type ProcessorParameter struct {
	ParameterName  string
	ParameterValue string
}
type Processor struct {
	Type       string
	Parameters []ProcessorParameter
}
type ProcessingConfiguration struct {
	Enabled    bool
	Processors []Processor
}
type BufferingHints struct {
	SizeInMBs         int
	IntervalInSeconds int
}
type RetryOptions struct{ DurationInSeconds int }
type DynamicPartitioningConfiguration struct {
	Enabled      bool
	RetryOptions *RetryOptions
}

// StreamConfig is owned immutable configuration; API readback uses copies.
type StreamConfig struct {
	Name                             string
	BucketARN                        string
	RoleARN                          string
	Prefix                           string
	ErrorOutputPrefix                string
	BufferingHints                   BufferingHints
	CompressionFormat                string
	CustomTimeZone                   string
	FileExtension                    string
	ProcessingConfiguration          ProcessingConfiguration
	DynamicPartitioningConfiguration DynamicPartitioningConfiguration
}

var streamNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)
var bucketARNRE = regexp.MustCompile(`^arn:aws(?:-[a-z]+)?:s3:::[A-Za-z0-9_.-]{1,255}$`)
var roleARNRE = regexp.MustCompile(`^arn:aws(?:-[a-z]+)?:iam::[0-9]{12}:role/[a-zA-Z_0-9+=,.@\-_/]+$`)
var fileExtensionRE = regexp.MustCompile(`^$|^\.[0-9a-z!\-_.*'()]+$`)

func cloneConfig(config StreamConfig) StreamConfig {
	config.ProcessingConfiguration.Processors = append([]Processor(nil), config.ProcessingConfiguration.Processors...)
	for index := range config.ProcessingConfiguration.Processors {
		config.ProcessingConfiguration.Processors[index].Parameters = append([]ProcessorParameter(nil), config.ProcessingConfiguration.Processors[index].Parameters...)
	}
	if config.DynamicPartitioningConfiguration.RetryOptions != nil {
		value := *config.DynamicPartitioningConfiguration.RetryOptions
		config.DynamicPartitioningConfiguration.RetryOptions = &value
	}
	return config
}

func (config StreamConfig) metadataQuery() string {
	for _, processor := range config.ProcessingConfiguration.Processors {
		if processor.Type == "MetadataExtraction" {
			for _, parameter := range processor.Parameters {
				if parameter.ParameterName == "MetadataExtractionQuery" {
					return parameter.ParameterValue
				}
			}
		}
	}
	return ""
}
func (config StreamConfig) appendDelimiter() bool {
	for _, processor := range config.ProcessingConfiguration.Processors {
		if processor.Type == "AppendDelimiterToRecord" {
			return true
		}
	}
	return false
}

// validateConfig also prepares the stream-owned timezone once at creation.
func validateConfig(config StreamConfig, extractor MetadataExtractor) (*time.Location, error) {
	if !streamNameRE.MatchString(config.Name) {
		return nil, fmt.Errorf("DeliveryStreamName must match [a-zA-Z0-9_.-]{1,64}")
	}
	if len(config.BucketARN) > 2048 || !bucketARNRE.MatchString(config.BucketARN) {
		return nil, fmt.Errorf("BucketARN must be an S3 bucket ARN of at most 2048 bytes")
	}
	if len(config.RoleARN) > 512 || !roleARNRE.MatchString(config.RoleARN) {
		return nil, fmt.Errorf("RoleARN must be an IAM role ARN of at most 512 bytes")
	}
	if config.BufferingHints.SizeInMBs < 1 || config.BufferingHints.SizeInMBs > 128 || config.BufferingHints.IntervalInSeconds < 0 || config.BufferingHints.IntervalInSeconds > 900 {
		return nil, fmt.Errorf("BufferingHints require SizeInMBs 1..128 and IntervalInSeconds 0..900")
	}
	if config.CompressionFormat != "UNCOMPRESSED" && config.CompressionFormat != "GZIP" {
		return nil, fmt.Errorf("unsupported CompressionFormat %q", config.CompressionFormat)
	}
	if len(config.FileExtension) > 128 || !fileExtensionRE.MatchString(config.FileExtension) {
		return nil, fmt.Errorf("invalid FileExtension")
	}
	location, err := time.LoadLocation(config.CustomTimeZone)
	if err != nil {
		return nil, fmt.Errorf("invalid CustomTimeZone")
	}
	if err := validatePrefix(config.Prefix, false, config.DynamicPartitioningConfiguration.Enabled); err != nil {
		return nil, err
	}
	if err := validatePrefix(config.ErrorOutputPrefix, true, false); err != nil {
		return nil, err
	}
	if strings.Contains(config.Prefix, "!{") && config.ErrorOutputPrefix == "" {
		return nil, fmt.Errorf("ErrorOutputPrefix is required when Prefix contains expressions")
	}
	if config.DynamicPartitioningConfiguration.RetryOptions != nil && !config.DynamicPartitioningConfiguration.Enabled {
		return nil, fmt.Errorf("dynamic partition RetryOptions require Enabled=true")
	}
	if config.DynamicPartitioningConfiguration.RetryOptions != nil && (config.DynamicPartitioningConfiguration.RetryOptions.DurationInSeconds < 0 || config.DynamicPartitioningConfiguration.RetryOptions.DurationInSeconds > 7200) {
		return nil, fmt.Errorf("dynamic partition RetryOptions.DurationInSeconds must be 0..7200")
	}
	seen := map[string]bool{}
	for _, processor := range config.ProcessingConfiguration.Processors {
		if seen[processor.Type] {
			return nil, fmt.Errorf("duplicate processor %q", processor.Type)
		}
		seen[processor.Type] = true
		parameters := map[string]string{}
		for _, parameter := range processor.Parameters {
			if _, exists := parameters[parameter.ParameterName]; exists {
				return nil, fmt.Errorf("duplicate processor parameter %q", parameter.ParameterName)
			}
			parameters[parameter.ParameterName] = parameter.ParameterValue
		}
		switch processor.Type {
		case "MetadataExtraction":
			if len(parameters) != 2 || parameters["JsonParsingEngine"] != "JQ-1.6" || parameters["MetadataExtractionQuery"] == "" {
				return nil, fmt.Errorf("MetadataExtraction requires only JsonParsingEngine=JQ-1.6 and MetadataExtractionQuery")
			}
			if !config.DynamicPartitioningConfiguration.Enabled {
				return nil, fmt.Errorf("MetadataExtraction requires dynamic partitioning")
			}
			if extractor == nil {
				return nil, fmt.Errorf("MetadataExtraction requires a configured metadata extractor")
			}
		case "AppendDelimiterToRecord":
			if len(parameters) != 0 && (len(parameters) != 1 || parameters["Delimiter"] != "\\n") {
				return nil, fmt.Errorf("AppendDelimiterToRecord supports only the newline Delimiter")
			}
		default:
			return nil, fmt.Errorf("unsupported processor %q", processor.Type)
		}
	}
	if !config.ProcessingConfiguration.Enabled && len(seen) != 0 {
		return nil, fmt.Errorf("Processors require ProcessingConfiguration.Enabled=true")
	}
	if config.DynamicPartitioningConfiguration.Enabled && (config.metadataQuery() == "" || config.ErrorOutputPrefix == "" || !strings.Contains(config.Prefix, "!{partitionKeyFromQuery:")) {
		return nil, fmt.Errorf("dynamic partitioning requires MetadataExtraction, partitionKeyFromQuery Prefix and ErrorOutputPrefix")
	}
	return location, nil
}

func configFromRequest(data map[string]interface{}) (StreamConfig, error) {
	config := StreamConfig{CompressionFormat: "UNCOMPRESSED", CustomTimeZone: "UTC", BufferingHints: BufferingHints{SizeInMBs: 5, IntervalInSeconds: 300}}
	config.Name, _ = data["DeliveryStreamName"].(string)
	for key, value := range data {
		switch key {
		case "DeliveryStreamName", "DeliveryStreamType", "ExtendedS3DestinationConfiguration", "S3DestinationConfiguration":
		default:
			return config, fmt.Errorf("unsupported CreateDeliveryStream setting %q", key)
		}
		if key == "DeliveryStreamType" && value != "DirectPut" {
			return config, fmt.Errorf("only DirectPut streams are supported")
		}
	}
	raw, extended := data["ExtendedS3DestinationConfiguration"]
	if !extended {
		raw = data["S3DestinationConfiguration"]
	} else if _, exists := data["S3DestinationConfiguration"]; exists {
		return config, fmt.Errorf("configure exactly one S3 destination")
	}
	destination, ok := raw.(map[string]interface{})
	if !ok {
		return config, fmt.Errorf("S3 destination configuration is required")
	}
	for key, value := range destination {
		switch key {
		case "BucketARN", "RoleARN", "Prefix", "ErrorOutputPrefix", "CompressionFormat", "CustomTimeZone", "FileExtension", "BufferingHints", "ProcessingConfiguration", "DynamicPartitioningConfiguration":
		case "S3BackupMode":
			if value == "Disabled" {
				continue
			}
			return config, fmt.Errorf("S3 backup is unsupported")
		case "CloudWatchLoggingOptions", "DataFormatConversionConfiguration":
			setting, ok := value.(map[string]interface{})
			if ok && len(setting) == 1 && setting["Enabled"] == false {
				continue
			}
			return config, fmt.Errorf("unsupported S3 destination setting %q", key)
		case "EncryptionConfiguration":
			setting, ok := value.(map[string]interface{})
			if ok && len(setting) == 1 && setting["NoEncryptionConfig"] == "NoEncryption" {
				continue
			}
			return config, fmt.Errorf("S3 encryption is unsupported")
		default:
			return config, fmt.Errorf("unsupported S3 destination setting %q", key)
		}
	}
	// Decode through the domain shape to reject wrong member types, then retain
	// defaults only for omitted settings (zero/false are supplied native values).
	supported := make(map[string]interface{}, len(destination))
	for key, value := range destination {
		if key != "S3BackupMode" && key != "CloudWatchLoggingOptions" && key != "DataFormatConversionConfiguration" && key != "EncryptionConfiguration" {
			supported[key] = value
		}
	}
	rawJSON, err := json.Marshal(supported)
	if err != nil {
		return config, err
	}
	var supplied StreamConfig
	decoder := json.NewDecoder(bytes.NewReader(rawJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&supplied); err != nil {
		return config, fmt.Errorf("invalid S3 destination: %w", err)
	}
	config.BucketARN, config.RoleARN, config.Prefix, config.ErrorOutputPrefix = supplied.BucketARN, supplied.RoleARN, supplied.Prefix, supplied.ErrorOutputPrefix
	config.FileExtension = supplied.FileExtension
	if _, exists := destination["CompressionFormat"]; exists {
		config.CompressionFormat = supplied.CompressionFormat
	}
	if _, exists := destination["CustomTimeZone"]; exists {
		config.CustomTimeZone = supplied.CustomTimeZone
		if config.CustomTimeZone == "" {
			config.CustomTimeZone = "UTC"
		}
	}
	if hints, exists := destination["BufferingHints"]; exists {
		values, ok := hints.(map[string]interface{})
		if !ok {
			return config, fmt.Errorf("BufferingHints must be an object")
		}
		_, hasSize := values["SizeInMBs"]
		_, hasInterval := values["IntervalInSeconds"]
		if hasSize != hasInterval {
			return config, fmt.Errorf("BufferingHints require both SizeInMBs and IntervalInSeconds")
		}
		for field := range values {
			if field != "SizeInMBs" && field != "IntervalInSeconds" {
				return config, fmt.Errorf("unsupported BufferingHints setting %q", field)
			}
		}
		if _, exists := values["SizeInMBs"]; exists {
			config.BufferingHints.SizeInMBs = supplied.BufferingHints.SizeInMBs
		}
		if _, exists := values["IntervalInSeconds"]; exists {
			config.BufferingHints.IntervalInSeconds = supplied.BufferingHints.IntervalInSeconds
		}
	}
	config.ProcessingConfiguration, config.DynamicPartitioningConfiguration = supplied.ProcessingConfiguration, supplied.DynamicPartitioningConfiguration
	return config, nil
}
