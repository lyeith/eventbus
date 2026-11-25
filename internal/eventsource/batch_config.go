package eventsource

import "strings"

const maxBatchWindowSeconds = 300

func batchWindowSeconds(seconds *int) int {
	if seconds == nil {
		return 0
	}
	return *seconds
}

func cloneProvisionedPollerConfig(config *ProvisionedPollerConfig) *ProvisionedPollerConfig {
	if config == nil {
		return nil
	}
	minimum, maximum := 2, 200
	if config.MinimumPollers != nil {
		minimum = *config.MinimumPollers
	}
	if config.MaximumPollers != nil {
		maximum = *config.MaximumPollers
	}
	return &ProvisionedPollerConfig{MinimumPollers: &minimum, MaximumPollers: &maximum}
}

func validateBatchConfig(input CreateInput) error {
	size := 10
	if input.BatchSize != nil {
		size = *input.BatchSize
	}
	window := batchWindowSeconds(input.MaximumBatchingWindowInSeconds)
	if window < 0 || window > maxBatchWindowSeconds {
		return invalid("MaximumBatchingWindowInSeconds must be 0 through 300")
	}
	provisioned := cloneProvisionedPollerConfig(input.ProvisionedPollerConfig)
	if provisioned != nil {
		minimum, maximum := *provisioned.MinimumPollers, *provisioned.MaximumPollers
		if minimum < 2 || minimum > 200 || maximum < minimum || maximum > 10_000 {
			return invalid("ProvisionedPollerConfig requires MinimumPollers 2 through 200 and MaximumPollers between MinimumPollers and 10000")
		}
		if cloneScalingConfig(input.ScalingConfig) != nil {
			return invalid("ProvisionedPollerConfig and ScalingConfig.MaximumConcurrency are mutually exclusive")
		}
	}
	if strings.HasSuffix(input.EventSourceARN, ".fifo") && provisioned == nil && (size > 10 || window != 0) {
		return invalid("FIFO mappings require provisioned pollers for BatchSize greater than 10 or a nonzero batching window")
	}
	if size < 1 || size > 10_000 {
		return invalid("BatchSize must be 1 through 10000")
	}
	if size > 10 && window == 0 {
		return invalid("BatchSize greater than 10 requires a batching window of at least one second")
	}
	return nil
}
