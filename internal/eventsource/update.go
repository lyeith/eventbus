package eventsource

import (
	"context"
	"strings"
)

type UpdateInput struct {
	FilterCriteria                 *FilterCriteria          `json:"FilterCriteria,omitempty"`
	ProvisionedPollerConfig        *ProvisionedPollerConfig `json:"ProvisionedPollerConfig,omitempty"`
	FunctionName                   *string                  `json:"FunctionName,omitempty"`
	BatchSize                      *int                     `json:"BatchSize,omitempty"`
	Enabled                        *bool                    `json:"Enabled,omitempty"`
	MaximumBatchingWindowInSeconds *int                     `json:"MaximumBatchingWindowInSeconds,omitempty"`
	FunctionResponseTypes          *[]string                `json:"FunctionResponseTypes,omitempty"`
	ScalingConfig                  *ScalingConfig           `json:"ScalingConfig,omitempty"`
}

// Update stops intake and joins accepted invocations and receipt settlement
// before publishing the new settings. The queue binding and mapping ID survive.
func (s *Service) Update(ctx context.Context, id string, input UpdateInput) (Mapping, error) {
	if err := ctx.Err(); err != nil {
		return Mapping{}, err
	}
	item, next, err := s.reserveUpdate(id, input)
	if err != nil {
		return Mapping{}, err
	}
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		item.updateTarget = ""
		s.mu.Unlock()
	}()

	validateCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	timeout, err := s.validateBinding(validateCtx, item.queue, next.EventSourceARN, next.FunctionARN)
	if err != nil {
		return Mapping{}, err
	}

	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return Mapping{}, err
	}
	if s.closing {
		s.mu.Unlock()
		return Mapping{}, unavailable()
	}
	// If the old bound source stopped during validation, preserve the disabled
	// state unless the request explicitly enabled it.
	next.State = item.mapping.State
	if input.Enabled != nil {
		next.State = "Disabled"
		if *input.Enabled {
			next.State = "Enabled"
		}
	}
	replacement, err := s.prepareEntry(next, item.queue, item.tags, timeout)
	if err != nil {
		s.mu.Unlock()
		return Mapping{}, err
	}
	item.mapping.State = "Updating"
	item.mapping.StateTransitionReason = "USER_INITIATED"
	item.stopIntake()
	s.mu.Unlock()

	// Even if the client's request ends here, this operation still owns the
	// transition. Shutdown can cancel executions using the service context.
	<-item.done
	s.mu.Lock()
	defer s.mu.Unlock()
	replacement.tags = item.tags // Tag changes during the drain belong to the mapping.
	replacement.mapping.LastProcessingResult = item.mapping.LastProcessingResult
	replacement.mapping.LastModified = float64(s.dev.Clock().UnixMilli()) / 1000
	replacement.mapping.StateTransitionReason = "USER_INITIATED"
	if s.closing {
		if replacement.releaseRegistration != nil {
			replacement.releaseRegistration()
		}
		item.mapping.State = "Disabled"
		return Mapping{}, unavailable()
	}
	s.entries[item.mapping.UUID] = replacement
	s.startEntry(replacement)
	return cloneMapping(replacement.mapping), ctx.Err()
}

func (s *Service) reserveUpdate(id string, input UpdateInput) (*entry, Mapping, error) {
	if !idPattern.MatchString(id) {
		return nil, Mapping{}, invalid("Invalid mapping UUID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, Mapping{}, unavailable()
	}
	item := s.entries[strings.ToLower(id)]
	if item == nil {
		return nil, Mapping{}, missing("Event source mapping not found")
	}
	if item.updateTarget != "" || item.mapping.State == "Deleting" {
		return nil, Mapping{}, mappingConflict("An event source mapping operation is in progress")
	}
	next := cloneMapping(item.mapping)
	proposed := CreateInput{
		EventSourceARN: next.EventSourceARN, FunctionName: next.FunctionARN,
		FilterCriteria: next.FilterCriteria, ProvisionedPollerConfig: next.ProvisionedPollerConfig,
		BatchSize: &next.BatchSize, ScalingConfig: next.ScalingConfig,
		FunctionResponseTypes:          next.FunctionResponseTypes,
		MaximumBatchingWindowInSeconds: &next.MaximumBatchingWindowInSeconds,
	}
	if input.FunctionName != nil {
		proposed.FunctionName = *input.FunctionName
	}
	if input.BatchSize != nil {
		proposed.BatchSize = input.BatchSize
	}
	if input.MaximumBatchingWindowInSeconds != nil {
		proposed.MaximumBatchingWindowInSeconds = input.MaximumBatchingWindowInSeconds
	}
	if input.FunctionResponseTypes != nil {
		proposed.FunctionResponseTypes = *input.FunctionResponseTypes
	}
	if input.ScalingConfig != nil {
		proposed.ScalingConfig = input.ScalingConfig
	}
	if input.FilterCriteria != nil {
		proposed.FilterCriteria = input.FilterCriteria
	}
	if input.ProvisionedPollerConfig != nil {
		proposed.ProvisionedPollerConfig = input.ProvisionedPollerConfig
		if input.ProvisionedPollerConfig.MinimumPollers == nil && input.ProvisionedPollerConfig.MaximumPollers == nil {
			proposed.ProvisionedPollerConfig = nil
		}
	}
	arn, err := s.validate(proposed)
	if err != nil {
		return nil, Mapping{}, err
	}
	if s.hasMappingConflict(item, next.EventSourceARN, arn) {
		return nil, Mapping{}, mappingConflict("A mapping already exists for this source and function")
	}
	next.FunctionARN = arn
	next.BatchSize = *proposed.BatchSize
	next.MaximumBatchingWindowInSeconds = *proposed.MaximumBatchingWindowInSeconds
	next.FunctionResponseTypes = append([]string{}, proposed.FunctionResponseTypes...)
	next.ScalingConfig = cloneScalingConfig(proposed.ScalingConfig)
	next.ProvisionedPollerConfig = cloneProvisionedPollerConfig(proposed.ProvisionedPollerConfig)
	next.FilterCriteria = cloneFilterCriteria(proposed.FilterCriteria)
	item.updateTarget = arn
	s.wg.Add(1)
	return item, next, nil
}
