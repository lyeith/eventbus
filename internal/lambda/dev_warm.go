package lambda

import "errors"

// DevWarmConfig enables retained local execution. It is not an AWS resource
// setting. MaxWorkers caps retained idle Python/Node environments; MaxConcurrency
// caps all executing, reserved and execution-retiring environments in the service.
// Idle retirement retains its idle budget until joined, so total environments are
// bounded by the sum. Saturated synchronous execution returns native HTTP 429.
type DevWarmConfig struct {
	MaxWorkers     int `yaml:"max_workers,omitempty"`
	MaxConcurrency int `yaml:"max_concurrency,omitempty"`
}

func validateDevWarm(config *DevWarmConfig) error {
	if config != nil && (config.MaxWorkers < 0 || config.MaxWorkers > 32) {
		return errors.New("dev_warm.max_workers must be 1..32, or omitted")
	}
	if config != nil && (config.MaxConcurrency < 0 || config.MaxConcurrency > 1024) {
		return errors.New("dev_warm.max_concurrency must be 1..1024, or omitted")
	}
	return nil
}

// DevBeginWarmDrain reversibly fences worker reuse. Idle workers retire now;
// active workers retire after their response or cancellation. Admitted calls
// during the drain execute fresh. Worker DevActivity leases keep the lifecycle
// coordinator from claiming quiescence before actual retirement joins.
func (service *Service) DevBeginWarmDrain() error {
	if service.warm != nil {
		service.warm.beginDrain()
	}
	return nil
}

func (service *Service) DevResumeWarm() error {
	if service.warm == nil {
		return nil
	}
	service.warm.mu.Lock()
	defer service.warm.mu.Unlock()
	if service.warm.closed {
		return errClosed
	}
	if service.warm.closeErr != nil {
		return service.warm.closeErr
	}
	if len(service.warm.workers) != 0 {
		return errors.New("Lambda warm workers have not joined")
	}
	service.warm.draining = false
	return nil
}
