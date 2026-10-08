// Development harness adapter: local YAML recipes.
// This file does not implement AWS resource-management APIs. Native request,
// event, response and resource semantics remain in the service core.
package lambda

import (
	"context"
	"errors"
	"fmt"
	"github.com/lyeith/eventbus/internal/devcapture"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// DevAsyncConfig controls local execution resources and evidence. RetryDelays
// accelerates the native one-/two-minute retry intervals in isolated tests; it
// does not change the native maximum of two function-error retries.
type DevAsyncConfig struct {
	Workers      int             `yaml:"workers,omitempty"`
	Capacity     int             `yaml:"capacity,omitempty"`
	HistoryLimit int             `yaml:"history_limit,omitempty"`
	RetryDelays  []time.Duration `yaml:"retry_delays,omitempty"`
	LogPath      string          `yaml:"log_path,omitempty"`
	// A borrowed writer supports embedded harnesses and deterministic I/O faults.
	LogWriter io.Writer `yaml:"-"`
}

func validateDevAsync(config *DevAsyncConfig) error {
	if config == nil {
		return nil
	}
	if config.Workers < 0 || config.Workers > 32 {
		return errors.New("dev_async.workers must be 1..32, or omitted")
	}
	if config.Capacity < 0 || config.Capacity > 1024 {
		return errors.New("dev_async.capacity must be 1..1024, or omitted")
	}
	if config.HistoryLimit < 0 || config.HistoryLimit > 10000 {
		return errors.New("dev_async.history_limit must be 1..10000, or omitted")
	}
	if config.RetryDelays != nil && len(config.RetryDelays) != 2 {
		return errors.New("dev_async.retry_delays must contain exactly two durations")
	}
	for _, delay := range config.RetryDelays {
		if delay < 0 || delay > time.Hour {
			return errors.New("dev_async.retry_delays must be 0..1h")
		}
	}
	if config.LogWriter != nil && config.LogPath != "" {
		return errors.New("dev_async log_writer and log_path cannot both be supplied")
	}
	return nil
}

func (service *Service) configureAsync(config *DevAsyncConfig, workDir string) error {
	service.asyncCapacity = 64
	service.asyncHistoryLimit = 256
	service.asyncRetryDelays = [2]time.Duration{time.Minute, 2 * time.Minute}
	workers := 4
	if config != nil {
		if config.Workers > 0 {
			workers = config.Workers
		}
		if config.Capacity > 0 {
			service.asyncCapacity = config.Capacity
		}
		if config.HistoryLimit > 0 {
			service.asyncHistoryLimit = config.HistoryLimit
		}
		if config.RetryDelays != nil {
			copy(service.asyncRetryDelays[:], config.RetryDelays)
		}
	}
	var err error
	if config != nil && config.LogPath != "" {
		path := config.LogPath
		if path != "-" && !filepath.IsAbs(path) {
			path = filepath.Join(workDir, path)
		}
		service.asyncCapture, err = devcapture.Open(path, "Lambda async")
	} else if config != nil && config.LogWriter != nil {
		service.asyncCapture = devcapture.NewWriter(config.LogWriter, "Lambda async")
	} else {
		service.asyncCapture = devcapture.NewWriter(os.Stderr, "Lambda async")
	}
	if err != nil {
		return err
	}
	service.asyncWake = make(chan struct{})
	service.asyncDrainDone = make(chan struct{})
	service.asyncTasks = make(map[string]*asyncTask)
	service.asyncContext, service.asyncCancel = context.WithCancelCause(context.Background())
	service.asyncWorkers.Add(workers)
	for index := 0; index < workers; index++ {
		go service.asyncWorker()
	}
	go func() { service.asyncWorkers.Wait(); close(service.asyncDrainDone) }()
	return nil
}

// LoadConfig rejects unknown fields and extra YAML documents.
func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("read Lambda configuration: %w", err)
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("parse Lambda configuration: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("Lambda configuration must contain exactly one YAML document")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}
