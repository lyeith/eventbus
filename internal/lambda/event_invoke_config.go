package lambda

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

type AsyncIdentity struct {
	Region, AccountID string
}

type EventInvokeConfig struct {
	FunctionARN              string            `json:"FunctionArn"`
	LastModified             float64           `json:"LastModified"`
	MaximumEventAgeInSeconds int               `json:"MaximumEventAgeInSeconds"`
	MaximumRetryAttempts     int               `json:"MaximumRetryAttempts"`
	DestinationConfig        DestinationConfig `json:"DestinationConfig"`
}

type DestinationConfig struct {
	OnSuccess Destination `json:"OnSuccess"`
	OnFailure Destination `json:"OnFailure"`
}

type Destination struct {
	Destination string `json:"Destination,omitempty"`
}

type eventInvokeDestinationInput struct {
	OnSuccess *Destination `json:"OnSuccess"`
	OnFailure *Destination `json:"OnFailure"`
}

type eventInvokeConfigInput struct {
	MaximumEventAgeInSeconds *int                         `json:"MaximumEventAgeInSeconds"`
	MaximumRetryAttempts     *int                         `json:"MaximumRetryAttempts"`
	DestinationConfig        *eventInvokeDestinationInput `json:"DestinationConfig"`
}

type eventInvokeConfigList struct {
	Configs    []EventInvokeConfig `json:"FunctionEventInvokeConfigs"`
	NextMarker string              `json:"NextMarker,omitempty"`
}

// The configuration for an invocation is fixed at admission. Workers never
// read mutable resource state while executing, retrying, or sending a record.
type asyncInvokePolicy struct {
	configured   bool
	age          time.Duration
	retries      int
	destinations DestinationConfig
}

func (policy asyncInvokePolicy) maximumAge() time.Duration {
	if policy.configured {
		return policy.age
	}
	return maxEventAge
}

func (policy asyncInvokePolicy) maximumRetries() int {
	if policy.configured {
		return policy.retries
	}
	return 2
}

func defaultEventInvokeConfig() EventInvokeConfig {
	return EventInvokeConfig{MaximumEventAgeInSeconds: int(maxEventAge / time.Second), MaximumRetryAttempts: 2}
}

func (config EventInvokeConfig) policy() asyncInvokePolicy {
	return asyncInvokePolicy{configured: true, age: time.Duration(config.MaximumEventAgeInSeconds) * time.Second,
		retries: config.MaximumRetryAttempts, destinations: config.DestinationConfig}
}

func canonicalAsyncFunction(name string) string {
	if !strings.Contains(name, ":") {
		return name + ":$LATEST"
	}
	return name
}

func (service *Service) asyncFunctionARN(name string) string {
	return "arn:aws:lambda:" + service.asyncIdentity.Region + ":" + service.asyncIdentity.AccountID + ":function:" + canonicalAsyncFunction(name)
}

func (service *Service) localAsyncReference(reference, qualifier string) (string, error) {
	if strings.HasPrefix(reference, "arn:") {
		parsed, err := arn.Parse(reference)
		if err != nil || parsed.Partition != "aws" || parsed.Service != "lambda" ||
			parsed.Region != service.asyncIdentity.Region || parsed.AccountID != service.asyncIdentity.AccountID {
			return "", invalidEventInvokeConfig("FunctionName must use the local Lambda namespace")
		}
	} else if account, _, ok := strings.Cut(reference, ":function:"); ok && account != service.asyncIdentity.AccountID {
		return "", invalidEventInvokeConfig("FunctionName must use the local Lambda account")
	}
	name, err := resolveName(reference, qualifier)
	if err != nil {
		return "", invalidEventInvokeConfig(err.Error())
	}
	return name, nil
}

func invalidEventInvokeConfig(message string) error {
	return invocationError(http.StatusBadRequest, "InvalidParameterValueException", message)
}

func eventInvokeConfigNotFound() error {
	return invocationError(http.StatusNotFound, "ResourceNotFoundException", "Asynchronous invocation configuration not found")
}

func (service *Service) serveEventInvokeConfig(writer http.ResponseWriter, request *http.Request, reference string, list bool) {
	name, err := service.localAsyncReference(reference, request.URL.Query().Get("Qualifier"))
	var output any
	if err == nil {
		switch {
		case list && request.Method == http.MethodGet:
			output, err = service.listEventInvokeConfigs(name, request.URL.Query().Get("Marker"), request.URL.Query().Get("MaxItems"))
		case !list && (request.Method == http.MethodPut || request.Method == http.MethodPost):
			var input *eventInvokeConfigInput
			decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 64<<10))
			decoder.DisallowUnknownFields()
			var extra any
			if decoder.Decode(&input) != nil || input == nil || decoder.Decode(&extra) != io.EOF {
				err = invocationError(http.StatusBadRequest, "InvalidRequestContentException", "Invalid asynchronous invocation configuration")
			} else {
				output, err = service.writeEventInvokeConfig(name, *input, request.Method == http.MethodPost)
			}
		case !list && request.Method == http.MethodGet:
			output, err = service.getEventInvokeConfig(name, false)
		case !list && request.Method == http.MethodDelete:
			_, err = service.getEventInvokeConfig(name, true)
		default:
			err = invocationError(http.StatusMethodNotAllowed, "InvalidRequestContentException", "Unsupported asynchronous invocation configuration method")
		}
	}
	if err != nil {
		var native *InvokeError
		if errors.As(err, &native) {
			invokeError(writer, native.Status, native.Code, native.Message)
		} else {
			invokeError(writer, http.StatusInternalServerError, "ServiceException", "Asynchronous invocation configuration failed")
		}
		return
	}
	if request.Method == http.MethodDelete {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	_ = json.NewEncoder(writer).Encode(output)
}

func (service *Service) writeEventInvokeConfig(name string, input eventInvokeConfigInput, update bool) (EventInvokeConfig, error) {
	if _, _, err := service.resolveTarget(name, ""); err != nil {
		return EventInvokeConfig{}, err
	}
	if input.MaximumEventAgeInSeconds != nil && (*input.MaximumEventAgeInSeconds < 60 || *input.MaximumEventAgeInSeconds > 21600) {
		return EventInvokeConfig{}, invalidEventInvokeConfig("MaximumEventAgeInSeconds must be 60 through 21600")
	}
	if input.MaximumRetryAttempts != nil && (*input.MaximumRetryAttempts < 0 || *input.MaximumRetryAttempts > 2) {
		return EventInvokeConfig{}, invalidEventInvokeConfig("MaximumRetryAttempts must be 0 through 2")
	}
	if input.DestinationConfig != nil {
		for _, side := range []struct {
			destination *Destination
			failure     bool
		}{
			{input.DestinationConfig.OnSuccess, false}, {input.DestinationConfig.OnFailure, true},
		} {
			if side.destination != nil && side.destination.Destination != "" {
				if err := service.validateAsyncDestination(name, side.destination.Destination, side.failure); err != nil {
					return EventInvokeConfig{}, err
				}
			}
		}
	}

	key := canonicalAsyncFunction(name)
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed || service.asyncClosed {
		return EventInvokeConfig{}, invocationError(http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
	}
	config := defaultEventInvokeConfig()
	if update {
		var exists bool
		config, exists = service.eventInvokeConfigs[key]
		if !exists {
			return EventInvokeConfig{}, eventInvokeConfigNotFound()
		}
	}
	if input.MaximumEventAgeInSeconds != nil {
		config.MaximumEventAgeInSeconds = *input.MaximumEventAgeInSeconds
	}
	if input.MaximumRetryAttempts != nil {
		config.MaximumRetryAttempts = *input.MaximumRetryAttempts
	}
	if input.DestinationConfig != nil {
		if input.DestinationConfig.OnSuccess != nil {
			config.DestinationConfig.OnSuccess = *input.DestinationConfig.OnSuccess
		}
		if input.DestinationConfig.OnFailure != nil {
			config.DestinationConfig.OnFailure = *input.DestinationConfig.OnFailure
		}
	}
	config.FunctionARN = service.asyncFunctionARN(name)
	config.LastModified = float64(time.Now().UnixMilli()) / 1000
	service.eventInvokeConfigs[key] = config
	return config, nil
}

func (service *Service) getEventInvokeConfig(name string, remove bool) (EventInvokeConfig, error) {
	key := canonicalAsyncFunction(name)
	service.mu.Lock()
	defer service.mu.Unlock()
	if remove && (service.closed || service.asyncClosed) {
		return EventInvokeConfig{}, invocationError(http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
	}
	config, exists := service.eventInvokeConfigs[key]
	if !exists {
		return EventInvokeConfig{}, eventInvokeConfigNotFound()
	}
	if remove {
		delete(service.eventInvokeConfigs, key)
	}
	return config, nil
}

func (service *Service) listEventInvokeConfigs(name, marker, maxItems string) (eventInvokeConfigList, error) {
	page := 50
	if maxItems != "" {
		limit, err := strconv.Atoi(maxItems)
		if err != nil || limit < 1 || limit > 50 {
			return eventInvokeConfigList{}, invalidEventInvokeConfig("MaxItems must be 1 through 50")
		}
		page = limit
	}
	after := ""
	if marker != "" {
		if len(marker) > 600 {
			return eventInvokeConfigList{}, invalidEventInvokeConfig("Invalid Marker")
		}
		raw, err := base64.RawURLEncoding.DecodeString(marker)
		filter, anchor, ok := strings.Cut(string(raw), "\x00")
		if err != nil || !ok || filter != name || !functionName.MatchString(anchor) {
			return eventInvokeConfigList{}, invalidEventInvokeConfig("Invalid Marker")
		}
		after = anchor
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	base, _, qualified := strings.Cut(name, ":")
	registered := false
	for key := range service.functions {
		if key == name || !qualified && strings.HasPrefix(key, base+":") || name == base+":$LATEST" && key == base {
			registered = true
			break
		}
	}
	if !registered {
		return eventInvokeConfigList{}, invocationError(http.StatusNotFound, "ResourceNotFoundException", "Function not found")
	}
	keys := make([]string, 0)
	for key := range service.eventInvokeConfigs {
		if key > after && (qualified && key == name || !qualified && strings.HasPrefix(key, base+":")) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := eventInvokeConfigList{Configs: make([]EventInvokeConfig, 0, min(len(keys), page))}
	for _, key := range keys[:min(len(keys), page)] {
		result.Configs = append(result.Configs, service.eventInvokeConfigs[key])
	}
	if len(keys) > page {
		result.NextMarker = base64.RawURLEncoding.EncodeToString([]byte(name + "\x00" + keys[page-1]))
	}
	return result, nil
}

func (service *Service) validateAsyncDestination(function, destination string, onFailure bool) error {
	parsed, err := arn.Parse(destination)
	if err != nil || len(destination) > 2048 || parsed.Partition != "aws" || strings.ContainsAny(destination, " \t\r\n\x00") {
		return invalidEventInvokeConfig("Destination must be a supported local ARN")
	}
	switch parsed.Service {
	case "s3":
		if !onFailure || parsed.Region != "" || parsed.AccountID != "" || parsed.Resource == "" || strings.Contains(parsed.Resource, "/") {
			return invalidEventInvokeConfig("S3 destination must be a bucket ARN on OnFailure")
		}
	case "sqs", "sns", "lambda", "events":
		if parsed.Region != service.asyncIdentity.Region || parsed.AccountID != service.asyncIdentity.AccountID || parsed.Resource == "" {
			return invalidEventInvokeConfig("Destination must use the local account and region")
		}
		if (parsed.Service == "sqs" || parsed.Service == "sns") && strings.HasSuffix(parsed.Resource, ".fifo") {
			return invalidEventInvokeConfig("Destination must use a standard queue or topic")
		}
		if parsed.Service == "lambda" {
			target, found := strings.CutPrefix(parsed.Resource, "function:")
			if !found || !functionName.MatchString(target) || canonicalAsyncFunction(target) == canonicalAsyncFunction(function) {
				return invalidEventInvokeConfig("Lambda destination must identify a different registered function target")
			}
		}
	default:
		return invalidEventInvokeConfig("Unsupported asynchronous destination service")
	}
	if service.asyncDestinations == nil || service.asyncDestinations.ValidateAsyncDestination(destination, onFailure) != nil {
		return invalidEventInvokeConfig("Destination is unavailable in this emulator")
	}
	return nil
}
