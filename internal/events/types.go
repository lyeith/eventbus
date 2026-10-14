// Package events owns native default-bus EventBridge scheduled rules.
// Rules are distinct from EventBridge Scheduler schedules.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type TargetInvoker interface {
	ValidateTarget(context.Context, string) error
	AdmitTarget(context.Context, string, []byte) error
}

type APIError struct {
	Code    string
	Status  int
	Message string
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }
func validation(message string) error {
	return &APIError{"ValidationException", http.StatusBadRequest, message}
}
func absent() error {
	return &APIError{"ResourceNotFoundException", http.StatusBadRequest, "Rule or default event bus not found"}
}
func unavailable() error {
	return &APIError{"InternalException", http.StatusServiceUnavailable, "EventBridge is closing"}
}

type PutRuleInput struct {
	Name               string          `json:"Name"`
	Description        string          `json:"Description,omitempty"`
	EventBusName       string          `json:"EventBusName,omitempty"`
	EventPattern       *string         `json:"EventPattern,omitempty"`
	ScheduleExpression string          `json:"ScheduleExpression"`
	State              string          `json:"State,omitempty"`
	RoleArn            *string         `json:"RoleArn,omitempty"`
	Tags               json.RawMessage `json:"Tags,omitempty"`
}
type Rule struct {
	Name               string `json:"Name"`
	Arn                string `json:"Arn"`
	Description        string `json:"Description,omitempty"`
	ScheduleExpression string `json:"ScheduleExpression"`
	State              string `json:"State"`
	EventBusName       string `json:"EventBusName"`
	CreatedBy          string `json:"CreatedBy"`
}
type RuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName,omitempty"`
	Force        bool   `json:"Force,omitempty"`
}
type InputTransformer struct {
	InputPathsMap map[string]string `json:"InputPathsMap,omitempty"`
	InputTemplate string            `json:"InputTemplate"`
}
type RetryPolicy struct {
	MaximumEventAgeInSeconds *int `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     *int `json:"MaximumRetryAttempts,omitempty"`
}
type Target struct {
	Id                          string            `json:"Id"`
	Arn                         string            `json:"Arn"`
	Input                       *string           `json:"Input,omitempty"`
	InputPath                   *string           `json:"InputPath,omitempty"`
	InputTransformer            *InputTransformer `json:"InputTransformer,omitempty"`
	RetryPolicy                 *RetryPolicy      `json:"RetryPolicy,omitempty"`
	RoleArn                     *string           `json:"RoleArn,omitempty"`
	DeadLetterConfig            json.RawMessage   `json:"DeadLetterConfig,omitempty"`
	AppSyncParameters           json.RawMessage   `json:"AppSyncParameters,omitempty"`
	BatchParameters             json.RawMessage   `json:"BatchParameters,omitempty"`
	EcsParameters               json.RawMessage   `json:"EcsParameters,omitempty"`
	HttpParameters              json.RawMessage   `json:"HttpParameters,omitempty"`
	KinesisParameters           json.RawMessage   `json:"KinesisParameters,omitempty"`
	RedshiftDataParameters      json.RawMessage   `json:"RedshiftDataParameters,omitempty"`
	RunCommandParameters        json.RawMessage   `json:"RunCommandParameters,omitempty"`
	SageMakerPipelineParameters json.RawMessage   `json:"SageMakerPipelineParameters,omitempty"`
	SqsParameters               json.RawMessage   `json:"SqsParameters,omitempty"`
}
type PutTargetsInput struct {
	Rule         string   `json:"Rule"`
	EventBusName string   `json:"EventBusName,omitempty"`
	Targets      []Target `json:"Targets"`
}
type RemoveTargetsInput struct {
	Rule         string   `json:"Rule"`
	EventBusName string   `json:"EventBusName,omitempty"`
	Ids          []string `json:"Ids"`
	Force        bool     `json:"Force,omitempty"`
}
type FailedEntry struct {
	TargetId     string `json:"TargetId"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}
type TargetsResult struct {
	FailedEntryCount int           `json:"FailedEntryCount"`
	FailedEntries    []FailedEntry `json:"FailedEntries"`
}
type ListRulesInput struct {
	EventBusName string `json:"EventBusName,omitempty"`
	NamePrefix   string `json:"NamePrefix,omitempty"`
	Limit        *int   `json:"Limit,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
}
type ListRulesOutput struct {
	Rules     []Rule `json:"Rules"`
	NextToken string `json:"NextToken,omitempty"`
}
type ListTargetsInput struct {
	Rule         string `json:"Rule"`
	EventBusName string `json:"EventBusName,omitempty"`
	Limit        *int   `json:"Limit,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
}
type ListTargetsOutput struct {
	Targets   []Target `json:"Targets"`
	NextToken string   `json:"NextToken,omitempty"`
}
type Options struct {
	Region, AccountID string
	Dev               DevOptions
}

func (s *Service) ruleARN(name string) string {
	return fmt.Sprintf("arn:aws:events:%s:%s:rule/%s", s.region, s.account, name)
}
