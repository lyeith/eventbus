package secrets

import (
	"bytes"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type APIError struct{ Code, Message string }

func (e *APIError) Error() string           { return e.Code + ": " + e.Message }
func invalidParameter(message string) error { return &APIError{"InvalidParameterException", message} }
func invalidRequest(message string) error   { return &APIError{"InvalidRequestException", message} }
func notFound() error {
	return &APIError{"ResourceNotFoundException", "Secrets Manager cannot find the requested secret version"}
}

// Secret is an owned snapshot. Secret values are included only by value operations.
// Describe's HTTP representation never serializes SecretString or SecretBinary.
type Secret struct {
	ARN, Name, Description, KmsKeyID              string
	SecretString                                  string
	SecretBinary                                  []byte
	StringValue, HasValue                         bool
	VersionID                                     string
	VersionStages                                 []string
	VersionIdsToStages                            map[string][]string
	CreatedDate, LastChangedDate, LastRotatedDate time.Time
	Tags                                          map[string]string
	RotationEnabled                               *bool
	RotationLambdaARN                             string
	RotationRules                                 *RotationRules
}
type RotationRules struct {
	AutomaticallyAfterDays *int   `json:"AutomaticallyAfterDays,omitempty"`
	Duration               string `json:"Duration,omitempty"`
	ScheduleExpression     string `json:"ScheduleExpression,omitempty"`
}
type SecretValue struct {
	String *string
	Binary []byte
}
type CreateInput struct {
	Name, ClientRequestToken, Description, KmsKeyID string
	Value                                           SecretValue
	Tags                                            map[string]string
}
type PutInput struct {
	SecretID, ClientRequestToken string
	Value                        SecretValue
	VersionStages                []string
}
type UpdateInput struct {
	SecretID, ClientRequestToken string
	Value                        SecretValue
	Description, KmsKeyID        *string
}
type StageInput struct{ SecretID, VersionStage, MoveToVersionID, RemoveFromVersionID string }

type secretVersion struct {
	value    SecretValue
	hasValue bool
	created  time.Time
}
type secretState struct {
	arn, name, description, kmsKeyID string
	created, changed, rotated        time.Time
	tags                             map[string]string
	versions                         map[string]*secretVersion
	labels                           map[string]string
	rotationEnabled                  *bool
	rotationLambda                   string
	rotationRules                    *RotationRules
}
type SecretsStore struct {
	secrets           map[string]*secretState
	secretsByARN      map[string]*secretState // Exact ARN aliases of the same state, guarded by mu.
	region, accountID string
	mu                sync.RWMutex
	now               func() time.Time
}

func NewSecretsStore(region, accountID string) *SecretsStore {
	return &SecretsStore{secrets: map[string]*secretState{}, secretsByARN: map[string]*secretState{}, region: region, accountID: accountID, now: time.Now}
}

var secretName = regexp.MustCompile(`^[A-Za-z0-9/_+=.@-]+$`)

func validateSecretID(id string) error {
	if len(id) < 1 || len(id) > 2048 {
		return invalidParameter("SecretId must contain 1..2048 characters")
	}
	return nil
}
func validateToken(token string) error {
	if token != "" && (len(token) < 32 || len(token) > 64) {
		return invalidParameter("ClientRequestToken/VersionId must contain 32..64 characters")
	}
	return nil
}
func validateValue(value SecretValue, required bool) error {
	if value.String != nil && value.Binary != nil {
		return invalidParameter("Specify SecretString or SecretBinary, not both")
	}
	if value.String == nil && value.Binary == nil {
		if required {
			return invalidParameter("A secret value is required")
		}
		return nil
	}
	if value.String != nil && (!utf8.ValidString(*value.String) || len(*value.String) < 1 || len(*value.String) > 65536) {
		return invalidParameter("SecretString must contain 1..65536 bytes of UTF-8")
	}
	if value.Binary != nil && (len(value.Binary) < 1 || len(value.Binary) > 65536) {
		return invalidParameter("SecretBinary must contain 1..65536 bytes")
	}
	return nil
}
func validateStages(stages []string) error {
	if stages != nil && (len(stages) < 1 || len(stages) > 20) {
		return invalidParameter("VersionStages must contain 1..20 labels")
	}
	seen := map[string]bool{}
	for _, stage := range stages {
		if len(stage) < 1 || len(stage) > 256 || seen[stage] {
			return invalidParameter("Staging labels must be unique and contain 1..256 characters")
		}
		seen[stage] = true
	}
	return nil
}
func copyValue(value SecretValue) SecretValue {
	out := SecretValue{Binary: bytes.Clone(value.Binary)}
	if value.String != nil {
		text := *value.String
		out.String = &text
	}
	return out
}
func sameValue(a, b SecretValue) bool {
	if (a.String == nil) != (b.String == nil) || (a.Binary == nil) != (b.Binary == nil) {
		return false
	}
	return (a.String == nil || *a.String == *b.String) && bytes.Equal(a.Binary, b.Binary)
}
func cloneRules(rules *RotationRules) *RotationRules {
	if rules == nil {
		return nil
	}
	out := *rules
	if rules.AutomaticallyAfterDays != nil {
		days := *rules.AutomaticallyAfterDays
		out.AutomaticallyAfterDays = &days
	}
	return &out
}
func (state *secretState) snapshot(versionID string) *Secret {
	out := &Secret{ARN: state.arn, Name: state.name, Description: state.description, KmsKeyID: state.kmsKeyID, CreatedDate: state.created, LastChangedDate: state.changed, LastRotatedDate: state.rotated, Tags: maps.Clone(state.tags), VersionID: versionID, VersionIdsToStages: map[string][]string{}, RotationLambdaARN: state.rotationLambda, RotationRules: cloneRules(state.rotationRules)}
	if state.rotationEnabled != nil {
		enabled := *state.rotationEnabled
		out.RotationEnabled = &enabled
	}
	for label, id := range state.labels {
		out.VersionIdsToStages[id] = append(out.VersionIdsToStages[id], label)
	}
	for id, labels := range out.VersionIdsToStages {
		sort.Strings(labels)
		out.VersionIdsToStages[id] = labels
	}
	if version := state.versions[versionID]; version != nil && version.hasValue {
		out.HasValue = true
		out.StringValue = version.value.String != nil
		if out.StringValue {
			out.SecretString = *version.value.String
		}
		out.SecretBinary = bytes.Clone(version.value.Binary)
		out.VersionStages = append([]string{}, out.VersionIdsToStages[versionID]...)
		out.CreatedDate = version.created
	}
	return out
}
func (ss *SecretsStore) Create(input CreateInput) (*Secret, error) {
	if !secretName.MatchString(input.Name) || len(input.Name) > 512 {
		return nil, invalidParameter("Name is required and must contain valid secret-name characters")
	}
	if err := validateToken(input.ClientRequestToken); err != nil {
		return nil, err
	}
	if err := validateValue(input.Value, false); err != nil {
		return nil, err
	}
	if len(input.Description) > 2048 || len(input.KmsKeyID) > 2048 || len(input.Tags) > 50 {
		return nil, invalidParameter("Secret metadata exceeds its limit")
	}
	for key, value := range input.Tags {
		if len(key) < 1 || len(key) > 128 || len(value) > 256 {
			return nil, invalidParameter("Invalid secret tag")
		}
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if existing := ss.secrets[input.Name]; existing != nil {
		if v := existing.versions[input.ClientRequestToken]; v != nil && v.hasValue && sameValue(v.value, input.Value) {
			return existing.snapshot(input.ClientRequestToken), nil
		}
		return nil, &APIError{"ResourceExistsException", "Secret already exists"}
	}
	now := ss.now()
	state := &secretState{arn: fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:%s-%s", ss.region, ss.accountID, input.Name, uuid.NewString()[:6]), name: input.Name, description: input.Description, kmsKeyID: input.KmsKeyID, created: now, changed: now, tags: maps.Clone(input.Tags), versions: map[string]*secretVersion{}, labels: map[string]string{}}
	id := ""
	if input.Value.String != nil || input.Value.Binary != nil {
		id = requestedOrGeneratedVersionID(input.ClientRequestToken)
		state.versions[id] = &secretVersion{value: copyValue(input.Value), hasValue: true, created: now}
		state.labels["AWSCURRENT"] = id
	}
	ss.secrets[input.Name] = state
	ss.secretsByARN[state.arn] = state
	return state.snapshot(id), nil
}
func (ss *SecretsStore) GetValue(secretID, versionID, stage string) (*Secret, error) {
	if err := validateSecretID(secretID); err != nil {
		return nil, err
	}
	if err := validateToken(versionID); err != nil {
		return nil, err
	}
	if stage != "" && len(stage) > 256 {
		return nil, invalidParameter("Invalid VersionStage")
	}
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	state := ss.findLocked(secretID)
	if state == nil {
		return nil, notFound()
	}
	if stage != "" {
		selected := state.labels[stage]
		if selected == "" {
			return nil, notFound()
		}
		if versionID != "" && versionID != selected {
			return nil, invalidParameter("VersionId and VersionStage must identify the same version")
		}
		versionID = selected
	} else if versionID == "" {
		versionID = state.labels["AWSCURRENT"]
	}
	if version := state.versions[versionID]; version == nil || !version.hasValue {
		return nil, notFound()
	}
	return state.snapshot(versionID), nil
}
func (ss *SecretsStore) Describe(secretID string) (*Secret, error) {
	if err := validateSecretID(secretID); err != nil {
		return nil, err
	}
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	state := ss.findLocked(secretID)
	if state == nil {
		return nil, notFound()
	}
	return state.snapshot(""), nil
}
func (ss *SecretsStore) PutValue(input PutInput) (*Secret, error) {
	if err := validateSecretID(input.SecretID); err != nil {
		return nil, err
	}
	if err := validateToken(input.ClientRequestToken); err != nil {
		return nil, err
	}
	if err := validateValue(input.Value, true); err != nil {
		return nil, err
	}
	if err := validateStages(input.VersionStages); err != nil {
		return nil, err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(input.SecretID)
	if state == nil {
		return nil, notFound()
	}
	return ss.putLocked(state, input)
}
func (ss *SecretsStore) putLocked(state *secretState, input PutInput) (*Secret, error) {
	id := requestedOrGeneratedVersionID(input.ClientRequestToken)
	if existing := state.versions[id]; existing != nil && existing.hasValue {
		if !sameValue(existing.value, input.Value) {
			return nil, &APIError{"ResourceExistsException", "An existing secret version cannot be modified"}
		}
		return state.snapshot(id), nil
	}
	stages := slices.Clone(input.VersionStages)
	if stages == nil {
		stages = []string{"AWSCURRENT"}
	}
	first := true
	for _, version := range state.versions {
		if version.hasValue {
			first = false
			break
		}
	}
	if first && !slices.Contains(stages, "AWSCURRENT") {
		stages = append(stages, "AWSCURRENT")
	}
	labels := maps.Clone(state.labels)
	for _, stage := range stages {
		if stage != "AWSCURRENT" {
			labels[stage] = id
		}
	}
	// AWSPREVIOUS follows the former current version even when both native
	// labels occur in the same unordered VersionStages request.
	if slices.Contains(stages, "AWSCURRENT") {
		moveLabel(labels, "AWSCURRENT", id)
	}
	if len(labels) > 20 {
		return nil, &APIError{"LimitExceededException", "A secret supports at most 20 staging labels"}
	}
	created := ss.now()
	if reserved := state.versions[id]; reserved != nil {
		created = reserved.created
	}
	state.versions[id] = &secretVersion{value: copyValue(input.Value), hasValue: true, created: created}
	state.labels = labels
	state.changed = ss.now()
	return state.snapshot(id), nil
}
func moveLabel(labels map[string]string, label, id string) {
	old := labels[label]
	labels[label] = id
	if label == "AWSCURRENT" && old != "" && old != id {
		labels["AWSPREVIOUS"] = old
	}
}
func (ss *SecretsStore) Update(input UpdateInput) (*Secret, error) {
	if err := validateSecretID(input.SecretID); err != nil {
		return nil, err
	}
	if err := validateToken(input.ClientRequestToken); err != nil {
		return nil, err
	}
	if err := validateValue(input.Value, false); err != nil {
		return nil, err
	}
	if input.Description != nil && len(*input.Description) > 2048 || input.KmsKeyID != nil && len(*input.KmsKeyID) > 2048 {
		return nil, invalidParameter("Secret metadata exceeds its limit")
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(input.SecretID)
	if state == nil {
		return nil, notFound()
	}
	result := state.snapshot("")
	var err error
	if input.Value.String != nil || input.Value.Binary != nil {
		result, err = ss.putLocked(state, PutInput{SecretID: input.SecretID, ClientRequestToken: input.ClientRequestToken, Value: input.Value})
		if err != nil {
			return nil, err
		}
	}
	if input.Description != nil {
		state.description = *input.Description
	}
	if input.KmsKeyID != nil {
		state.kmsKeyID = *input.KmsKeyID
	}
	state.changed = ss.now()
	return state.snapshot(result.VersionID), nil
}
func (ss *SecretsStore) UpdateVersionStage(input StageInput) (*Secret, error) {
	if err := validateSecretID(input.SecretID); err != nil {
		return nil, err
	}
	if input.VersionStage == "" || len(input.VersionStage) > 256 {
		return nil, invalidParameter("VersionStage is required")
	}
	if err := validateToken(input.MoveToVersionID); err != nil {
		return nil, err
	}
	if err := validateToken(input.RemoveFromVersionID); err != nil {
		return nil, err
	}
	if input.MoveToVersionID == "" && input.RemoveFromVersionID == "" {
		return nil, invalidParameter("A target or source version is required")
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(input.SecretID)
	if state == nil {
		return nil, notFound()
	}
	labels := maps.Clone(state.labels)
	owner := labels[input.VersionStage]
	if input.RemoveFromVersionID != "" && owner != input.RemoveFromVersionID {
		return nil, invalidParameter("RemoveFromVersionId does not own the label")
	}
	if input.MoveToVersionID != "" {
		version := state.versions[input.MoveToVersionID]
		if version == nil {
			return nil, notFound()
		}
		if input.VersionStage == "AWSCURRENT" && !version.hasValue {
			return nil, invalidRequest("An empty pending version cannot become current")
		}
		if owner != "" && owner != input.MoveToVersionID && input.RemoveFromVersionID == "" {
			return nil, invalidParameter("RemoveFromVersionId is required to move an attached label")
		}
		moveLabel(labels, input.VersionStage, input.MoveToVersionID)
	} else {
		delete(labels, input.VersionStage)
	}
	if len(labels) > 20 {
		return nil, &APIError{"LimitExceededException", "A secret supports at most 20 staging labels"}
	}
	state.labels = labels
	state.changed = ss.now()
	return state.snapshot(""), nil
}
func requestedOrGeneratedVersionID(id string) string {
	if id != "" {
		return id
	}
	return uuid.NewString()
}
func (ss *SecretsStore) findLocked(id string) *secretState {
	if state := ss.secrets[id]; state != nil {
		return state
	}
	return ss.secretsByARN[id]
}

// Compatibility entry points are used by existing embedded hosts and tests.
func (ss *SecretsStore) CreateSecret(name, value, id string, tags map[string]string) (*Secret, error) {
	return ss.Create(CreateInput{Name: name, ClientRequestToken: id, Value: SecretValue{String: &value}, Tags: tags})
}
func (ss *SecretsStore) GetSecretValue(id, version string) (*Secret, error) {
	return ss.GetValue(id, version, "")
}
func (ss *SecretsStore) PutSecretValue(id, value, version string) (*Secret, error) {
	return ss.PutValue(PutInput{SecretID: id, ClientRequestToken: version, Value: SecretValue{String: &value}})
}
func (ss *SecretsStore) UpdateSecret(id, value, version string) (*Secret, error) {
	return ss.Update(UpdateInput{SecretID: id, ClientRequestToken: version, Value: SecretValue{String: &value}})
}
func (ss *SecretsStore) DeleteSecret(id string, force bool) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(id)
	if state == nil {
		return notFound()
	}
	delete(ss.secrets, state.name)
	delete(ss.secretsByARN, state.arn)
	return nil
}
