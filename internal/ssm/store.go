package ssm

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	errParameterAlreadyExists   = errors.New("ParameterAlreadyExists")
	errParameterNotFound        = errors.New("ParameterNotFound")
	errParameterVersionNotFound = errors.New("ParameterVersionNotFound")
	errHierarchyTypeMismatch    = errors.New("HierarchyTypeMismatchException")
	errInvalidNextToken         = errors.New("InvalidNextToken")
)

type SSMStore struct {
	parameters map[string][]storedParameter // immutable versions, oldest first
	names      []string                     // raw names, sorted; updated with parameters under mu
	mu         sync.RWMutex
	sealer     cipher.AEAD
	cursorKey  [32]byte
}

type storedParameter struct {
	parameter   SSMParameter
	sealedValue []byte // plaintext is absent for SecureString
}

type SSMParameter struct {
	Name             string
	Value            string
	Type             string
	Version          int64
	LastModifiedDate time.Time
	DataType         string
	Description      string
}

// NewSSMStore owns an ephemeral local sealing key. This models SecureString
// read choices without implementing or claiming AWS KMS key/policy behavior.
func NewSSMStore() *SSMStore {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		panic(err)
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	sealer, err := cipher.NewGCMWithRandomNonce(block)
	if err != nil {
		panic(err)
	}
	store := &SSMStore{parameters: make(map[string][]storedParameter), sealer: sealer}
	if _, err := rand.Read(store.cursorKey[:]); err != nil {
		panic(err)
	}
	return store
}

func (s *SSMStore) PutParameter(name, value, paramType string, overwrite bool) error {
	_, err := s.putParameterValue(SSMParameter{Name: name, Value: value, Type: paramType}, overwrite, nil)
	return err
}

// PutParameterValue returns the exact written version under the same lock,
// rather than rereading a version that a concurrent overwrite may replace.
func (s *SSMStore) PutParameterValue(parameter SSMParameter, overwrite bool) (*SSMParameter, error) {
	return s.putParameterValue(parameter, overwrite, &parameter.Description)
}

func (s *SSMStore) putParameterValue(parameter SSMParameter, overwrite bool, description *string) (*SSMParameter, error) {
	name, err := validateParameterName(parameter.Name)
	if err != nil {
		return nil, err
	}
	parameter.Name = name
	first, _, _ := strings.Cut(strings.TrimPrefix(name, "/"), "/")
	first = strings.ToLower(first)
	if strings.HasPrefix(first, "aws") || strings.HasPrefix(first, "ssm") {
		return nil, newParameterError("ValidationException", "Names cannot begin with aws or ssm")
	}
	if description != nil && utf8.RuneCountInString(*description) > 1024 {
		return nil, newParameterError("ValidationException", "Description can contain at most 1024 characters")
	}
	if len(parameter.Value) == 0 || len(parameter.Value) > 4096 {
		return nil, newParameterError("ValidationException", "Standard parameter Value must contain 1 to 4096 UTF-8 bytes")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	history := s.parameters[name]
	if len(history) > 0 && !overwrite {
		return nil, errParameterAlreadyExists
	}
	if parameter.Type == "" {
		parameter.Type = "String"
		if len(history) > 0 {
			parameter.Type = history[len(history)-1].parameter.Type
		}
	}
	switch parameter.Type {
	case "String", "StringList", "SecureString":
	default:
		return nil, newParameterError("UnsupportedParameterType", "Type must be String, StringList, or SecureString")
	}
	if len(history) > 0 && parameter.Type != history[len(history)-1].parameter.Type {
		return nil, errHierarchyTypeMismatch
	}
	if parameter.Type == "StringList" {
		parts := strings.Split(parameter.Value, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
			if parts[i] == "" {
				return nil, newParameterError("ValidationException", "StringList items must be non-empty")
			}
		}
		parameter.Value = strings.Join(parts, ",")
	}
	if parameter.DataType == "" {
		parameter.DataType = "text"
	}
	if parameter.DataType != "text" {
		return nil, newParameterError("ValidationException", "Only the text DataType is supported")
	}
	if description != nil {
		parameter.Description = *description
	} else if len(history) > 0 {
		parameter.Description = history[len(history)-1].parameter.Description
	}
	parameter.Version = 1
	if len(history) > 0 {
		parameter.Version = history[len(history)-1].parameter.Version + 1
	}
	parameter.LastModifiedDate = time.Now().UTC()
	stored := storedParameter{parameter: parameter}
	if parameter.Type == "SecureString" {
		stored.sealedValue = s.sealer.Seal(nil, nil, []byte(parameter.Value), parameterAAD(parameter))
		stored.parameter.Value = ""
	}
	history = append(history, stored)
	if len(history) > 100 {
		// Parameter Store retains the latest 100 versions. Labels are not
		// supported, so no oldest-version label can prevent this deletion.
		history = append([]storedParameter(nil), history[len(history)-100:]...)
	}
	if len(s.parameters[name]) == 0 {
		index := sort.SearchStrings(s.names, name)
		s.names = slices.Insert(s.names, index, name)
	}
	s.parameters[name] = history
	result := parameter // independent public value snapshot, never cipher material
	return &result, nil
}

// GetParameter retains the internal plaintext snapshot contract for fixtures.
func (s *SSMStore) GetParameter(name string) (*SSMParameter, error) {
	return s.GetParameterValue(name, 0, true)
}

func (s *SSMStore) GetParameterValue(name string, version int64, withDecryption bool) (*SSMParameter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	history := s.parameters[name]
	if len(history) == 0 {
		return nil, errParameterNotFound
	}
	selected := history[len(history)-1]
	if version != 0 {
		found := false
		for _, value := range history {
			if value.parameter.Version == version {
				selected, found = value, true
				break
			}
		}
		if !found {
			return nil, errParameterVersionNotFound
		}
	}
	return s.snapshot(selected, withDecryption)
}

func parameterAAD(parameter SSMParameter) []byte {
	return []byte(parameter.Name + "\x00" + strconv.FormatInt(parameter.Version, 10))
}

func (s *SSMStore) snapshot(stored storedParameter, withDecryption bool) (*SSMParameter, error) {
	result := stored.parameter
	if result.Type == "SecureString" {
		if withDecryption {
			value, err := s.sealer.Open(nil, nil, stored.sealedValue, parameterAAD(result))
			if err != nil {
				return nil, fmt.Errorf("open locally sealed parameter: %w", err)
			}
			result.Value = string(value)
		} else {
			result.Value = base64.StdEncoding.EncodeToString(stored.sealedValue)
		}
	}
	return &result, nil
}

func canonicalPath(path string) string {
	path = strings.TrimRight(path, "/")
	if path == "" {
		return "/"
	}
	return path
}

// GetParametersByPath retains the internal recursive plaintext snapshot API.
// Native reads use ListParametersByPath for recursive selection and pagination.
func (s *SSMStore) GetParametersByPath(path string) []*SSMParameter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := s.pathNames(path, true, "", 0)
	result := make([]*SSMParameter, 0, len(names))
	for _, name := range names {
		history := s.parameters[name]
		parameter, err := s.snapshot(history[len(history)-1], true)
		if err != nil {
			panic(err)
		} // private in-memory invariant; native API reports errors
		result = append(result, parameter)
	}
	return result
}

type parameterNameRange struct {
	start, end int
	prefix     string
}

// pathNameRanges selects only the matching raw-name ranges. Bare and slash
// names share hierarchy semantics but retain their original lexical order.
// Callers hold mu for both the index and the associated stored versions.
func (s *SSMStore) pathNameRanges(path string) []parameterNameRange {
	path = canonicalPath(path)
	if path == "/" {
		return []parameterNameRange{{start: 0, end: len(s.names)}}
	}
	if !strings.HasPrefix(path, "/") {
		return nil
	}
	prefix := path + "/"
	ranges := make([]parameterNameRange, 0, 2)
	for _, rawPrefix := range []string{prefix, strings.TrimPrefix(prefix, "/")} {
		// Only the first range may contain slash names. Invalid internal
		// double-slash paths must not accidentally match ordinary names.
		if rawPrefix != prefix && strings.HasPrefix(rawPrefix, "/") {
			continue
		}
		start := sort.SearchStrings(s.names, rawPrefix)
		end := start + sort.Search(len(s.names)-start, func(offset int) bool {
			return !strings.HasPrefix(s.names[start+offset], rawPrefix)
		})
		if start != end {
			ranges = append(ranges, parameterNameRange{start: start, end: end, prefix: rawPrefix})
		}
	}
	if len(ranges) == 2 && ranges[1].start < ranges[0].start {
		ranges[0], ranges[1] = ranges[1], ranges[0]
	}
	return ranges
}

// pathNames returns detached names after the cursor. A positive limit stops
// after the page and its lookahead; zero selects all names for the fixture API.
func (s *SSMStore) pathNames(path string, recursive bool, lastName string, limit int) []string {
	capacity := limit
	if capacity == 0 {
		capacity = 10
	}
	names := make([]string, 0, capacity)
	after := 0
	if lastName != "" {
		after = sort.Search(len(s.names), func(index int) bool { return s.names[index] > lastName })
	}
	for _, span := range s.pathNameRanges(path) {
		for index := max(span.start, after); index < span.end; {
			name := s.names[index]
			remainder := name[len(span.prefix):]
			if span.prefix == "" {
				remainder = strings.TrimPrefix(remainder, "/")
			}
			if !recursive {
				first, _, nested := strings.Cut(remainder, "/")
				if nested {
					// All descendants of this immediate child are contiguous.
					// Skip the subtree rather than revisiting it on every page.
					subtree := name[:len(name)-len(remainder)+len(first)+1]
					index += sort.Search(span.end-index, func(offset int) bool {
						return !strings.HasPrefix(s.names[index+offset], subtree)
					})
					continue
				}
			}
			names = append(names, name)
			if limit > 0 && len(names) == limit {
				return names
			}
			index++
		}
	}
	return names
}

type parameterCursor struct {
	Path           string `json:"path"`
	Recursive      bool   `json:"recursive"`
	WithDecryption bool   `json:"with_decryption"`
	LastName       string `json:"last_name"`
}

func (s *SSMStore) ListParametersByPath(path string, recursive, withDecryption bool, maxResults int, nextToken string) ([]*SSMParameter, string, error) {
	if maxResults < 1 || maxResults > 10 {
		return nil, "", newParameterError("ValidationException", "MaxResults must be between 1 and 10")
	}
	path = canonicalPath(path)
	cursor := parameterCursor{Path: path, Recursive: recursive, WithDecryption: withDecryption}
	if nextToken != "" {
		if len(nextToken) > 4096 {
			return nil, "", errInvalidNextToken
		}
		raw, err := base64.RawURLEncoding.Strict().DecodeString(nextToken)
		if err != nil || len(raw) <= sha256.Size {
			return nil, "", errInvalidNextToken
		}
		payload, signature := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
		mac := hmac.New(sha256.New, s.cursorKey[:])
		mac.Write(payload)
		if !hmac.Equal(mac.Sum(nil), signature) {
			return nil, "", errInvalidNextToken
		}
		var previous parameterCursor
		if json.Unmarshal(payload, &previous) != nil || previous.Path != path || previous.Recursive != recursive || previous.WithDecryption != withDecryption || previous.LastName == "" {
			return nil, "", errInvalidNextToken
		}
		cursor.LastName = previous.LastName
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := s.pathNames(path, recursive, cursor.LastName, maxResults+1)
	end := min(maxResults, len(names))
	parameters := make([]*SSMParameter, 0, end)
	for _, name := range names[:end] {
		history := s.parameters[name]
		parameter, err := s.snapshot(history[len(history)-1], withDecryption)
		if err != nil {
			return nil, "", err
		}
		parameters = append(parameters, parameter)
	}
	token := ""
	if end < len(names) {
		cursor.LastName = names[end-1]
		payload, err := json.Marshal(cursor)
		if err != nil {
			return nil, "", err
		}
		mac := hmac.New(sha256.New, s.cursorKey[:])
		mac.Write(payload)
		token = base64.RawURLEncoding.EncodeToString(append(payload, mac.Sum(nil)...))
	}
	return parameters, token, nil
}

func (s *SSMStore) DeleteParameter(name string) {
	s.DeleteParameterIfExists(name)
}

func (s *SSMStore) DeleteParameterIfExists(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.parameters[name]; !exists {
		return false
	}
	delete(s.parameters, name)
	index := sort.SearchStrings(s.names, name)
	s.names = slices.Delete(s.names, index, index+1)
	return true
}
