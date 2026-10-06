package ssm

import (
	"fmt"
	"sync"
)

type SSMStore struct {
	parameters map[string]*SSMParameter // name → parameter
	mu         sync.RWMutex
}

type SSMParameter struct {
	Name  string
	Value string
	Type  string // String, StringList, SecureString
}

func NewSSMStore() *SSMStore {
	return &SSMStore{
		parameters: make(map[string]*SSMParameter),
	}
}

func (s *SSMStore) PutParameter(name, value, paramType string, overwrite bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.parameters[name]; exists && !overwrite {
		return fmt.Errorf("ParameterAlreadyExists")
	}

	s.parameters[name] = &SSMParameter{
		Name:  name,
		Value: value,
		Type:  paramType,
	}
	return nil
}

func (s *SSMStore) GetParameter(name string) (*SSMParameter, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	param, ok := s.parameters[name]
	if !ok {
		return nil, fmt.Errorf("ParameterNotFound")
	}
	return param, nil
}

func (s *SSMStore) GetParametersByPath(path string) []*SSMParameter {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*SSMParameter
	for name, param := range s.parameters {
		if len(name) > len(path) && name[:len(path)] == path {
			result = append(result, param)
		}
	}
	return result
}

func (s *SSMStore) DeleteParameter(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.parameters, name)
}
