package lambda

import (
	"path/filepath"
	"strings"
	"time"
)

// FunctionConfiguration is the read-only GetFunction subset for registered
// targets. Runtime is the configured execution family, not an invented AWS
// runtime version. Handler is a public basename/export reference; local paths,
// command arguments and environment variables never enter this projection.
type FunctionConfiguration struct {
	FunctionName string `json:"FunctionName"`
	FunctionARN  string `json:"FunctionArn"`
	Runtime      string `json:"Runtime"`
	Handler      string `json:"Handler,omitempty"`
	Timeout      int64  `json:"Timeout"`
	Version      string `json:"Version"`
	State        string `json:"State"`
}

func (service *Service) GetFunction(name, qualifier string) (FunctionConfiguration, error) {
	entry, resolved, err := service.resolveTarget(name, qualifier)
	if err != nil {
		return FunctionConfiguration{}, err
	}
	service.mu.Lock()
	closed := service.closed
	service.mu.Unlock()
	if closed {
		return FunctionConfiguration{}, invocationError(503, "ServiceException", "Lambda service is closing")
	}
	base, _, _ := strings.Cut(resolved, ":")
	metadata := FunctionConfiguration{
		FunctionName: base,
		FunctionARN:  functionARN(entry, prepareInvocation(entry, resolved, InvokeInput{FunctionName: name, Qualifier: qualifier}, "")),
		Runtime:      entry.runtime,
		// The registered local deadline may be fractional; AWS exposes seconds.
		Timeout: int64((entry.timeout + time.Second - 1) / time.Second),
		Version: executedVersion(resolved),
		State:   "Active",
	}
	if entry.exported != "" {
		module := filepath.Base(entry.module)
		switch filepath.Ext(module) {
		case ".py", ".js", ".mjs", ".cjs":
			module = strings.TrimSuffix(module, filepath.Ext(module))
		}
		metadata.Handler = module + "." + entry.exported
	}
	return metadata, nil
}
