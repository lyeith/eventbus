package lambda

import (
	"errors"

	"github.com/lyeith/eventbus/internal/localexec"
)

// Preserve this service's startup refusal while the shared owner defines which
// operating systems can safely own handler descendants.
func processGroupsSupported() error {
	if localexec.Supported() != nil {
		return errors.New("local Lambda runtimes require Linux or macOS process groups")
	}
	return nil
}
