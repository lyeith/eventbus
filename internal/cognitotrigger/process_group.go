package cognitotrigger

import (
	"errors"

	"github.com/lyeith/eventbus/internal/localexec"
)

// Preserve this service's startup refusal while the shared owner defines which
// operating systems can safely own handler descendants.
func processGroupsSupported() error {
	if localexec.Supported() != nil {
		return errors.New("Cognito trigger execution requires Linux or macOS process groups")
	}
	return nil
}
