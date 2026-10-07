package lambda

import (
	"net/http"

	"github.com/lyeith/eventbus/internal/devactivity"
)

// DevActivity is the optional application-owned lifecycle observer. A successful
// BeginActivity returns a non-nil release, which Lambda calls once after all
// owned work has joined. Release receives evidence or cleanup uncertainty, never
// an ordinary handler error. Callbacks may only update their own coordinator:
// they run under Lambda's mutex and must not call back into this service.
// This port is configured before startup and is not part of the AWS API.
type DevActivity = devactivity.Activity

func (service *Service) beginActivityLocked(kind, requestID string) (func(error), error) {
	if service.devActivity == nil {
		return nil, nil
	}
	release, err := service.devActivity.BeginActivity(kind, requestID)
	if err != nil || release == nil {
		return nil, invocationError(http.StatusServiceUnavailable, "ServiceException", "Lambda invocation is unavailable during developer lifecycle control")
	}
	return release, nil
}
