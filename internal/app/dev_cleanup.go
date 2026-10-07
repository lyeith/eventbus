package app

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
)

// Declared cleanup targets remain native registered handlers. The application
// owns their authentication, signed payload, exact resource scope and effects.
func retainedCleanupInvocations(owner *devquiescence.Coordinator, functions *lambdaservice.Service, declarations, region, accountID string, router http.Handler) (http.Handler, error) {
	base := retainedCleanupHandler(router)
	if declarations == "" {
		return base, nil
	}
	if functions == nil {
		return nil, errors.New("retained cleanup targets require lambda-functions")
	}
	allowed := make(map[string]bool)
	for _, declaration := range strings.Split(declarations, ",") {
		declaration = strings.TrimSpace(declaration)
		if declaration == "" {
			return nil, errors.New("retained cleanup targets must be nonempty exact function references")
		}
		target, err := functions.DescribeDevTarget(declaration, "")
		if err != nil {
			return nil, fmt.Errorf("retained cleanup target %q: %w", declaration, err)
		}
		if !retainedCleanupNamespace(target, region, accountID) {
			return nil, fmt.Errorf("retained cleanup target %q is outside the configured Lambda namespace", declaration)
		}
		if allowed[target.FunctionName] {
			return nil, fmt.Errorf("duplicate retained cleanup target %q", target.FunctionName)
		}
		allowed[target.FunctionName] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target, native := functions.DevRequestResponseTarget(r)
		if !native || !allowed[target.FunctionName] || !retainedCleanupNamespace(target, region, accountID) {
			base.ServeHTTP(w, r)
			return
		}
		// This handler is dispatched only with an immutable CleanupOnly HTTP
		// envelope. Its held generation cannot resume while that envelope lives.
		generation := owner.Snapshot().Generation
		complete, err := owner.BeginCleanup(generation, "lambda_cleanup", "")
		if err != nil {
			devquiescence.WriteAdmissionError(w, r, err)
			return
		}
		defer func() {
			if value := recover(); value != nil {
				complete(devquiescence.ErrEvidence)
				panic(value)
			}
			complete(nil)
		}()
		router.ServeHTTP(w, r)
	}), nil
}

func retainedRESTDeletion(path string) bool {
	for _, prefix := range []string{"/2015-03-31/event-source-mappings/", "/schedules/", "/schedule-groups/"} {
		if suffix, matched := strings.CutPrefix(path, prefix); matched && suffix != "" && !strings.Contains(suffix, "/") {
			return true
		}
	}
	return false
}

// Native Lambda resolves registered names independently of the broker's AWS
// namespace. Retained cleanup additionally binds full resource references to
// this exclusive app owner; ordinary Invoke routing remains service-owned.
func retainedCleanupNamespace(target lambdaservice.DevInvokeTarget, region, accountID string) bool {
	return (target.Region == "" || target.Region == region) && (target.AccountID == "" || target.AccountID == accountID)
}
