// Development harness adapter: an exclusive retained owner with separate suite
// and callback endpoints. Native services keep their ordinary admission policy.
package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
)

// devRetainedHTTP keeps accepted callback chains usable before the normal,
// irreversible runtime/HTTP shutdown. It owns no application fixture cleanup.
type devRetainedHTTP struct {
	owner     *devquiescence.Coordinator
	callbacks *http.Server
}

func validateRetainedConfig(cfg config) error {
	if cfg.retainedCallbackPort == 0 {
		if cfg.retainedCleanupFunctions != "" {
			return errors.New("retained-owner-cleanup-functions requires retained-owner-callback-port")
		}
		return nil
	}
	if cfg.port < 1 || cfg.port > 65535 {
		return errors.New("retained-owner mode requires port 1..65535")
	}
	if cfg.retainedCallbackPort < 1 || cfg.retainedCallbackPort > 65535 || cfg.retainedCallbackPort == cfg.port {
		return errors.New("retained-owner-callback-port must be 1..65535 and differ from port")
	}
	if cfg.consumersFile != "" {
		return errors.New("retained-owner mode does not support legacy consumers")
	}
	return nil
}

func loadRetainedFunctions(filename, workDir string, retained *devquiescence.Coordinator) (*lambdaservice.Service, error) {
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(workDir, filename)
	}
	recipe, err := lambdaservice.LoadConfig(filename)
	if err != nil {
		return nil, err
	}
	if retained != nil {
		recipe.DevActivity = retained
	}
	return lambdaservice.NewService(recipe, workDir)
}

func newRetainedHTTP(owner *devquiescence.Coordinator, router http.Handler, callbackPort int, cleanup http.Handler) (http.Handler, *devRetainedHTTP) {
	if cleanup == nil {
		cleanup = retainedCleanupHandler(router)
	}
	controls := devquiescence.NewHandler(owner)
	source := owner.Wrap(devquiescence.Source, router, nil)
	ingress := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == devquiescence.ControlPath || strings.HasPrefix(r.URL.Path, devquiescence.ControlPath+"/") {
			controls.ServeHTTP(w, r)
			return
		}
		source.ServeHTTP(w, r)
	})
	callbacks := &http.Server{
		Addr:        fmt.Sprintf("127.0.0.1:%d", callbackPort),
		Handler:     owner.Wrap(devquiescence.Callback, router, cleanup),
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 16,
	}
	return ingress, &devRetainedHTTP{owner: owner, callbacks: callbacks}
}

// No autonomous source can escape the retained activity owner. These profile
// refusals remain explicit dev adapters; normal AWS service cores are unchanged.
func retainedServices(services server.Services) server.Services {
	services.Secrets = retainedSecretsHandler{delegate: services.Secrets}
	services.Messaging = retainedMessagingHandler{delegate: services.Messaging}
	return services
}

func retainedUnsupportedHTTP(feature string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		devquiescence.WriteAdmissionError(w, r, fmt.Errorf("%s is not supported by the retained-owner profile", feature))
	})
}

type retainedSecretsHandler struct{ delegate server.ActionHandler }

func (h retainedSecretsHandler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	if action == "RotateSecret" {
		retainedUnsupportedHTTP("Secrets rotation").ServeHTTP(w, r)
		return
	}
	h.delegate.ServeAction(w, r, action)
}

type retainedMessagingHandler struct{ delegate server.MessagingHandler }

func (h retainedMessagingHandler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	if action == "StartMessageMoveTask" {
		retainedUnsupportedHTTP("SQS message-move tasks").ServeHTTP(w, r)
		return
	}
	h.delegate.ServeAction(w, r, action)
}
func (h retainedMessagingHandler) ServeQuery(w http.ResponseWriter, r *http.Request, action string) {
	if action == "StartMessageMoveTask" {
		devquiescence.WriteAdmissionError(w, r, errors.New("SQS message-move tasks are not supported by the retained-owner profile"))
		return
	}
	h.delegate.ServeQuery(w, r, action)
}

// Held cleanup permits exact native deletions. Declared Lambda cleanup is
// composed separately by retainedCleanupInvocations.
// Parsing happens only after the envelope received immutable CleanupOnly mode;
// resume can never turn a delayed request into work-producing admission.
func retainedCleanupHandler(router http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && retainedRESTDeletion(r.URL.Path) {
			router.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodPost {
			retainedUnsupportedHTTP("Work-producing admission while held").ServeHTTP(w, r)
			return
		}
		if target := r.Header.Get("X-Amz-Target"); target != "" {
			allowed := strings.HasPrefix(target, "AmazonSQS.") && retainedSQSDeletion(awsprotocol.TargetAction(target)) ||
				strings.HasPrefix(target, "Firehose_") && awsprotocol.TargetAction(target) == "DeleteDeliveryStream"
			if (r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, "/queue/")) && allowed {
				router.ServeHTTP(w, r)
				return
			}
		} else if r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, "/queue/") {
			r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
			if r.ParseForm() == nil {
				action, version := r.FormValue("Action"), r.FormValue("Version")
				allowed := version == "2010-03-31" && (action == "DeleteTopic" || action == "Unsubscribe") || version == "2012-11-05" && retainedSQSDeletion(action)
				if allowed {
					router.ServeHTTP(w, r)
					return
				}
			}
		}
		retainedUnsupportedHTTP("Work-producing admission while held").ServeHTTP(w, r)
	})
}
func retainedSQSDeletion(action string) bool {
	switch action {
	case "DeleteQueue", "DeleteMessage", "DeleteMessageBatch", "PurgeQueue":
		return true
	default:
		return false
	}
}

func (listener *eventBusListener) httpServers() []*http.Server {
	servers := []*http.Server{listener.server}
	if listener.devRetained != nil {
		servers = append(servers, listener.devRetained.callbacks)
	}
	return servers
}
func (listener *eventBusListener) joinRetained(ctx context.Context) error {
	if listener.devRetained == nil {
		return nil
	}
	listener.devRetained.owner.Shutdown()
	_, err := listener.devRetained.owner.Quiesce(ctx)
	if err == nil {
		return nil
	}
	// Final owner shutdown may abort after a failed join; resumable controls
	// never do. Keep AWS peers/stores available while native cleanup joins.
	abortCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cancel()
	abortErr := listener.abortRetainedOwners(abortCtx)
	// Once native admission and children have joined, interrupt stalled HTTP
	// reads/writes, then join the envelopes themselves. Never infer that socket
	// closure means a received SNS publication has stopped.
	var transportErrs []error
	for _, server := range listener.httpServers() {
		transportErrs = append(transportErrs, server.Close())
	}
	remoteErr := listener.devRetained.owner.AbandonRemoteSources()
	_, joinedErr := listener.devRetained.owner.Quiesce(context.WithoutCancel(ctx))
	return fmt.Errorf("join retained owner; resources retained: %w", errors.Join(err, abortErr, errors.Join(transportErrs...), remoteErr, joinedErr))
}

// Permanent abort is a final failed shutdown policy, never a resumable control.
// Every source/runner is stopped and actually rejoined while peers/stores are
// still live. Caller budgets may fail without abandoning the cleanup owner.
func (listener *eventBusListener) abortRetainedOwners(ctx context.Context) error {
	if listener.owned == nil {
		return nil
	}
	var failures []error
	for _, item := range []struct {
		name  string
		owner contextCloser
	}{
		{"SQS mappings", listener.owned.mappings},
		{"Scheduler", listener.owned.scheduler},
		{"Cognito triggers", listener.owned.triggers},
		{"Lambda", listener.owned.functions},
	} {
		if item.owner == nil {
			continue
		}
		first := item.owner.Close(ctx)
		joined := item.owner.Close(context.WithoutCancel(ctx))
		if first != nil || joined != nil {
			failures = append(failures, fmt.Errorf("abort/join %s: %w", item.name, errors.Join(first, joined)))
		}
	}
	if listener.owned.firehose != nil {
		if err := listener.owned.firehose.DevAbortJoin(context.WithoutCancel(ctx)); err != nil {
			failures = append(failures, fmt.Errorf("abort/join Firehose: %w", err))
		}
	}
	return errors.Join(failures...)
}
