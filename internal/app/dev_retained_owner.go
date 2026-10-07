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
		return nil
	}
	if cfg.port < 1 || cfg.port > 65535 {
		return errors.New("retained-owner mode requires port 1..65535")
	}
	if cfg.retainedCallbackPort < 1 || cfg.retainedCallbackPort > 65535 || cfg.retainedCallbackPort == cfg.port {
		return errors.New("retained-owner-callback-port must be 1..65535 and differ from port")
	}
	if cfg.consumersFile != "" || cfg.cognitoTriggers != "" {
		return errors.New("retained-owner mode does not support legacy consumers or Cognito trigger runners")
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

func newRetainedHTTP(owner *devquiescence.Coordinator, router http.Handler, callbackPort int) (http.Handler, *devRetainedHTTP) {
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
		Handler:     owner.Wrap(devquiescence.Callback, router, retainedCleanupHandler(router)),
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 16,
	}
	return ingress, &devRetainedHTTP{owner: owner, callbacks: callbacks}
}

// No autonomous source can escape the retained activity owner. These profile
// refusals remain explicit dev adapters; normal AWS service cores are unchanged.
func retainedServices(services server.Services) server.Services {
	services.EventSources = retainedUnsupportedHTTP("SQS event-source mappings")
	services.Scheduler = retainedUnsupportedHTTP("Scheduler")
	services.Firehose = retainedUnsupportedAction{feature: "Firehose"}
	services.Secrets = retainedSecretsHandler{delegate: services.Secrets}
	services.Messaging = retainedMessagingHandler{delegate: services.Messaging}
	return services
}

func retainedUnsupportedHTTP(feature string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		devquiescence.WriteAdmissionError(w, r, fmt.Errorf("%s is not supported by the retained-owner profile", feature))
	})
}

type retainedUnsupportedAction struct{ feature string }

func (h retainedUnsupportedAction) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	retainedUnsupportedHTTP(h.feature).ServeHTTP(w, r)
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
	h.delegate.ServeAction(w, r, action)
}
func (h retainedMessagingHandler) ServeQuery(w http.ResponseWriter, r *http.Request, action string) {
	if action == "Subscribe" && r.FormValue("Protocol") == "firehose" {
		devquiescence.WriteAdmissionError(w, r, errors.New("Firehose subscriptions are not supported by the retained-owner profile"))
		return
	}
	h.delegate.ServeQuery(w, r, action)
}

// Held cleanup is limited to exact, synchronous SNS/SQS deletion operations.
// Parsing happens only after the envelope received immutable CleanupOnly mode;
// resume can never turn a delayed request into work-producing admission.
func retainedCleanupHandler(router http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			retainedUnsupportedHTTP("Work-producing admission while held").ServeHTTP(w, r)
			return
		}
		if target := r.Header.Get("X-Amz-Target"); target != "" {
			if (r.URL.Path == "/" || strings.HasPrefix(r.URL.Path, "/queue/")) && strings.HasPrefix(target, "AmazonSQS.") && retainedSQSDeletion(awsprotocol.TargetAction(target)) {
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
	var abortErr error
	if listener.owned != nil && listener.owned.functions != nil {
		abortErr = listener.owned.functions.Close(abortCtx)
	}
	// Once native admission and children have joined, interrupt stalled HTTP
	// reads/writes, then join the envelopes themselves. Never infer that socket
	// closure means a received SNS publication has stopped.
	var transportErrs []error
	for _, server := range listener.httpServers() {
		transportErrs = append(transportErrs, server.Close())
	}
	_, joinedErr := listener.devRetained.owner.Quiesce(context.WithoutCancel(ctx))
	return fmt.Errorf("join retained owner; resources retained: %w", errors.Join(err, abortErr, errors.Join(transportErrs...), joinedErr))
}
