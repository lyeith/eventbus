package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Options struct {
	NoAuth        bool
	FrontendDir   string
	FrontendProxy string
	Logger        zerolog.Logger
}

type compiledRoute struct {
	config   RouteConfig
	template pathTemplate
	proxy    *httputil.ReverseProxy
}
type Gateway struct {
	config      Config
	options     Options
	routes      []compiledRoute
	authorizers map[string]*lambdaAuthorizer
	frontend    http.Handler
	transport   *http.Transport
	connections *connectionTracker
	closed      atomic.Bool
}
type integrationRequest struct {
	target  *url.URL
	headers http.Header
}
type integrationRequestKey struct{}

func New(cfg Config, options Options) (*Gateway, error) {
	// Copy caller maps/slices so validation defaults and per-request state cannot
	// mutate fixtures owned by the application.
	cfg = cfg.clone()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	connections := newConnectionTracker()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = connections.DialContext
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 20
	result := &Gateway{config: cfg, options: options, transport: transport, connections: connections, authorizers: make(map[string]*lambdaAuthorizer)}
	authorizerClient := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for name, configuration := range cfg.Authorizers {
		result.authorizers[name] = &lambdaAuthorizer{config: configuration, client: authorizerClient, cache: make(map[string]cachedAuthorization), now: time.Now}
	}
	for _, configuration := range cfg.Routes {
		template, _ := parseTemplate(configuration.Path)
		proxy := &httputil.ReverseProxy{
			Transport: transport,
			Rewrite: func(request *httputil.ProxyRequest) {
				mapped := request.In.Context().Value(integrationRequestKey{}).(integrationRequest)
				upgrade, trailers := request.Out.Header.Get("Upgrade"), request.Out.Header.Get("Te")
				request.Out.URL = mapped.target
				request.Out.Host = mapped.target.Host
				request.Out.Header = mapped.headers
				if trailers != "" {
					request.Out.Header.Set("Te", trailers)
				}
				if upgrade != "" {
					request.Out.Header.Set("Connection", "Upgrade")
					request.Out.Header.Set("Upgrade", upgrade)
				}
				request.SetXForwarded()
			},
			ErrorLog: log.New(io.Discard, "", 0),
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, _ error) {
				if !errors.Is(r.Context().Err(), context.Canceled) {
					writeGatewayError(w, http.StatusBadGateway)
				}
			},
			ModifyResponse: removeBackendRequestID,
			FlushInterval:  -1,
		}
		result.routes = append(result.routes, compiledRoute{config: configuration, template: template, proxy: proxy})
	}
	sort.SliceStable(result.routes, func(i, j int) bool {
		left, right := result.routes[i], result.routes[j]
		if left.template.original == right.template.original {
			return left.config.Method != "ANY" && right.config.Method == "ANY"
		}
		return moreSpecific(left.template, right.template)
	})
	frontend, err := result.newFrontend()
	if err != nil {
		_ = result.Close()
		return nil, err
	}
	result.frontend = frontend
	return result, nil
}

func (gateway *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if gateway.closed.Load() {
		writeGatewayError(w, http.StatusServiceUnavailable)
		return
	}
	started := time.Now()
	requestID := uuid.NewString()
	r = r.Clone(r.Context())
	for _, name := range gateway.config.RemoveHeaders {
		r.Header.Del(name)
	}
	r.Header.Set("X-Request-Id", requestID)
	correlation := r.Header.Get("X-Correlation-Id")
	if _, err := uuid.Parse(correlation); err != nil {
		correlation = requestID
	}
	r.Header.Set("X-Correlation-Id", correlation)
	w.Header().Set("X-Request-Id", requestID)
	routeLabel := "{unmatched}"
	defer func() {
		gateway.options.Logger.Info().Str("request_id", requestID).Str("method", r.Method).Str("route", routeLabel).Dur("duration", time.Since(started)).Msg("Gateway request")
	}()
	if r.URL.Path == "/health" && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"healthy","service":"eventbus-gateway"}`)
		return
	}
	if !canonicalRequestPath(r.URL) {
		writeGatewayError(w, http.StatusBadRequest)
		return
	}
	var route *compiledRoute
	var parameters map[string]string
	for i := range gateway.routes {
		candidate := &gateway.routes[i]
		if candidate.config.Method != "ANY" && candidate.config.Method != r.Method {
			continue
		}
		if values, ok := candidate.template.match(r.URL.Path); ok {
			route = candidate
			parameters = values
			break
		}
	}
	if route == nil {
		if gateway.frontend != nil {
			routeLabel = "{frontend}"
			gateway.frontend.ServeHTTP(w, r)
			return
		}
		writeGatewayError(w, http.StatusForbidden)
		return
	}
	routeLabel = route.config.Path
	for _, redaction := range gateway.config.LogRedactions {
		template, _ := parseTemplate(redaction)
		if _, ok := template.match(r.URL.Path); ok {
			routeLabel = redaction
			break
		}
	}
	response := authorizerResponse{Context: make(map[string]string)}
	if route.config.Authorizer != "" && !gateway.options.NoAuth {
		event := newRequestEvent(gateway.config, route.config, r, parameters, requestID)
		var status int
		response, status = gateway.authorizers[route.config.Authorizer].authorize(r.Context(), event)
		if status != 0 {
			writeGatewayError(w, status)
			return
		}
	}
	target, headers, err := mapIntegration(route.config.Integration, mappingInput{request: r, parameters: parameters, authorizer: response, config: gateway.config, requestID: requestID})
	if err != nil {
		writeGatewayError(w, http.StatusInternalServerError)
		return
	}
	route.proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), integrationRequestKey{}, integrationRequest{target: target, headers: headers})))
}

func (gateway *Gateway) Close() error {
	if gateway == nil || !gateway.closed.CompareAndSwap(false, true) {
		return nil
	}
	gateway.transport.CloseIdleConnections()
	return gateway.connections.Close()
}
func removeBackendRequestID(response *http.Response) error {
	response.Header.Del("X-Request-Id")
	return nil
}
func writeGatewayError(w http.ResponseWriter, status int) {
	message := "Internal server error"
	switch status {
	case http.StatusUnauthorized:
		message = "Unauthorized"
	case http.StatusForbidden:
		message = "Forbidden"
	case http.StatusBadRequest:
		message = "Bad request"
	case http.StatusRequestURITooLong:
		message = "Request URI too long"
	case http.StatusBadGateway:
		message = "Bad gateway"
	case http.StatusServiceUnavailable:
		message = "Service unavailable"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
}
func (gateway *Gateway) newFrontend() (http.Handler, error) {
	if gateway.options.FrontendProxy != "" && gateway.options.FrontendDir != "" {
		return nil, errors.New("select either frontend directory or frontend proxy")
	}
	if gateway.options.FrontendProxy != "" {
		target, err := validHTTPURL(gateway.options.FrontendProxy)
		if err != nil {
			return nil, errors.New("invalid frontend proxy URL")
		}
		proxy := &httputil.ReverseProxy{Rewrite: func(request *httputil.ProxyRequest) { request.SetURL(target); request.SetXForwarded() }}
		proxy.Transport = gateway.transport
		proxy.ErrorLog = log.New(io.Discard, "", 0)
		proxy.ModifyResponse = removeBackendRequestID
		proxy.FlushInterval = -1
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, _ error) {
			if !errors.Is(r.Context().Err(), context.Canceled) {
				writeGatewayError(w, http.StatusBadGateway)
			}
		}
		return proxy, nil
	}
	if gateway.options.FrontendDir == "" {
		return nil, nil
	}
	root, err := filepath.Abs(gateway.options.FrontendDir)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(filepath.Join(root, "index.html")); err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("frontend directory must contain index.html")
	}
	files := http.FileServer(http.Dir(root))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		filename := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(r.URL.Path, "/")))
		if info, err := os.Stat(filename); err == nil && info.Mode().IsRegular() {
			files.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(root, "index.html"))
	}), nil
}

type connectionTracker struct {
	mu          sync.Mutex
	connections map[*trackedConnection]struct{}
	closed      bool
	dialer      net.Dialer
}
type trackedConnection struct {
	net.Conn
	owner *connectionTracker
	once  sync.Once
}

func newConnectionTracker() *connectionTracker {
	return &connectionTracker{connections: make(map[*trackedConnection]struct{}), dialer: net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}}
}
func (tracker *connectionTracker) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	connection, err := tracker.dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	wrapped := &trackedConnection{Conn: connection, owner: tracker}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.closed {
		_ = connection.Close()
		return nil, net.ErrClosed
	}
	tracker.connections[wrapped] = struct{}{}
	return wrapped, nil
}
func (connection *trackedConnection) Close() error {
	err := connection.Conn.Close()
	connection.once.Do(func() {
		connection.owner.mu.Lock()
		delete(connection.owner.connections, connection)
		connection.owner.mu.Unlock()
	})
	return err
}
func (tracker *connectionTracker) Close() error {
	tracker.mu.Lock()
	tracker.closed = true
	connections := make([]*trackedConnection, 0, len(tracker.connections))
	for connection := range tracker.connections {
		connections = append(connections, connection)
	}
	tracker.mu.Unlock()
	var failures []error
	for _, connection := range connections {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
