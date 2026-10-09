package gateway

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func publicFixture(backend string) Config {
	return Config{Routes: []RouteConfig{{Path: "/{proxy+}", Method: "ANY", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: backend + "/{proxy}"}}}}
}

func TestConnectionHeadersCannotSpoofForwardingOrDeleteMintedHeaders(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Sensitive") != "" || r.Header.Get("Forwarded") != "" || strings.Contains(r.Header.Get("X-Forwarded-For"), "attacker") {
			t.Errorf("hop or spoofed forwarding headers escaped: %v", r.Header)
		}
		if r.Header.Get("X-Request-Id") == "" || r.Header.Get("X-Minted") != "trusted" {
			t.Errorf("caller removed minted header: %v", r.Header)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()
	cfg := publicFixture(backend.URL)
	cfg.Routes[0].Integration.RequestParameters = map[string]string{"integration.request.header.X-Minted": "'trusted'"}
	gateway := newTestGateway(t, cfg, Options{})
	request := httptest.NewRequest("GET", "http://edge/item", nil)
	request.Header.Set("Connection", "keep-alive, X-Sensitive, X-Request-Id, X-Minted")
	request.Header.Set("X-Sensitive", "private")
	request.Header.Set("X-Minted", "attacker")
	request.Header.Set("Forwarded", "for=attacker")
	request.Header.Set("X-Forwarded-For", "attacker")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("status=%d", response.Code)
	}
}

func TestProxyStreamsBeforeBackendFinishes(t *testing.T) {
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "first\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "last\n")
		case <-r.Context().Done():
		}
	}))
	defer backend.Close()
	defer close(release)
	gateway := newTestGateway(t, publicFixture(backend.URL), Options{})
	edge := httptest.NewServer(gateway)
	defer edge.Close()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(edge.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || line != "first\n" {
		t.Fatalf("stream not flushed: %q %v", line, err)
	}
}

func TestCloseTerminatesOwnedWebSocketUpgrade(t *testing.T) {
	backendClosed := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			t.Error("upgrade lost")
			w.WriteHeader(400)
			return
		}
		connection, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		defer close(backendClosed)
		_, _ = buffer.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\nready\n")
		_ = buffer.Flush()
		_, _ = io.Copy(io.Discard, connection)
	}))
	defer backend.Close()
	gateway := newTestGateway(t, publicFixture(backend.URL), Options{})
	edge := httptest.NewServer(gateway)
	defer edge.Close()
	connection, err := net.Dial("tcp", strings.TrimPrefix(edge.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = fmt.Fprint(connection, "GET /socket HTTP/1.1\r\nHost: edge\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("upgrade failed: %q %v", status, err)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	if line, err := reader.ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("upgrade bytes lost: %q %v", line, err)
	}
	if err := gateway.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("upgrade remained open")
	}
	select {
	case <-backendClosed:
	case <-time.After(time.Second):
		t.Fatal("backend upgrade leaked")
	}
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest("GET", "/socket", nil))
	if response.Code != 503 {
		t.Fatalf("closed gateway status=%d", response.Code)
	}
}

func TestFrontendSPAFallbackAndConfigStrictness(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("SPA"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Routes: []RouteConfig{{Path: "/api/{proxy+}", Method: "ANY", Integration: IntegrationConfig{Type: "HTTP_PROXY", URI: "http://127.0.0.1:1/{proxy}"}}}}
	gateway := newTestGateway(t, cfg, Options{FrontendDir: root})
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, httptest.NewRequest("GET", "/page/nested", nil))
	if response.Code != 200 || response.Body.String() != "SPA" {
		t.Fatalf("SPA failed: %d %s", response.Code, response.Body.String())
	}
	for _, yaml := range []string{"unexpected: true\n", "routes: []\n---\nroutes: []\n"} {
		filename := filepath.Join(t.TempDir(), "gateway.yaml")
		_ = os.WriteFile(filename, []byte(yaml), 0600)
		if _, err := LoadConfig(filename); err == nil {
			t.Fatal("invalid YAML accepted")
		}
	}
}

func TestIntegrationURIWithoutPathTargetsRoot(t *testing.T) {
	target, _, err := compileTestIntegration(t, IntegrationConfig{Type: "HTTP_PROXY", URI: "http://backend"}).mapRequest(mappingInput{request: httptest.NewRequest("GET", "/source", nil)})
	if err != nil || target.Path != "/" {
		t.Fatalf("root integration: target=%v err=%v", target, err)
	}
}
