// Explicit loopback fixture transport. SNS owns delivery policy and projections.
package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"
)

type snsFixtureHTTP struct {
	endpoints map[string]struct{}
	client    *http.Client
}

func fixtureHTTPURL(text string) (*url.URL, error) {
	target, err := url.Parse(text)
	if err != nil || target.Host == "" || target.User != nil || target.Fragment != "" || target.Opaque != "" ||
		target.Scheme != "http" && target.Scheme != "https" {
		return nil, errors.New("fixture endpoint must be an absolute loopback HTTP/S URL")
	}
	if !loopbackHost(target.Hostname()) {
		return nil, errors.New("fixture endpoint must use localhost or a loopback IP")
	}
	return target, nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address, err := netip.ParseAddr(host)
	return err == nil && address.Zone() == "" && address.Unmap().IsLoopback()
}

func newFixtureHTTPClient(caFile string) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.New("load system certificate roots")
		}
		data, err := os.ReadFile(caFile)
		if err != nil || !roots.AppendCertsFromPEM(data) {
			return nil, errors.New("read fixture TLS CA PEM")
		}
		tlsConfig.RootCAs = roots
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		ForceAttemptHTTP2: true, TLSClientConfig: tlsConfig, TLSHandshakeTimeout: 3 * time.Second,
		MaxIdleConns: 32, IdleConnTimeout: 30 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil || !loopbackHost(host) {
				return nil, errors.New("fixture destination is outside loopback")
			}
			if strings.EqualFold(host, "localhost") {
				host = "127.0.0.1"
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
		},
	}
	return &http.Client{
		Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

func newSNSFixtureHTTP(raw string, client *http.Client) (*snsFixtureHTTP, error) {
	transport := &snsFixtureHTTP{client: client, endpoints: make(map[string]struct{})}
	for _, value := range strings.Split(raw, ",") {
		endpoint := strings.TrimSpace(value)
		if endpoint == "" {
			return nil, errors.New("SNS fixture endpoint cannot be empty")
		}
		if _, err := fixtureHTTPURL(endpoint); err != nil {
			return nil, err
		}
		transport.endpoints[endpoint] = struct{}{}
	}
	return transport, nil
}

func (transport *snsFixtureHTTP) Allows(endpoint string) bool {
	_, ok := transport.endpoints[endpoint]
	return ok
}

func (transport *snsFixtureHTTP) Post(ctx context.Context, endpoint string, headers http.Header, body string) (int, error) {
	if !transport.Allows(endpoint) {
		return 0, errors.New("SNS fixture endpoint is not selected")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return 0, errors.New("invalid SNS fixture request")
	}
	request.Header = headers.Clone()
	response, err := transport.client.Do(request)
	if err != nil {
		return 0, errors.New("SNS fixture transport failed")
	}
	// Status is the protocol result. A fixture that closes its optional body
	// after accepting a message does not turn acceptance into a retry.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32<<10))
	_ = response.Body.Close()
	return response.StatusCode, nil
}
