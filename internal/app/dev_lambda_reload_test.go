package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
)

type devReloadProbe struct {
	names   []string
	entries []lambdaservice.Function
	err     error
}

func (probe *devReloadProbe) RegisterFunction(_ context.Context, name string, function lambdaservice.Function) error {
	probe.names = append(probe.names, name)
	probe.entries = append(probe.entries, function)
	return probe.err
}

func TestDevLambdaReloadReadsCurrentSelectedRecipe(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "functions.yaml")
	write := func(value string) {
		require.NoError(t, os.WriteFile(path, []byte("functions:\n  processor:live:\n    runtime: python\n    handler: handler.process\n    environment:\n      PRIVATE_SETTING: "+value+"\n"), 0600))
	}
	probe := &devReloadProbe{}
	router := withDevLambdaReload(http.NotFoundHandler(), probe, "functions.yaml", directory)
	for _, value := range []string{"before", "after"} {
		write(value)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, devLambdaReloadPrefix+"processor:live/reload", nil))
		require.Equal(t, 200, response.Code, response.Body.String())
		require.NotContains(t, response.Body.String(), "PRIVATE_SETTING")
		require.NotContains(t, response.Body.String(), value)
		require.NotContains(t, response.Body.String(), directory)
	}
	require.Equal(t, []string{"processor:live", "processor:live"}, probe.names)
	require.Equal(t, "before", probe.entries[0].Environment["PRIVATE_SETTING"])
	require.Equal(t, "after", probe.entries[1].Environment["PRIVATE_SETTING"])
	require.Equal(t, "handler.process", probe.entries[1].Handler)
}

func TestDevLambdaReloadRefusalsDoNotRegister(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "functions.yaml")
	require.NoError(t, os.WriteFile(path, []byte("functions:\n  processor:\n    runtime: command\n    command: [echo]\n"), 0600))
	probe := &devReloadProbe{}
	router := withDevLambdaReload(http.NotFoundHandler(), probe, path, directory)
	for _, test := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, devLambdaReloadPrefix + "processor/reload", 405},
		{http.MethodPost, devLambdaReloadPrefix + "missing/reload", 404},
		{http.MethodPost, devLambdaReloadPrefix + "processor/other/reload", 404},
		{http.MethodPost, devLambdaReloadPrefix + "processor/unknown", 404},
		{http.MethodPost, "/2015-03-31/functions/processor", 404},
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		require.Equal(t, test.status, response.Code, response.Body.String())
	}
	require.Empty(t, probe.names)
	require.NoError(t, os.WriteFile(path, []byte("not valid: [yaml"), 0600))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, devLambdaReloadPrefix+"processor/reload", nil))
	require.Equal(t, 400, response.Code)
	require.Empty(t, probe.names)
	require.NotContains(t, response.Body.String(), directory)
}

func TestDevLambdaReloadReportsInterruptedJoinWithoutSecrets(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "functions.yaml")
	require.NoError(t, os.WriteFile(path, []byte("functions:\n  processor:\n    runtime: command\n    command: [echo]\n"), 0600))
	for _, test := range []struct {
		err    error
		status int
	}{
		{context.DeadlineExceeded, 504},
		{errors.New("private-runtime-path"), 503},
	} {
		probe := &devReloadProbe{err: test.err}
		router := withDevLambdaReload(http.NotFoundHandler(), probe, path, directory)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, devLambdaReloadPrefix+"processor/reload", strings.NewReader("{}")))
		require.Equal(t, test.status, response.Code)
		require.Len(t, probe.names, 1)
		require.NotContains(t, response.Body.String(), "private-runtime-path")
		require.NotContains(t, response.Body.String(), directory)
	}
}
