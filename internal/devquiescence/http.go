package devquiescence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

type Lane string

const (
	Source   Lane = "source"
	Callback Lane = "callback"
)

type AdmissionMode string

const (
	Work        AdmissionMode = "work"
	CleanupOnly AdmissionMode = "cleanup_only"
)

type modeKey struct{}

func RequestMode(request *http.Request) AdmissionMode {
	mode, _ := request.Context().Value(modeKey{}).(AdmissionMode)
	return mode
}

func (c *Coordinator) beginEnvelope(lane Lane) (AdmissionMode, func(error), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == Open && !c.closing && !c.evidenceFailure || lane == Callback && c.state == Draining && c.work > 0 {
		return Work, c.beginWorkLocked("http."+string(lane), ""), nil
	}
	// Provisional cleanup envelopes are tracked even before rejecting a source.
	// An app cleanup handler never gains Work mode later through resume.
	c.cleanup++
	c.wakeLocked()
	var released bool
	release := func(evidenceErr error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if released {
			return
		}
		released = true
		c.cleanup--
		if evidenceErr != nil {
			c.evidenceFailure = true
			if c.evidenceCode == "" {
				c.evidenceCode = "incomplete_ownership_evidence"
			}
			if c.state == Open {
				c.state = Draining
			}
		}
		c.wakeLocked()
	}
	if c.closing {
		return CleanupOnly, release, ErrShutdown
	}
	if c.evidenceFailure {
		return CleanupOnly, release, ErrEvidence
	}
	if lane != Callback {
		return CleanupOnly, release, ErrFenced
	}
	if c.state != Held {
		return CleanupOnly, release, ErrNotSafe
	}
	return CleanupOnly, release, nil
}

// Wrap must be the outermost work route, before parsing, reading, capture or
// launch. Handler return releases its envelope, independently of caller cancel.
// Cleanup handlers own the explicit non-producing operation allowlist.
func (c *Coordinator) Wrap(lane Lane, handler, cleanup http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			handler.ServeHTTP(writer, request)
			return
		}
		mode, release, err := c.beginEnvelope(lane)
		defer func() {
			if recovered := recover(); recovered != nil {
				release(ErrEvidence)
				panic(recovered)
			}
			release(nil)
		}()
		if err != nil {
			WriteAdmissionError(writer, request, err)
			return
		}
		request = request.WithContext(context.WithValue(request.Context(), modeKey{}, mode))
		if mode == CleanupOnly {
			if cleanup == nil {
				WriteAdmissionError(writer, request, ErrFenced)
				return
			}
			cleanup.ServeHTTP(writer, request)
			// Keep the cleanup envelope held until any service-side evidence
			// failure is sticky, even when its native response already returned.
			release(c.checkEvidence())
			return
		}
		handler.ServeHTTP(writer, request)
	})
}

// WriteAdmissionError refuses work in the request's native AWS protocol.
// It uses only the route, target and already available form/query metadata;
// fencing never reads or parses a request body to identify its wire format.
func WriteAdmissionError(writer http.ResponseWriter, request *http.Request, admissionErr error) {
	message := ErrFenced.Error()
	if admissionErr != nil {
		message = admissionErr.Error()
	}
	path := request.URL.Path
	restCode := ""
	switch {
	case strings.HasPrefix(path, "/2015-03-31/"):
		restCode = "ServiceException"
	case path == "/schedules" || strings.HasPrefix(path, "/schedules/") || path == "/schedule-groups" || strings.HasPrefix(path, "/schedule-groups/"):
		restCode = "InternalServerException"
	case strings.HasPrefix(path, "/v2/email/"):
		restCode = "ServiceUnavailable"
	}
	if restCode != "" {
		awsprotocol.EnsureRequestID(writer)
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("X-Amzn-ErrorType", restCode)
		writer.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(writer).Encode(map[string]string{"message": message})
		return
	}
	if target := request.Header.Get("X-Amz-Target"); target != "" {
		protocol := awsprotocol.JSONForTarget(target)
		protocol.Error(writer, http.StatusServiceUnavailable, "ServiceUnavailable", message)
		return
	}
	version := request.Form.Get("Version")
	if version == "" {
		version = request.URL.Query().Get("Version")
	}
	namespace, _ := awsprotocol.QueryNamespace(version)
	awsprotocol.QueryError(writer, http.StatusServiceUnavailable, namespace, "ServiceUnavailable", message)
}

const ControlPath = "/__eventbus/dev/retained-owner"

func NewHandler(c *Coordinator) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case ControlPath:
			if request.Method != http.MethodGet {
				methodError(writer, http.MethodGet)
				return
			}
			_ = json.NewEncoder(writer).Encode(c.Snapshot())
		case ControlPath + "/quiesce":
			if request.Method != http.MethodPost {
				methodError(writer, http.MethodPost)
				return
			}
			var input struct {
				TimeoutMS int `json:"timeout_ms"`
			}
			if err := decodeControl(writer, request, &input); err != nil {
				writeFailure(writer, http.StatusBadRequest, errors.New("invalid quiesce request"))
				return
			}
			if input.TimeoutMS < 1 || input.TimeoutMS > 300000 {
				writeFailure(writer, http.StatusBadRequest, errors.New("timeout_ms must be 1..300000"))
				return
			}
			ctx, cancel := context.WithTimeout(request.Context(), time.Duration(input.TimeoutMS)*time.Millisecond)
			defer cancel()
			result, err := c.Quiesce(ctx)
			writeControlResult(writer, result, err)
		case ControlPath + "/resume":
			if request.Method != http.MethodPost {
				methodError(writer, http.MethodPost)
				return
			}
			var input struct {
				Generation uint64 `json:"generation"`
			}
			if err := decodeControl(writer, request, &input); err != nil || input.Generation == 0 {
				writeFailure(writer, http.StatusBadRequest, errors.New("invalid resume request"))
				return
			}
			result, err := c.Resume(input.Generation)
			writeControlResult(writer, result, err)
		default:
			http.NotFound(writer, request)
		}
	})
}

func decodeControl(writer http.ResponseWriter, request *http.Request, output any) error {
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 4096))
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("control requires an object")
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return errors.New("control fields must be unique")
		}
		seen[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("exactly one control object is required")
	}
	decoder = json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	return decoder.Decode(output)
}

func methodError(writer http.ResponseWriter, allowed string) {
	writer.Header().Set("Allow", allowed)
	writeFailure(writer, http.StatusMethodNotAllowed, errors.New("unsupported control method"))
}
func writeFailure(writer http.ResponseWriter, status int, err error) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
}
func writeControlResult(writer http.ResponseWriter, result Snapshot, err error) {
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			status = http.StatusRequestTimeout
		}
		writer.WriteHeader(status)
	}
	_ = json.NewEncoder(writer).Encode(result)
}
