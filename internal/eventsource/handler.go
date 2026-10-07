package eventsource

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

const mappingPath = "/2015-03-31/event-source-mappings"

type Handler struct{ service *Service }

func NewHandler(service *Service) *Handler { return &Handler{service: service} }
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	awsprotocol.EnsureRequestID(w)
	w.Header().Set("Content-Type", "application/json")
	if h.service == nil {
		writeError(w, missing("Event source mappings are not configured"))
		return
	}
	var mapping Mapping
	var err error
	status := http.StatusOK
	switch {
	case r.URL.Path == mappingPath && r.Method == http.MethodPost:
		var input CreateInput
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&input); err != nil {
			writeError(w, invalid("Invalid or unsupported CreateEventSourceMapping request"))
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			writeError(w, invalid("Request must contain one JSON object"))
			return
		}
		mapping, err = h.service.Create(r.Context(), input)
		status = http.StatusAccepted
	case strings.HasPrefix(r.URL.Path, mappingPath+"/") && r.Method == http.MethodGet:
		mapping, err = h.service.Get(strings.TrimPrefix(r.URL.Path, mappingPath+"/"))
	case strings.HasPrefix(r.URL.Path, mappingPath+"/") && r.Method == http.MethodDelete:
		mapping, err = h.service.Delete(r.Context(), strings.TrimPrefix(r.URL.Path, mappingPath+"/"))
		status = http.StatusAccepted
	default:
		err = invalid("Only CreateEventSourceMapping, GetEventSourceMapping and DeleteEventSourceMapping are supported")
	}
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(mapping)
}

func writeError(w http.ResponseWriter, err error) {
	failure := &APIError{Code: "ServiceException", Status: http.StatusInternalServerError, Message: "Event source mapping operation failed"}
	var native *APIError
	if errors.As(err, &native) {
		failure = native
	}
	w.Header().Set("X-Amzn-ErrorType", failure.Code)
	w.WriteHeader(failure.Status)
	_ = json.NewEncoder(w).Encode(map[string]string{"Type": "User", "message": failure.Message})
}
