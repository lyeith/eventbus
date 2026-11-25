package eventsource

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
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
	if awsprotocol.IsLambdaMappingTagPath(r.URL.Path) {
		h.serveMappingTags(w, r)
		return
	}
	var result any
	var err error
	status := http.StatusOK
	switch {
	// Older native SDK models include the collection's final slash. Both
	// exact forms reach the same owner without redirecting or normalizing
	// neighboring routes.
	case (r.URL.Path == mappingPath || r.URL.Path == mappingPath+"/") && r.Method == http.MethodGet:
		query := r.URL.Query()
		input := ListInput{EventSourceARN: query.Get("EventSourceArn"), FunctionName: query.Get("FunctionName"), Marker: query.Get("Marker")}
		if query.Has("MaxItems") {
			limit, parseErr := strconv.Atoi(query.Get("MaxItems"))
			if parseErr != nil {
				writeError(w, invalid("Invalid MaxItems"))
				return
			}
			input.MaxItems = &limit
		}
		result, err = h.service.List(input)
	case (r.URL.Path == mappingPath || r.URL.Path == mappingPath+"/") && r.Method == http.MethodPost:
		var input CreateInput
		if err := decodeMappingRequest(w, r, &input); err != nil {
			writeError(w, err)
			return
		}
		result, err = h.service.Create(r.Context(), input)
		status = http.StatusAccepted
	case strings.HasPrefix(r.URL.Path, mappingPath+"/") && r.Method == http.MethodPut:
		var input UpdateInput
		if err := decodeMappingRequest(w, r, &input); err != nil {
			writeError(w, err)
			return
		}
		result, err = h.service.Update(r.Context(), strings.TrimPrefix(r.URL.Path, mappingPath+"/"), input)
		status = http.StatusAccepted
	case strings.HasPrefix(r.URL.Path, mappingPath+"/") && r.Method == http.MethodGet:
		result, err = h.service.Get(strings.TrimPrefix(r.URL.Path, mappingPath+"/"))
	case strings.HasPrefix(r.URL.Path, mappingPath+"/") && r.Method == http.MethodDelete:
		result, err = h.service.Delete(r.Context(), strings.TrimPrefix(r.URL.Path, mappingPath+"/"))
		status = http.StatusAccepted
	default:
		err = invalid("Unsupported event source mapping operation")
	}
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}

func decodeMappingRequest[T any](w http.ResponseWriter, r *http.Request, input *T) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var parsed *T
	if err := decoder.Decode(&parsed); err != nil || parsed == nil {
		return invalid("Invalid or unsupported event source mapping request")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return invalid("Request must contain one JSON object")
	}
	*input = *parsed
	return nil
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
