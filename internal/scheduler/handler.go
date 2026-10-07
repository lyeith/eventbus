package scheduler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

type Handler struct{ service *Service }

func NewHandler(service *Service) *Handler { return &Handler{service} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	awsprotocol.EnsureRequestID(w)
	w.Header().Set("Content-Type", "application/json")
	name := strings.TrimPrefix(r.URL.Path, "/schedules/")
	if name == r.URL.Path || !namePattern.MatchString(name) {
		writeError(w, validation("Unsupported Scheduler path or invalid schedule name"))
		return
	}
	if h.service == nil {
		writeError(w, &APIError{"InternalServerException", 503, "Scheduler is not configured"})
		return
	}
	var err error
	switch r.Method {
	case http.MethodPost:
		var input CreateInput
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&input); err != nil {
			writeError(w, validation("Invalid or unsupported CreateSchedule request"))
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			writeError(w, validation("Request must contain one JSON object"))
			return
		}
		input.Name = name
		var arn string
		arn, err = h.service.Create(r.Context(), input)
		if err == nil {
			_ = json.NewEncoder(w).Encode(map[string]string{"ScheduleArn": arn})
			return
		}
	case http.MethodGet:
		var schedule Schedule
		schedule, err = h.service.Get(r.URL.Query().Get("groupName"), name)
		if err == nil {
			_ = json.NewEncoder(w).Encode(schedule)
			return
		}
	case http.MethodDelete:
		err = h.service.Delete(r.Context(), r.URL.Query().Get("groupName"), name, r.URL.Query().Get("clientToken"))
		if err == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
	default:
		err = validation("Only CreateSchedule, GetSchedule and DeleteSchedule are supported")
	}
	writeError(w, err)
}

func writeError(w http.ResponseWriter, err error) {
	failure := &APIError{Code: "InternalServerException", Status: 500, Message: "Scheduler operation failed"}
	var native *APIError
	if errors.As(err, &native) {
		failure = native
	}
	w.Header().Set("X-Amzn-ErrorType", failure.Code)
	w.WriteHeader(failure.Status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": failure.Message})
}
