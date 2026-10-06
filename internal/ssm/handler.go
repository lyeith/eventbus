package ssm

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// Handler owns the Parameter Store AWS JSON adapter.
type Handler struct {
	ssm *SSMStore
}

func NewHandler(store *SSMStore) *Handler {
	return &Handler{ssm: store}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.ServeAction(w, r, extractSSMAction(r.Header.Get("X-Amz-Target")))
}

func (s *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	log.Debug().Str("action", action).Msg("SSM JSON API request")

	switch action {
	case "PutParameter":
		s.handleSSMPutParameter(w, r)
	case "GetParameter":
		s.handleSSMGetParameter(w, r)
	case "GetParametersByPath":
		s.handleSSMGetParametersByPath(w, r)
	case "DeleteParameter":
		s.handleSSMDeleteParameter(w, r)
	default:
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown SSM action: %s", action))
	}
}

func (s *Handler) handleSSMPutParameter(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	value, _ := data["Value"].(string)
	paramType, _ := data["Type"].(string)
	overwrite, _ := data["Overwrite"].(bool)

	if name == "" || value == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "Name and Value are required")
		return
	}
	if paramType == "" {
		paramType = "String"
	}

	err = s.ssm.PutParameter(name, value, paramType, overwrite)
	if err != nil {
		if strings.Contains(err.Error(), "ParameterAlreadyExists") {
			awsprotocol.JSONError(w, http.StatusBadRequest, "ParameterAlreadyExists", "Parameter already exists: "+name)
			return
		}
		awsprotocol.JSONError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	log.Debug().Str("name", name).Msg("SSM parameter stored")

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"Version": 1,
		"Tier":    "Standard",
	})
}

func (s *Handler) handleSSMGetParameter(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "Name is required")
		return
	}

	param, err := s.ssm.GetParameter(name)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "ParameterNotFound", "Parameter not found: "+name)
		return
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"Parameter": map[string]interface{}{
			"Name":    param.Name,
			"Type":    param.Type,
			"Value":   param.Value,
			"Version": 1,
		},
	})
}

func (s *Handler) handleSSMGetParametersByPath(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	path, _ := data["Path"].(string)
	if path == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "Path is required")
		return
	}

	params := s.ssm.GetParametersByPath(path)

	paramList := make([]map[string]interface{}, 0, len(params))
	for _, p := range params {
		paramList = append(paramList, map[string]interface{}{
			"Name":    p.Name,
			"Type":    p.Type,
			"Value":   p.Value,
			"Version": 1,
		})
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"Parameters": paramList,
	})
}

func (s *Handler) handleSSMDeleteParameter(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "Name is required")
		return
	}

	s.ssm.DeleteParameter(name)
	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func extractSSMAction(target string) string {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}
