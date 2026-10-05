package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
)

func (s *Server) handleSSMJSON(w http.ResponseWriter, r *http.Request, action string) {
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
		jsonError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown SSM action: %s", action))
	}
}

func (s *Server) handleSSMPutParameter(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	value, _ := data["Value"].(string)
	paramType, _ := data["Type"].(string)
	overwrite, _ := data["Overwrite"].(bool)

	if name == "" || value == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "Name and Value are required")
		return
	}
	if paramType == "" {
		paramType = "String"
	}

	err = s.ssm.PutParameter(name, value, paramType, overwrite)
	if err != nil {
		if strings.Contains(err.Error(), "ParameterAlreadyExists") {
			jsonError(w, http.StatusBadRequest, "ParameterAlreadyExists", "Parameter already exists: "+name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	log.Debug().Str("name", name).Msg("SSM parameter stored")

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"Version": 1,
		"Tier":    "Standard",
	})
}

func (s *Server) handleSSMGetParameter(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "Name is required")
		return
	}

	param, err := s.ssm.GetParameter(name)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "ParameterNotFound", "Parameter not found: "+name)
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"Parameter": map[string]interface{}{
			"Name":    param.Name,
			"Type":    param.Type,
			"Value":   param.Value,
			"Version": 1,
		},
	})
}

func (s *Server) handleSSMGetParametersByPath(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	path, _ := data["Path"].(string)
	if path == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "Path is required")
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

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"Parameters": paramList,
	})
}

func (s *Server) handleSSMDeleteParameter(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["Name"].(string)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "Name is required")
		return
	}

	s.ssm.DeleteParameter(name)
	jsonResponse(w, http.StatusOK, map[string]interface{}{})
}

// isSSMTarget checks if the X-Amz-Target is an SSM operation.
func isSSMTarget(target string) bool {
	return strings.HasPrefix(target, "AmazonSSM.")
}

func extractSSMAction(target string) string {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}
