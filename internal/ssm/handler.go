package ssm

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/rs/zerolog/log"
)

// The transport budget is separate from Standard parameters' 4 KiB values.
// Escaped JSON can be larger than the decoded native value.
const maxJSONRequestBytes int64 = 1 << 20

type Handler struct{ ssm *SSMStore }

func NewHandler(store *SSMStore) *Handler { return &Handler{ssm: store} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	prefix, action, found := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	if !found || prefix != "AmazonSSM" || action == "" {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "UnknownOperationException", "Unknown Systems Manager target")
		return
	}
	h.ServeAction(w, r, action)
}

func (h *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	log.Debug().Str("action", action).Msg("SSM JSON API request")
	switch action {
	case "PutParameter":
		h.putParameter(w, r)
	case "GetParameter":
		h.getParameter(w, r)
	case "GetParametersByPath":
		h.getParametersByPath(w, r)
	case "DeleteParameter":
		h.deleteParameter(w, r)
	default:
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "UnknownOperationException", fmt.Sprintf("Unknown SSM action: %s", action))
	}
}

// ReadJSONBody owns the bounded read and object requirement. Typed request
// decoding rejects wrong option types rather than silently treating them as zero.
func readRequest(w http.ResponseWriter, r *http.Request, request interface{}) bool {
	data, err := awsprotocol.ReadJSONBody(r, maxJSONRequestBytes)
	if err == nil {
		var encoded []byte
		encoded, err = json.Marshal(data)
		if err == nil {
			err = json.Unmarshal(encoded, request)
		}
	}
	if err != nil {
		awsprotocol.JSON11.Error(w, http.StatusBadRequest, "SerializationException", "Invalid JSON request body")
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, err error) {
	code, message := parameterErrorCode(err)
	status := http.StatusBadRequest
	if code == "InternalServerError" {
		status = http.StatusInternalServerError
	}
	awsprotocol.JSON11.Error(w, status, code, message)
}

type putParameterRequest struct {
	Name           string
	Value          string
	Type           string
	Overwrite      bool
	Description    *string
	AllowedPattern string
	KeyID          string `json:"KeyId"`
	DataType       string
	Tier           string
	Policies       string
	Tags           []json.RawMessage
}

func (h *Handler) putParameter(w http.ResponseWriter, r *http.Request) {
	var request putParameterRequest
	if !readRequest(w, r, &request) {
		return
	}
	if request.AllowedPattern != "" || request.KeyID != "" && request.KeyID != "alias/aws/ssm" ||
		request.DataType != "" && request.DataType != "text" || request.Tier != "" && request.Tier != "Standard" ||
		request.Policies != "" && strings.TrimSpace(request.Policies) != "[]" || len(request.Tags) > 0 {
		writeError(w, newParameterError("ValidationException", "AllowedPattern, custom KMS keys, non-text DataType, advanced tiers, parameter policies and tags are not supported"))
		return
	}
	parameter := SSMParameter{Name: request.Name, Value: request.Value, Type: request.Type, DataType: request.DataType}
	result, err := h.ssm.putParameterValue(parameter, request.Overwrite, request.Description)
	if err != nil {
		writeError(w, err)
		return
	}
	log.Debug().Str("name", result.Name).Msg("SSM parameter stored")
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]interface{}{"Version": result.Version, "Tier": "Standard"})
}

type getParameterRequest struct {
	Name           string
	WithDecryption bool
}

func (h *Handler) getParameter(w http.ResponseWriter, r *http.Request) {
	var request getParameterRequest
	if !readRequest(w, r, &request) {
		return
	}
	name, version, selector, err := parameterSelector(request.Name)
	if err != nil {
		writeError(w, err)
		return
	}
	parameter, err := h.ssm.GetParameterValue(name, version, request.WithDecryption)
	if err != nil {
		writeError(w, err)
		return
	}
	out := parameterResponse(parameter)
	if selector != "" {
		out["Selector"] = selector
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]interface{}{"Parameter": out})
}

type getParametersByPathRequest struct {
	Path             string
	Recursive        bool
	WithDecryption   bool
	MaxResults       *int
	NextToken        string
	ParameterFilters []json.RawMessage
}

func (h *Handler) getParametersByPath(w http.ResponseWriter, r *http.Request) {
	var request getParametersByPathRequest
	if !readRequest(w, r, &request) {
		return
	}
	path := strings.TrimSpace(request.Path)
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "//") {
		writeError(w, newParameterError("ValidationException", "Path must begin with /"))
		return
	}
	path = canonicalPath(path)
	if path != "/" {
		if _, err := validateParameterName(path); err != nil {
			writeError(w, err)
			return
		}
	}
	if len(request.ParameterFilters) > 0 {
		writeError(w, newParameterError("ValidationException", "ParameterFilters are not supported"))
		return
	}
	maxResults := 10
	if request.MaxResults != nil {
		maxResults = *request.MaxResults
	}
	parameters, token, err := h.ssm.ListParametersByPath(path, request.Recursive, request.WithDecryption, maxResults, request.NextToken)
	if err != nil {
		writeError(w, err)
		return
	}
	list := make([]map[string]interface{}, 0, len(parameters))
	for _, parameter := range parameters {
		list = append(list, parameterResponse(parameter))
	}
	out := map[string]interface{}{"Parameters": list}
	if token != "" {
		out["NextToken"] = token
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, out)
}

func parameterResponse(parameter *SSMParameter) map[string]interface{} {
	return map[string]interface{}{
		"Name": parameter.Name, "Value": parameter.Value, "Type": parameter.Type,
		"Version": parameter.Version, "DataType": parameter.DataType,
		"LastModifiedDate": float64(parameter.LastModifiedDate.UnixMicro()) / 1e6,
	}
}

type deleteParameterRequest struct{ Name string }

func (h *Handler) deleteParameter(w http.ResponseWriter, r *http.Request) {
	var request deleteParameterRequest
	if !readRequest(w, r, &request) {
		return
	}
	name, err := validateParameterName(request.Name)
	if err != nil {
		writeError(w, err)
		return
	}
	if !h.ssm.DeleteParameterIfExists(name) {
		writeError(w, errParameterNotFound)
		return
	}
	awsprotocol.JSON11.Response(w, http.StatusOK, map[string]interface{}{})
}
