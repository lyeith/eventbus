package lambda

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

// GetFunction has no operation suffix. Other Lambda management paths continue
// to produce the existing unsupported-operation response.
func getFunctionPath(request *http.Request) bool {
	return strings.HasPrefix(request.URL.Path, invokePrefix) &&
		!strings.Contains(strings.TrimPrefix(request.URL.Path, invokePrefix), "/")
}

func (service *Service) serveGetFunction(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		invokeError(writer, http.StatusMethodNotAllowed, "InvalidRequestContentException", "Lambda GetFunction requires GET")
		return
	}
	query := request.URL.Query()
	if qualifiers, supplied := query["Qualifier"]; supplied && (len(qualifiers) != 1 || qualifiers[0] == "") {
		invokeError(writer, http.StatusBadRequest, "InvalidParameterValueException", "Qualifier must be a single nonempty version or alias")
		return
	}
	metadata, err := service.GetFunction(strings.TrimPrefix(request.URL.Path, invokePrefix), query.Get("Qualifier"))
	if err != nil {
		var native *InvokeError
		if errors.As(err, &native) {
			invokeError(writer, native.Status, native.Code, native.Message)
		} else {
			invokeError(writer, http.StatusInternalServerError, "ServiceException", "Cannot describe function")
		}
		return
	}
	_ = json.NewEncoder(writer).Encode(struct {
		Configuration FunctionConfiguration `json:"Configuration"`
	}{metadata})
}
