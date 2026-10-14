// Shared bounded JSON decoding for explicitly named Lambda development controls.
package lambda

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

func decodeDevControlBody(request *http.Request, limit int64, target any) error {
	body, err := awsprotocol.ReadBoundedBody(request.Body, request.ContentLength, limit)
	if err != nil {
		return err
	}
	if int64(len(body)) > limit {
		return errors.New("development control body exceeds its limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("development control body must contain one JSON object")
	}
	return nil
}
