// The example is a Go custom runtime using only the standard library. Existing
// binaries using aws-lambda-go/lambda.Start use the same Runtime API unchanged.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	base := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/"
	client := &http.Client{}
	for {
		request, err := client.Get(base + "invocation/next")
		if err != nil {
			fail(err)
		}
		payload, err := io.ReadAll(request.Body)
		_ = request.Body.Close()
		if err != nil {
			fail(err)
		}
		var event struct {
			Headers   map[string]string `json:"headers"`
			MethodARN string            `json:"methodArn"`
		}
		if err := json.Unmarshal(payload, &event); err != nil {
			fail(err)
		}
		token := ""
		for key, value := range event.Headers {
			if strings.EqualFold(key, "authorization") {
				token = value
				break
			}
		}
		var output any
		operation := "response"
		if token == "" {
			operation = "error"
			output = map[string]string{"errorType": "Error", "errorMessage": "Unauthorized"}
		} else {
			effect := "Deny"
			if token == "Bearer local-authorizer-token" {
				effect = "Allow"
			}
			output = map[string]any{
				"principalId":    "local-example-user",
				"policyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{"Action": "execute-api:Invoke", "Effect": effect, "Resource": event.MethodARN}}},
				"context":        map[string]string{"language": "go"},
			}
		}
		response, err := json.Marshal(output)
		if err != nil {
			fail(err)
		}
		id := request.Header.Get("Lambda-Runtime-Aws-Request-Id")
		result, err := client.Post(base+"invocation/"+id+"/"+operation, "application/json", bytes.NewReader(response))
		if err != nil {
			fail(err)
		}
		_ = result.Body.Close()
		if result.StatusCode != http.StatusAccepted {
			fail(fmt.Errorf("Runtime API returned %d", result.StatusCode))
		}
	}
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
