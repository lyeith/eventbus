package ssm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type parameterError struct{ code, message string }

func (err *parameterError) Error() string { return err.code + ": " + err.message }

func newParameterError(code, message string) error {
	return &parameterError{code: code, message: message}
}

func validateParameterName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(name) > 1011 {
		return "", newParameterError("ValidationException", "Name must contain 1 to 1011 characters")
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("_.-/", character)) {
			return "", newParameterError("ValidationException", "Name contains an invalid character")
		}
	}
	hierarchy := strings.TrimPrefix(name, "/")
	if hierarchy == "" || strings.HasSuffix(hierarchy, "/") || strings.Contains(hierarchy, "//") {
		return "", newParameterError("ValidationException", "Name must identify a parameter, not an empty hierarchy level")
	}
	if len(strings.Split(hierarchy, "/")) > 15 {
		return "", newParameterError("HierarchyLevelLimitExceededException", "A parameter hierarchy can contain at most 15 levels")
	}
	return name, nil
}

func parameterSelector(input string) (name string, version int64, selector string, err error) {
	input = strings.TrimSpace(input)
	if strings.HasPrefix(input, "arn:") {
		return "", 0, "", newParameterError("ValidationException", "Shared and ARN parameter selectors are not supported")
	}
	name = input
	if before, after, found := strings.Cut(input, ":"); found {
		name, selector = before, ":"+after
		for _, digit := range after {
			if digit < '0' || digit > '9' {
				return "", 0, "", newParameterError("ValidationException", "Only numeric version selectors are supported; labels are not supported")
			}
		}
		version, err = strconv.ParseInt(after, 10, 64)
		if err != nil || version <= 0 {
			return "", 0, "", newParameterError("ValidationException", "Only positive numeric version selectors are supported; labels are not supported")
		}
	}
	name, err = validateParameterName(name)
	return name, version, selector, err
}

func parameterErrorCode(err error) (string, string) {
	var validation *parameterError
	if errors.As(err, &validation) {
		return validation.code, validation.message
	}
	for _, native := range []error{errParameterAlreadyExists, errParameterNotFound, errParameterVersionNotFound, errHierarchyTypeMismatch, errInvalidNextToken} {
		if errors.Is(err, native) {
			return native.Error(), native.Error()
		}
	}
	return "InternalServerError", fmt.Sprint(err)
}
