package firehose

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

func validatePrefix(prefix string, errorOutput, dynamic bool) error {
	if len(prefix) > 1024 {
		return fmt.Errorf("S3 prefix must be at most 1024 characters")
	}
	if errorOutput && strings.Contains(prefix, "!{") && !strings.Contains(prefix, "!{firehose:error-output-type}") {
		return fmt.Errorf("ErrorOutputPrefix expressions require firehose:error-output-type")
	}
	_, err := renderPrefix(prefix, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), nil, "processing-failed", errorOutput, dynamic, true)
	return err
}

// timestampFormat is a deliberately bounded Java DateTimeFormatter subset.
// Unsupported format letters are refused at stream creation rather than left
// as misleading literal output. Quoted literals follow the native syntax.
func timestampFormat(format string, instant time.Time) (string, error) {
	tokens := map[string]string{"yyyy": "2006", "MM": "01", "dd": "02", "HH": "15", "mm": "04", "ss": "05", "SSS": "000"}
	var output strings.Builder
	for len(format) > 0 {
		if format[0] == '\'' {
			if strings.HasPrefix(format, "''") {
				output.WriteByte('\'')
				format = format[2:]
				continue
			}
			format = format[1:]
			closed := false
			for len(format) > 0 {
				if strings.HasPrefix(format, "''") {
					output.WriteByte('\'')
					format = format[2:]
					continue
				}
				if format[0] == '\'' {
					format = format[1:]
					closed = true
					break
				}
				output.WriteByte(format[0])
				format = format[1:]
			}
			if !closed {
				return "", fmt.Errorf("unclosed timestamp literal")
			}
			continue
		}
		matched := false
		for _, token := range []string{"yyyy", "SSS", "DDD", "MM", "dd", "HH", "mm", "ss"} {
			if strings.HasPrefix(format, token) {
				if token == "SSS" {
					fmt.Fprintf(&output, "%03d", instant.Nanosecond()/int(time.Millisecond))
				} else if token == "DDD" {
					fmt.Fprintf(&output, "%03d", instant.YearDay())
				} else {
					output.WriteString(instant.Format(tokens[token]))
				}
				format = format[len(token):]
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if unicode.IsLetter(rune(format[0])) {
			return "", fmt.Errorf("unsupported timestamp format token in %q", format)
		}
		output.WriteByte(format[0])
		format = format[1:]
	}
	return output.String(), nil
}

func renderPrefix(prefix string, instant time.Time, keys map[string]string, errorType string, errorOutput, dynamic, validate bool) (string, error) {
	if !strings.Contains(prefix, "!{timestamp:") {
		prefix += "!{timestamp:yyyy/MM/dd/HH/}"
	}
	var output strings.Builder
	for {
		before, rest, found := strings.Cut(prefix, "!{")
		output.WriteString(before)
		if !found {
			break
		}
		expression, suffix, closed := strings.Cut(rest, "}")
		if !closed {
			return "", fmt.Errorf("unclosed S3 prefix expression")
		}
		namespace, value, ok := strings.Cut(expression, ":")
		if !ok || value == "" {
			return "", fmt.Errorf("invalid S3 prefix expression")
		}
		switch namespace {
		case "timestamp":
			formatted, err := timestampFormat(value, instant)
			if err != nil {
				return "", err
			}
			output.WriteString(formatted)
		case "partitionKeyFromQuery":
			if errorOutput || !dynamic {
				return "", fmt.Errorf("partitionKeyFromQuery requires dynamic partitioning and an output Prefix")
			}
			if validate {
				break
			}
			extracted, exists := keys[value]
			if !exists || extracted == "" {
				return "", fmt.Errorf("missing partition key %q", value)
			}
			output.WriteString(extracted)
		case "firehose":
			switch value {
			case "random-string":
				if !validate {
					output.WriteString(uuid.NewString()[:11])
				}
			case "error-output-type":
				if !errorOutput {
					return "", fmt.Errorf("firehose:error-output-type is valid only in ErrorOutputPrefix")
				}
				output.WriteString(errorType)
			default:
				return "", fmt.Errorf("unsupported firehose prefix expression %q", value)
			}
		default:
			return "", fmt.Errorf("unsupported S3 prefix namespace %q", namespace)
		}
		prefix = suffix
	}
	if output.Len() > 512 {
		return "", fmt.Errorf("evaluated S3 prefix exceeds 512 characters")
	}
	return output.String(), nil
}
