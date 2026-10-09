package ses

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSESRawLineLengthBoundaries(t *testing.T) {
	for _, api := range []string{"v1", "v2"} {
		for _, test := range []struct {
			name, body string
			valid      bool
		}{
			{"CRLF exact limit", strings.Repeat("a", 998) + "\r\n", true},
			{"CRLF over limit", strings.Repeat("a", 999) + "\r\n", false},
			{"LF exact limit", strings.Repeat("a", 998) + "\n", true},
			{"LF over limit", strings.Repeat("a", 999) + "\n", false},
			{"final line exact limit", strings.Repeat("a", 998), true},
			{"final line over limit", strings.Repeat("a", 999), false},
			{"final CR exact limit", strings.Repeat("a", 998) + "\r", true},
			{"final CR over limit", strings.Repeat("a", 999) + "\r", false},
			{"one CR removed at exact limit", strings.Repeat("a", 997) + "\r\r\n", true},
			{"only one CR removed", strings.Repeat("a", 998) + "\r\r\n", false},
			{"UTF8 bytes exact limit", strings.Repeat("é", 499) + "\r\n", true},
			{"UTF8 bytes over limit", strings.Repeat("é", 500) + "\r\n", false},
			{"empty body", "", true},
			{"empty final lines", "\r\n\n\r\n", true},
		} {
			t.Run(api+"/"+test.name, func(t *testing.T) {
				// The existing native rule charges two CRLF bytes even for LF
				// endings or an unterminated final line, and counts MIME bytes.
				raw := "From: sender@example.test\r\nTo: to@example.test\r\nSubject: Line boundary\r\nContent-Type: text/plain\r\n\r\n" + test.body
				metadata, apiErr := sesValidateRaw(api, base64.StdEncoding.EncodeToString([]byte(raw)), sesV1MaxMessageSize)
				if test.valid {
					require.Nil(t, apiErr)
					require.Equal(t, "Line boundary", metadata["subject"])
					require.Equal(t, "sender@example.test", metadata["from"])
				} else {
					require.NotNil(t, apiErr)
					require.Equal(t, "Raw message lines must not exceed 1000 characters including CRLF", apiErr.Message)
				}
			})
		}
	}
}
