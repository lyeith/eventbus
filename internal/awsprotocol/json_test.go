package awsprotocol

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadJSONBodyRejectsOversizeCompletePrefix(t *testing.T) {
	// The old reader accepted the complete object at the front and silently
	// discarded the rest of an oversized request, including a second document.
	const limit = 64
	prefix := `{"Name":"owned"}`
	boundary := prefix + strings.Repeat(" ", limit-len(prefix))
	for _, suffix := range []string{"", " ", `{"Name":"other"}`} {
		t.Run(suffix, func(t *testing.T) {
			request := httptest.NewRequest("POST", "/", strings.NewReader(boundary+suffix))
			value, err := ReadJSONBody(request, limit)
			if suffix == "" {
				if err != nil || value["Name"] != "owned" {
					t.Fatalf("valid boundary request: value=%v error=%v", value, err)
				}
			} else if err == nil || value != nil {
				t.Fatalf("oversized request accepted: value=%v error=%v", value, err)
			}
		})
	}
}

type failingReader struct{ err error }

func (reader failingReader) Read([]byte) (int, error) { return 0, reader.err }

func TestReadJSONBodyRetainsDecodeAndReadErrors(t *testing.T) {
	for _, body := range []string{`{`, `{} {}`, `[]`} {
		request := httptest.NewRequest("POST", "/", strings.NewReader(body))
		if _, err := ReadJSONBody(request, 64); err == nil {
			t.Fatalf("malformed object accepted: %q", body)
		}
	}
	failure := errors.New("read failed")
	request := httptest.NewRequest("POST", "/", nil)
	request.Body = io.NopCloser(failingReader{err: failure})
	if _, err := ReadJSONBody(request, 64); !errors.Is(err, failure) {
		t.Fatalf("read failure lost: %v", err)
	}
	if _, err := ReadJSONBody(request, 0); err == nil {
		t.Fatal("invalid caller budget accepted")
	}
}
