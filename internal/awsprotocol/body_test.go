package awsprotocol

import (
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

func TestReadBoundedBodyTreatsLengthAsHint(t *testing.T) {
	for _, declared := range []int64{-1, 0, 1, 8, 9, 100, math.MaxInt64} {
		for _, body := range []string{"", "123", "12345678", "123456789", "123456789overflow"} {
			got, err := ReadBoundedBody(strings.NewReader(body), declared, 8)
			want := body[:min(9, len(body))]
			if err != nil || string(got) != want {
				t.Fatalf("declared=%d body=%q got=%q err=%v", declared, body, got, err)
			}
		}
	}
}

type partialBodyReader struct {
	called  bool
	failure error
}

func (r *partialBodyReader) Read(p []byte) (int, error) {
	if r.called {
		return 0, io.EOF
	}
	r.called = true
	return copy(p, "abc"), r.failure
}
func TestReadBoundedBodyRetainsPartialReadFailure(t *testing.T) {
	failure := errors.New("owned read failed")
	body, err := ReadBoundedBody(&partialBodyReader{failure: failure}, 3, 8)
	if !errors.Is(err, failure) || string(body) != "abc" {
		t.Fatalf("body=%q err=%v", body, err)
	}
	body, err = ReadBoundedBody(&partialBodyReader{failure: io.ErrUnexpectedEOF}, 7, 8)
	if !errors.Is(err, io.ErrUnexpectedEOF) || string(body) != "abc" {
		t.Fatalf("truncated body=%q err=%v", body, err)
	}
	for _, limit := range []int64{-1, 0, math.MaxInt64} {
		if _, err := ReadBoundedBody(strings.NewReader("x"), 1, limit); err == nil {
			t.Fatalf("invalid limit=%d accepted", limit)
		}
	}
}
