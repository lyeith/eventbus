package eventsource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQSFilterPattern(t *testing.T) {
	for _, test := range []struct {
		name, pattern, body string
		match               bool
	}{
		{"nested JSON", `{"body":{"kind":["send"]}}`, `{"kind":"send"}`, true},
		{"nested field differs", `{"body":{"kind":["send"]}}`, `{"kind":"discard"}`, false},
		{"plain text mismatch", `{"body":{"kind":["send"]}}`, "send", false},
		{"plain text prefix", `{"body":[{"prefix":{"equals-ignore-case":"SEND/"}}]}`, "send/now", true},
		{"JSON object doesn't satisfy text filter", `{"body":[{"prefix":"{"}]}`, `{"kind":"send"}`, false},
		{"exact numeric spelling", `{"body":{"n":[300]}}`, `{"n":300.0}`, false},
		{"numeric operator", `{"body":{"n":[{"numeric":[">",10,"<=",300]}]}}`, `{"n":3.0e2}`, true},
		{"exists false", `{"body":{"ignored":[{"exists":false}]}}`, `{"kind":"send"}`, true},
		{"exists true on null", `{"body":{"value":[{"exists":true}]}}`, `{"value":null}`, true},
		{"exists on object", `{"body":{"value":[{"exists":true}]}}`, `{"value":{"nested":true}}`, false},
		{"list values", `{"body":{"kind":["send"]}}`, `{"kind":["drop","send"]}`, true},
		{"anything but", `{"body":{"kind":[{"anything-but":["drop","sleep"]}]}}`, `{"kind":"send"}`, true},
		{"$or combines with sibling", `{"body":{"enabled":[true],"$or":[{"kind":["send"]},{"kind":["notify"]}]}}`, `{"kind":"notify","enabled":false}`, false},
		{"$or second match", `{"$or":[{"body":{"enabled":[true]}},{"attributes":{"MessageGroupId":["native"]}}]}`, "plain", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			filter, err := compileFilterCriteria(&FilterCriteria{Filters: []Filter{{Pattern: test.pattern}}})
			require.NoError(t, err)
			require.Equal(t, test.match, filter(Record{Body: test.body, Attributes: map[string]string{"MessageGroupId": "native"}}))
		})
	}
}

func TestSQSFilterValidationAndOR(t *testing.T) {
	for _, pattern := range []string{"{} {}", "null", "[]", `{"body":[]}`, `{"body":[{"numeric":[">"]}]}`,
		`{"body":[{"exists":"yes"}]}`, `{"body":[{"anything-but":["string",1]}]}`, `{"$or":[{}]}`} {
		_, err := compileFilterCriteria(&FilterCriteria{Filters: []Filter{{Pattern: pattern}}})
		require.Error(t, err, pattern)
	}
	input := &FilterCriteria{Filters: []Filter{{Pattern: `{"body":["one"]}`}, {Pattern: `{"body":["two"]}`}}}
	filter, err := compileFilterCriteria(input)
	require.NoError(t, err)
	settings := cloneFilterCriteria(input)
	input.Filters[1].Pattern = "{}"
	require.Equal(t, `{"body":["two"]}`, settings.Filters[1].Pattern)
	require.True(t, filter(Record{Body: "two"}))
	require.False(t, filter(Record{Body: "three"}))
}
