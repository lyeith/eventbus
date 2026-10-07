package gateway

import (
	"net/url"
	"reflect"
	"testing"
)

func TestTrailingSlashTemplatesMatchLiterally(t *testing.T) {
	for _, tc := range []struct {
		template, request string
		parameters        map[string]string
		match             bool
	}{
		{"/", "/", map[string]string{}, true},
		{"/docs", "/docs", map[string]string{}, true},
		{"/docs", "/docs/", nil, false},
		{"/docs/", "/docs/", map[string]string{}, true},
		{"/docs/", "/docs", nil, false},
		{"/docs/", "/docs/index.html", nil, false},
		{"/docs/{id}/", "/docs/example/", map[string]string{"id": "example"}, true},
		{"/docs/{id}/", "/docs/example", nil, false},
		{"/docs/{id}/", "/docs/", nil, false},
		{"/docs/{proxy+}", "/docs", nil, false},
		{"/docs/{proxy+}", "/docs/", nil, false},
		{"/docs/{proxy+}", "/docs/assets/", map[string]string{"proxy": "assets/"}, true},
		{"/docs/{proxy+}", "/docs/assets/nested.js", map[string]string{"proxy": "assets/nested.js"}, true},
	} {
		t.Run(tc.template+"->"+tc.request, func(t *testing.T) {
			template, err := parseTemplate(tc.template)
			if err != nil {
				t.Fatal(err)
			}
			if template.original != tc.template {
				t.Fatalf("template canonicalized: %q", template.original)
			}
			parameters, matched := template.match(tc.request)
			if matched != tc.match || matched && !reflect.DeepEqual(parameters, tc.parameters) {
				t.Fatalf("match=%t parameters=%v, want %t %v", matched, parameters, tc.match, tc.parameters)
			}
		})
	}
	for _, raw := range []string{"//", "/docs//", "/docs//assets", "/docs/./", "/docs/../", "/docs/%2f", "/docs/{proxy+}/", "/docs/{proxy+}/index.html"} {
		if _, err := parseTemplate(raw); err == nil {
			t.Errorf("invalid template accepted: %q", raw)
		}
	}
}

func TestCanonicalRequestRetainsSingleTrailingSlash(t *testing.T) {
	for _, raw := range []string{"/", "/docs", "/docs/", "/docs/assets/", "/docs/a%20b/"} {
		requestURL, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		before := *requestURL
		if !canonicalRequestPath(requestURL) || *requestURL != before {
			t.Errorf("valid request rejected or rewritten: %q %+v", raw, requestURL)
		}
	}
	for _, raw := range []string{"//", "/docs//", "/docs/./", "/docs/../", "/docs/%2f", "/docs/%2e/"} {
		requestURL, err := url.ParseRequestURI(raw)
		if err != nil {
			t.Fatal(err)
		}
		if canonicalRequestPath(requestURL) {
			t.Errorf("ambiguous request accepted: %q", raw)
		}
	}
}

func TestRouteSpecificityPrioritizesFullMatchBeforeGreedy(t *testing.T) {
	for _, pair := range [][2]string{
		{"/docs/", "/docs/{proxy+}"},
		{"/docs/index.html/", "/docs/{id}/"},
		{"/{section}/index.html/", "/docs/{proxy+}"},
		{"/{section}/index.html", "/docs/assets/{proxy+}"},
		{"/docs/{proxy+}", "/{proxy+}"},
	} {
		left, err := parseTemplate(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		right, err := parseTemplate(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if !moreSpecific(left, right) || moreSpecific(right, left) {
			t.Errorf("route precedence reversed: %q before %q", pair[0], pair[1])
		}
	}
}
