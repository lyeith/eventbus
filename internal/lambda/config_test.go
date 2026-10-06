package lambda

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigDefaultsStrictnessAndResolution(t *testing.T) {
	directory := t.TempDir()
	writeFixture(t, directory, "handler.mjs", `export const handler = async event => event;`)
	path := filepath.Join(directory, "functions.yaml")
	if err := os.WriteFile(path, []byte("functions:\n  authorizer:\n    runtime: node\n    handler: handler.handler\n    environment:\n      FIXTURE_VALUE: declared\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.Functions["authorizer"].Timeout != 10*time.Second {
		t.Fatal("missing default timeout")
	}
	service, err := New("functions.yaml", directory)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(service.functions["authorizer"].module) || !filepath.IsAbs(service.functions["authorizer"].command[0]) {
		t.Fatal("unresolved startup paths")
	}
	for _, source := range []string{
		"functions: {}\n",
		"functions:\n  a:\n    runtime: perl\n    command: [perl]\n",
		"functions:\n  a:\n    runtime: provided\n",
		"functions:\n  a:\n    runtime: node\n    handler: missing\n",
		"functions:\n  a:\n    runtime: node\n    handler: handler.mjs.handler\n    timeotu: 5s\n",
		"functions:\n  a:\n    runtime: node\n    handler: handler.mjs.handler\n    timeout: 901s\n",
		"functions:\n  a:\n    runtime: node\n    handler: handler.mjs.handler\n    environment:\n      invalid-name: x\n",
		"functions:\n  a:\n    runtime: node\n    handler: handler.mjs.handler\n---\nfunctions: {}\n",
	} {
		if err := os.WriteFile(path, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); err == nil {
			t.Fatalf("accepted invalid config: %s", source)
		}
	}
}

func TestResolveFunctionIdentifiers(t *testing.T) {
	for _, test := range []struct{ name, qualifier, want string }{
		{"local", "", "local"},
		{"local:live", "", "local:live"},
		{"123456789012:function:local:live", "", "local:live"},
		{"arn:aws:lambda:us-east-1:123456789012:function:local", "2", "local:2"},
		{"arn:aws-us-gov:lambda:us-gov-west-1:123456789012:function:local:live", "", "local:live"},
	} {
		got, err := resolveName(test.name, test.qualifier)
		if err != nil || got != test.want {
			t.Fatalf("%s: %q %v", test.name, got, err)
		}
	}
	for _, test := range []struct{ name, qualifier string }{{"local:2", "3"}, {"arn:aws:s3:us-east-1:123456789012:function:local", ""}, {"bad/name", ""}, {"", ""}} {
		if _, err := resolveName(test.name, test.qualifier); err == nil {
			t.Fatalf("accepted %q", test.name)
		}
	}
}
