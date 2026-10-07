package lambda

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRegisteredTargetMetadata(t *testing.T) {
	f := providedFunction(t, "echo")
	f.Timeout = 2 * time.Second
	service := newTestService(t, map[string]Function{"function": f, "function:live": f}, t.TempDir())
	for _, name := range []string{"function", "function:live", "function:$LATEST"} {
		info, err := service.DescribeTarget("arn:aws:lambda:eu-west-1:123456789012:function:"+name, "")
		if err != nil || info.FunctionName != name || info.Timeout != 2*time.Second {
			t.Fatalf("wrong immutable metadata: %+v %v", info, err)
		}
	}
	if _, err := service.DescribeTarget("function:unknown", ""); err == nil {
		t.Fatal("unknown alias accepted")
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := service.DescribeTarget("function", "")
	var native *InvokeError
	if !errors.As(err, &native) || native.Status != 503 {
		t.Fatalf("closed target exposed: %v", err)
	}
}
