//go:build !linux && !darwin

package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/lyeith/eventbus/internal/messaging"
)

func TestConsumerRefusesUnsupportedProcessOwnershipBeforeExecution(t *testing.T) {
	manager := NewConsumerManager(messaging.NewBroker("us-east-1", "000000000000", 0), t.TempDir())
	entry := ConsumerEntry{Name: "unsupported", Type: "go", Handler: "must-not-start", TimeoutSeconds: 1}
	_, err := manager.invokeHandlerResult(context.Background(), entry, buildLambdaEvent(nil))
	if !errors.Is(err, localexec.ErrUnsupported) {
		t.Fatalf("got %v, want unsupported process ownership", err)
	}
}
