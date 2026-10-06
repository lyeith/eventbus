package messaging

import (
	"fmt"
	"net/http"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// Handler adapts the shared messaging broker to the SNS and SQS AWS protocols.
// The router selects a protocol and operation before invoking this adapter.
type Handler struct {
	broker *Broker
}

func NewHandler(broker *Broker) *Handler {
	return &Handler{broker: broker}
}

// ServeQuery serves SNS and legacy SQS operations from parsed Query parameters.
func (s *Handler) ServeQuery(w http.ResponseWriter, r *http.Request, action string) {
	switch action {
	case "CreateTopic":
		s.handleCreateTopic(w, r)
	case "Subscribe":
		s.handleSubscribe(w, r)
	case "Publish":
		s.handlePublish(w, r)
	case "ListTopics":
		s.handleListTopics(w, r)
	case "ListSubscriptionsByTopic":
		s.handleListSubscriptionsByTopic(w, r)
	case "DeleteTopic":
		s.handleDeleteTopic(w, r)
	case "CreateQueue":
		s.handleCreateQueue(w, r)
	case "GetQueueAttributes":
		s.handleGetQueueAttributes(w, r)
	case "GetQueueUrl":
		s.handleGetQueueUrl(w, r)
	case "ReceiveMessage":
		s.handleReceiveMessage(w, r)
	case "DeleteMessage":
		s.handleDeleteMessage(w, r)
	case "PurgeQueue":
		s.handlePurgeQueue(w, r)
	case "DeleteQueue":
		s.handleDeleteQueue(w, r)
	case "ListQueues":
		s.handleListQueues(w, r)
	default:
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown action: %s", action))
	}
}
