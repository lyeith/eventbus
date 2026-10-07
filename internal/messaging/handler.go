package messaging

import (
	"fmt"
	"github.com/lyeith/eventbus/internal/awsprotocol"
	"net/http"
	"strings"
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
	version := r.FormValue("Version")
	sqs := version == "2012-11-05" || strings.HasPrefix(r.URL.Path, "/queue/") || r.FormValue("QueueUrl") != ""
	if version == "" && !sqs {
		switch action {
		case "CancelMessageMoveTask", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "CreateQueue", "DeleteMessage", "DeleteMessageBatch", "DeleteQueue", "GetQueueAttributes", "GetQueueUrl", "ListDeadLetterSourceQueues", "ListMessageMoveTasks", "ListQueues", "ListQueueTags", "PurgeQueue", "ReceiveMessage", "SendMessage", "SendMessageBatch", "SetQueueAttributes", "StartMessageMoveTask", "TagQueue", "UntagQueue":
			sqs = true
		}
	}
	if version != "2010-03-31" && sqs {
		s.serveSQSQuery(w, r, action)
		return
	}
	if !s.serveSNSQuery(w, r, action) {
		namespace, _ := awsprotocol.QueryNamespace(version)
		awsprotocol.QueryError(w, http.StatusBadRequest, namespace, "InvalidAction", fmt.Sprintf("Unknown action: %s", action))
	}
}
