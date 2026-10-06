package messaging

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/rs/zerolog/log"
)

func (s *Handler) handleCreateQueue(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("QueueName")
	if name == "" {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "QueueName is required")
		return
	}

	var visTimeout time.Duration
	var retention time.Duration

	// Parse queue attributes
	for i := 1; i <= 20; i++ {
		attrName := r.FormValue(fmt.Sprintf("Attribute.%d.Name", i))
		attrValue := r.FormValue(fmt.Sprintf("Attribute.%d.Value", i))
		if attrName == "" {
			break
		}
		switch attrName {
		case "VisibilityTimeout":
			if secs, err := strconv.Atoi(attrValue); err == nil {
				visTimeout = time.Duration(secs) * time.Second
			}
		case "MessageRetentionPeriod":
			if secs, err := strconv.Atoi(attrValue); err == nil {
				retention = time.Duration(secs) * time.Second
			}
		}
	}

	queue := s.broker.CreateQueue(name, visTimeout, retention)
	log.Info().Str("queue", name).Str("url", queue.URL).Msg("Created queue")

	body := fmt.Sprintf(`
<CreateQueueResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <CreateQueueResult>
    <QueueUrl>%s</QueueUrl>
  </CreateQueueResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</CreateQueueResponse>`, awsprotocol.XMLEscape(queue.URL), awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body)
}

func (s *Handler) handleGetQueueAttributes(w http.ResponseWriter, r *http.Request) {
	queueURL := r.FormValue("QueueUrl")
	if queueURL == "" {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.XMLError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+queueName)
		return
	}

	attributes := s.queueAttributes(queue)
	names := make([]string, 0, len(attributes))
	for name := range attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	var body strings.Builder
	body.WriteString(`
<GetQueueAttributesResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <GetQueueAttributesResult>`)
	for _, name := range names {
		fmt.Fprintf(&body, `
    <Attribute>
      <Name>%s</Name>
      <Value>%s</Value>
    </Attribute>`, awsprotocol.XMLEscape(name), awsprotocol.XMLEscape(attributes[name]))
	}
	fmt.Fprintf(&body, `
  </GetQueueAttributesResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</GetQueueAttributesResponse>`, awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body.String())
}

// queueAttributes answers GetQueueAttributes in both SQS protocols. The two
// message counts are SQS's own: waiting to be received, and received but not
// yet deleted. Together they tell a local caller when a queue's consumer has
// finished every message sent to it.
func (s *Handler) queueAttributes(queue *Queue) map[string]string {
	waiting, inFlight := s.broker.QueueDepth(queue)
	return map[string]string{
		"QueueArn":                              queue.ARN,
		"VisibilityTimeout":                     strconv.Itoa(int(queue.VisibilityTimeout.Seconds())),
		"MessageRetentionPeriod":                strconv.Itoa(int(queue.RetentionPeriod.Seconds())),
		"ApproximateNumberOfMessages":           strconv.Itoa(waiting),
		"ApproximateNumberOfMessagesNotVisible": strconv.Itoa(inFlight),
	}
}

func (s *Handler) handleGetQueueUrl(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("QueueName")
	if name == "" {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "QueueName is required")
		return
	}

	queue := s.broker.GetQueue(name)
	if queue == nil {
		awsprotocol.XMLError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+name)
		return
	}

	body := fmt.Sprintf(`
<GetQueueUrlResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <GetQueueUrlResult>
    <QueueUrl>%s</QueueUrl>
  </GetQueueUrlResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</GetQueueUrlResponse>`, awsprotocol.XMLEscape(queue.URL), awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body)
}

func (s *Handler) handleReceiveMessage(w http.ResponseWriter, r *http.Request) {
	queueURL := r.FormValue("QueueUrl")
	if queueURL == "" {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.XMLError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+queueName)
		return
	}

	maxMessages := 1
	if v := r.FormValue("MaxNumberOfMessages"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			maxMessages = n
		}
	}

	waitTime := time.Duration(0)
	if v := r.FormValue("WaitTimeSeconds"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			waitTime = time.Duration(secs) * time.Second
		}
	}

	messages := s.broker.ReceiveMessages(queue, maxMessages, waitTime)

	msgXML := ""
	for _, msg := range messages {
		msgXML += fmt.Sprintf(`    <Message>
      <MessageId>%s</MessageId>
      <ReceiptHandle>%s</ReceiptHandle>
      <Body>%s</Body>
    </Message>
`, awsprotocol.XMLEscape(msg.ID), awsprotocol.XMLEscape(msg.ReceiptHandle), awsprotocol.XMLEscape(msg.Body))
	}

	body := fmt.Sprintf(`
<ReceiveMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <ReceiveMessageResult>
%s  </ReceiveMessageResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</ReceiveMessageResponse>`, msgXML, awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body)
}

func (s *Handler) handleDeleteMessage(w http.ResponseWriter, r *http.Request) {
	queueURL := r.FormValue("QueueUrl")
	receiptHandle := r.FormValue("ReceiptHandle")

	if queueURL == "" || receiptHandle == "" {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl and ReceiptHandle are required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.XMLError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
		return
	}

	s.broker.DeleteMessage(queue, receiptHandle)

	body := fmt.Sprintf(`
<DeleteMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</DeleteMessageResponse>`, awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body)
}

func (s *Handler) handlePurgeQueue(w http.ResponseWriter, r *http.Request) {
	queueURL := r.FormValue("QueueUrl")
	if queueURL == "" {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.XMLError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
		return
	}

	s.broker.PurgeQueue(queue)

	body := fmt.Sprintf(`
<PurgeQueueResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</PurgeQueueResponse>`, awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body)
}

func (s *Handler) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	queueURL := r.FormValue("QueueUrl")
	if queueURL == "" {
		awsprotocol.XMLError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	s.broker.DeleteQueue(queueName)

	body := fmt.Sprintf(`
<DeleteQueueResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</DeleteQueueResponse>`, awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body)
}

func (s *Handler) handleListQueues(w http.ResponseWriter, r *http.Request) {
	queues := s.broker.ListQueues()

	urlsXML := ""
	for _, q := range queues {
		urlsXML += fmt.Sprintf("      <QueueUrl>%s</QueueUrl>\n", awsprotocol.XMLEscape(q.URL))
	}

	body := fmt.Sprintf(`
<ListQueuesResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">
  <ListQueuesResult>
%s  </ListQueuesResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</ListQueuesResponse>`, urlsXML, awsprotocol.RequestID())

	awsprotocol.XMLResponse(w, http.StatusOK, body)
}

// queueNameFromURL extracts the queue name from a URL like http://localhost:4100/queue/my-queue
func queueNameFromURL(queueURL string) string {
	parts := strings.Split(queueURL, "/queue/")
	if len(parts) == 2 {
		return parts[1]
	}
	// Fallback: last path segment
	parts = strings.Split(strings.TrimRight(queueURL, "/"), "/")
	return parts[len(parts)-1]
}
