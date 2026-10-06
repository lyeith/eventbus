package messaging

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/awsprotocol"
	"github.com/rs/zerolog/log"
)

// ServeAction handles SQS requests using the AWS JSON 1.0 protocol.
// The action is extracted from X-Amz-Target: AmazonSQS.<Action>.
func (s *Handler) ServeAction(w http.ResponseWriter, r *http.Request, action string) {
	log.Debug().Str("action", action).Str("path", r.URL.Path).Msg("SQS JSON API request")

	switch action {
	case "CreateQueue":
		s.handleCreateQueueJSON(w, r)
	case "GetQueueAttributes":
		s.handleGetQueueAttributesJSON(w, r)
	case "GetQueueUrl":
		s.handleGetQueueUrlJSON(w, r)
	case "ReceiveMessage":
		s.handleReceiveMessageJSON(w, r)
	case "DeleteMessage":
		s.handleDeleteMessageJSON(w, r)
	case "PurgeQueue":
		s.handlePurgeQueueJSON(w, r)
	case "DeleteQueue":
		s.handleDeleteQueueJSON(w, r)
	case "ListQueues":
		s.handleListQueuesJSON(w, r)
	default:
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown SQS action: %s", action))
	}
}

func (s *Handler) handleCreateQueueJSON(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["QueueName"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "QueueName is required")
		return
	}

	var visTimeout time.Duration
	var retention time.Duration

	if attrs, ok := data["Attributes"].(map[string]interface{}); ok {
		if v, ok := attrs["VisibilityTimeout"].(string); ok {
			if secs, err := strconv.Atoi(v); err == nil {
				visTimeout = time.Duration(secs) * time.Second
			}
		}
		if v, ok := attrs["MessageRetentionPeriod"].(string); ok {
			if secs, err := strconv.Atoi(v); err == nil {
				retention = time.Duration(secs) * time.Second
			}
		}
	}

	queue := s.broker.CreateQueue(name, visTimeout, retention)
	log.Info().Str("queue", name).Str("url", queue.URL).Msg("Created queue (JSON)")

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]string{
		"QueueUrl": queue.URL,
	})
}

func (s *Handler) handleGetQueueAttributesJSON(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+queueName)
		return
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"Attributes": s.queueAttributes(queue),
	})
}

func (s *Handler) handleGetQueueUrlJSON(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["QueueName"].(string)
	if name == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "QueueName is required")
		return
	}

	queue := s.broker.GetQueue(name)
	if queue == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+name)
		return
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]string{
		"QueueUrl": queue.URL,
	})
}

func (s *Handler) handleReceiveMessageJSON(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+queueName)
		return
	}

	maxMessages := 1
	if v, ok := data["MaxNumberOfMessages"].(float64); ok {
		maxMessages = int(v)
	}

	waitTime := time.Duration(0)
	if v, ok := data["WaitTimeSeconds"].(float64); ok {
		waitTime = time.Duration(int(v)) * time.Second
	}

	messages := s.broker.ReceiveMessages(queue, maxMessages, waitTime)

	type jsonMessage struct {
		MessageId     string `json:"MessageId"`
		ReceiptHandle string `json:"ReceiptHandle"`
		Body          string `json:"Body"`
	}

	result := make([]jsonMessage, 0, len(messages))
	for _, msg := range messages {
		result = append(result, jsonMessage{
			MessageId:     msg.ID,
			ReceiptHandle: msg.ReceiptHandle,
			Body:          msg.Body,
		})
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"Messages": result,
	})
}

func (s *Handler) handleDeleteMessageJSON(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	receiptHandle, _ := data["ReceiptHandle"].(string)

	if queueURL == "" || receiptHandle == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl and ReceiptHandle are required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
		return
	}

	s.broker.DeleteMessage(queue, receiptHandle)
	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handlePurgeQueueJSON(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
		return
	}

	s.broker.PurgeQueue(queue)
	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handleDeleteQueueJSON(w http.ResponseWriter, r *http.Request) {
	data, err := awsprotocol.ReadJSONBody(r)
	if err != nil {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		awsprotocol.JSONError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	s.broker.DeleteQueue(queueName)
	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Handler) handleListQueuesJSON(w http.ResponseWriter, r *http.Request) {
	queues := s.broker.ListQueues()

	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		urls = append(urls, q.URL)
	}

	awsprotocol.JSONResponse(w, http.StatusOK, map[string]interface{}{
		"QueueUrls": urls,
	})
}

// extractSQSJSONAction extracts the SQS action from the X-Amz-Target header.
// Format: "AmazonSQS.CreateQueue" -> "CreateQueue"
func extractSQSJSONAction(target string) string {
	parts := strings.SplitN(target, ".", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return ""
}
