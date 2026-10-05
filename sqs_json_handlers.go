package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// handleSQSJSON handles SQS requests using the AWS JSON 1.0 protocol.
// The action is extracted from X-Amz-Target: AmazonSQS.<Action>.
func (s *Server) handleSQSJSON(w http.ResponseWriter, r *http.Request, action string) {
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
		jsonError(w, http.StatusBadRequest, "InvalidAction", fmt.Sprintf("Unknown SQS action: %s", action))
	}
}

func readJSONBody(r *http.Request) (map[string]interface{}, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func jsonResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, statusCode int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"__type":  code,
		"Message": message,
	})
}

func (s *Server) handleCreateQueueJSON(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["QueueName"].(string)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "QueueName is required")
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

	jsonResponse(w, http.StatusOK, map[string]string{
		"QueueUrl": queue.URL,
	})
}

func (s *Server) handleGetQueueAttributesJSON(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		jsonError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+queueName)
		return
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"Attributes": s.queueAttributes(queue),
	})
}

func (s *Server) handleGetQueueUrlJSON(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	name, _ := data["QueueName"].(string)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "QueueName is required")
		return
	}

	queue := s.broker.GetQueue(name)
	if queue == nil {
		jsonError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+name)
		return
	}

	jsonResponse(w, http.StatusOK, map[string]string{
		"QueueUrl": queue.URL,
	})
}

func (s *Server) handleReceiveMessageJSON(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		jsonError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found: "+queueName)
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

	jsonResponse(w, http.StatusOK, map[string]interface{}{
		"Messages": result,
	})
}

func (s *Server) handleDeleteMessageJSON(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	receiptHandle, _ := data["ReceiptHandle"].(string)

	if queueURL == "" || receiptHandle == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl and ReceiptHandle are required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		jsonError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
		return
	}

	s.broker.DeleteMessage(queue, receiptHandle)
	jsonResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Server) handlePurgeQueueJSON(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	queue := s.broker.GetQueue(queueName)
	if queue == nil {
		jsonError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "Queue not found")
		return
	}

	s.broker.PurgeQueue(queue)
	jsonResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Server) handleDeleteQueueJSON(w http.ResponseWriter, r *http.Request) {
	data, err := readJSONBody(r)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "InvalidInput", "Invalid JSON body")
		return
	}

	queueURL, _ := data["QueueUrl"].(string)
	if queueURL == "" {
		jsonError(w, http.StatusBadRequest, "InvalidParameter", "QueueUrl is required")
		return
	}

	queueName := queueNameFromURL(queueURL)
	s.broker.DeleteQueue(queueName)
	jsonResponse(w, http.StatusOK, map[string]interface{}{})
}

func (s *Server) handleListQueuesJSON(w http.ResponseWriter, r *http.Request) {
	queues := s.broker.ListQueues()

	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		urls = append(urls, q.URL)
	}

	jsonResponse(w, http.StatusOK, map[string]interface{}{
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
