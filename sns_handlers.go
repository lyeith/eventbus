package main

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/rs/zerolog/log"
)

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("Name")
	if name == "" {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "Name is required")
		return
	}

	topic := s.broker.CreateTopic(name)
	log.Info().Str("topic", topic.ARN).Msg("Created topic")

	body := fmt.Sprintf(`
<CreateTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <CreateTopicResult>
    <TopicArn>%s</TopicArn>
  </CreateTopicResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</CreateTopicResponse>`, xmlEscape(topic.ARN), requestID())

	xmlResponse(w, http.StatusOK, body)
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	topicARN := r.FormValue("TopicArn")
	protocol := r.FormValue("Protocol")
	endpoint := r.FormValue("Endpoint")

	if topicARN == "" || protocol == "" || endpoint == "" {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn, Protocol, and Endpoint are required")
		return
	}

	// Parse optional filter policy from subscription attributes
	var filterPolicy *FilterPolicy
	attrs := formMapValues(r, "Attributes")
	if raw, ok := attrs["FilterPolicy"]; ok {
		var err error
		filterPolicy, err = ParseFilterPolicy(raw)
		if err != nil {
			xmlError(w, http.StatusBadRequest, "InvalidParameter", "Invalid FilterPolicy: "+err.Error())
			return
		}
	}

	sub, err := s.broker.Subscribe(topicARN, protocol, endpoint, filterPolicy)
	if errors.Is(err, errSubscriptionAttributesDiffer) {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "Invalid parameter: Attributes Reason: Subscription already exists with different attributes")
		return
	}
	if err != nil {
		xmlError(w, http.StatusNotFound, "NotFound", err.Error())
		return
	}

	log.Info().Str("topic", topicARN).Str("protocol", protocol).Str("endpoint", endpoint).Msg("Subscribed")

	body := fmt.Sprintf(`
<SubscribeResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <SubscribeResult>
    <SubscriptionArn>%s</SubscriptionArn>
  </SubscribeResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</SubscribeResponse>`, xmlEscape(sub.ARN), requestID())

	xmlResponse(w, http.StatusOK, body)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topicARN := r.FormValue("TopicArn")
	message := r.FormValue("Message")

	if topicARN == "" {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
		return
	}
	if message == "" {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "Message is required")
		return
	}

	attrs := parseMessageAttributes(r)

	messageID, err := s.broker.Publish(topicARN, message, attrs)
	if err != nil {
		xmlError(w, http.StatusNotFound, "NotFound", err.Error())
		return
	}

	log.Debug().Str("topic", topicARN).Str("messageId", messageID).Msg("Published")

	body := fmt.Sprintf(`
<PublishResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <PublishResult>
    <MessageId>%s</MessageId>
  </PublishResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</PublishResponse>`, xmlEscape(messageID), requestID())

	xmlResponse(w, http.StatusOK, body)
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.broker.ListTopics()

	members := ""
	for _, t := range topics {
		members += fmt.Sprintf("      <member><TopicArn>%s</TopicArn></member>\n", xmlEscape(t.ARN))
	}

	body := fmt.Sprintf(`
<ListTopicsResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListTopicsResult>
    <Topics>
%s    </Topics>
  </ListTopicsResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</ListTopicsResponse>`, members, requestID())

	xmlResponse(w, http.StatusOK, body)
}

func (s *Server) handleListSubscriptionsByTopic(w http.ResponseWriter, r *http.Request) {
	topicARN := r.FormValue("TopicArn")
	if topicARN == "" {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
		return
	}

	subs, err := s.broker.ListSubscriptionsByTopic(topicARN)
	if err != nil {
		xmlError(w, http.StatusNotFound, "NotFound", err.Error())
		return
	}

	members := ""
	for _, sub := range subs {
		members += fmt.Sprintf(`      <member>
        <TopicArn>%s</TopicArn>
        <Protocol>%s</Protocol>
        <SubscriptionArn>%s</SubscriptionArn>
        <Endpoint>%s</Endpoint>
      </member>
`, xmlEscape(sub.TopicARN), xmlEscape(sub.Protocol), xmlEscape(sub.ARN), xmlEscape(sub.Endpoint))
	}

	body := fmt.Sprintf(`
<ListSubscriptionsByTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ListSubscriptionsByTopicResult>
    <Subscriptions>
%s    </Subscriptions>
  </ListSubscriptionsByTopicResult>
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</ListSubscriptionsByTopicResponse>`, members, requestID())

	xmlResponse(w, http.StatusOK, body)
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	topicARN := r.FormValue("TopicArn")
	if topicARN == "" {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required")
		return
	}

	s.broker.DeleteTopic(topicARN)

	body := fmt.Sprintf(`
<DeleteTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>%s</RequestId>
  </ResponseMetadata>
</DeleteTopicResponse>`, requestID())

	xmlResponse(w, http.StatusOK, body)
}
