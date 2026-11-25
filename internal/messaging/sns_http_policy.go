package messaging

import (
	"encoding/json"
	"io"
	"math"
	"mime"
	"strings"
	"time"
)

const snsDefaultContentType = "text/plain; charset=UTF-8"

type snsHTTPRetryOverrides struct {
	MinDelay    *int    `json:"minDelayTarget,omitempty"`
	MaxDelay    *int    `json:"maxDelayTarget,omitempty"`
	Retries     *int    `json:"numRetries,omitempty"`
	Immediate   *int    `json:"numNoDelayRetries,omitempty"`
	PreBackoff  *int    `json:"numMinDelayRetries,omitempty"`
	PostBackoff *int    `json:"numMaxDelayRetries,omitempty"`
	Backoff     *string `json:"backoffFunction,omitempty"`
}

type snsHTTPRetryPolicy struct {
	MinDelay    int    `json:"minDelayTarget"`
	MaxDelay    int    `json:"maxDelayTarget"`
	Retries     int    `json:"numRetries"`
	Immediate   int    `json:"numNoDelayRetries"`
	PreBackoff  int    `json:"numMinDelayRetries"`
	PostBackoff int    `json:"numMaxDelayRetries"`
	Backoff     string `json:"backoffFunction"`
}

type snsHTTPThrottlePolicy struct {
	MaxReceivesPerSecond int `json:"maxReceivesPerSecond"`
}

type snsHTTPRequestPolicy struct {
	HeaderContentType string `json:"headerContentType"`
}

type snsHTTPSubscriptionOverrides struct {
	Retry    snsHTTPRetryOverrides  `json:"healthyRetryPolicy,omitempty"`
	Throttle *snsHTTPThrottlePolicy `json:"throttlePolicy,omitempty"`
	Request  *snsHTTPRequestPolicy  `json:"requestPolicy,omitempty"`
}

type snsHTTPTopicOverrides struct {
	HTTP struct {
		Retry                        snsHTTPRetryOverrides  `json:"defaultHealthyRetryPolicy,omitempty"`
		Throttle                     *snsHTTPThrottlePolicy `json:"defaultThrottlePolicy,omitempty"`
		Request                      *snsHTTPRequestPolicy  `json:"defaultRequestPolicy,omitempty"`
		DisableSubscriptionOverrides bool                   `json:"disableSubscriptionOverrides,omitempty"`
	} `json:"http,omitempty"`
}

type snsHTTPPolicy struct {
	Retry    snsHTTPRetryPolicy     `json:"healthyRetryPolicy"`
	Throttle *snsHTTPThrottlePolicy `json:"throttlePolicy,omitempty"`
	Request  snsHTTPRequestPolicy   `json:"requestPolicy"`
}

func snsDecodeHTTPPolicy[T any](raw string) (T, error) {
	var zero T
	if raw == "" {
		return zero, nil
	}
	if len(raw) > 32768 {
		return zero, snsInvalid("DeliveryPolicy exceeds the supported size")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value *T
	if err := decoder.Decode(&value); err != nil || value == nil || decoder.Decode(new(any)) != io.EOF {
		return zero, snsInvalid("Invalid DeliveryPolicy object")
	}
	return *value, nil
}

func (overrides snsHTTPRetryOverrides) apply(policy *snsHTTPRetryPolicy) error {
	for _, value := range []struct {
		from     *int
		into     *int
		min, max int
	}{
		{overrides.MinDelay, &policy.MinDelay, 1, 3600},
		{overrides.MaxDelay, &policy.MaxDelay, 1, 3600},
		{overrides.Retries, &policy.Retries, 0, 100},
		{overrides.Immediate, &policy.Immediate, 0, 100},
		{overrides.PreBackoff, &policy.PreBackoff, 0, 100},
		{overrides.PostBackoff, &policy.PostBackoff, 0, 100},
	} {
		if value.from == nil {
			continue
		}
		if *value.from < value.min || *value.from > value.max {
			return snsInvalid("DeliveryPolicy retry value is out of range")
		}
		*value.into = *value.from
	}
	if overrides.Backoff != nil {
		switch *overrides.Backoff {
		case "linear", "arithmetic", "geometric", "exponential":
			policy.Backoff = *overrides.Backoff
		default:
			return snsInvalid("Invalid DeliveryPolicy backoffFunction")
		}
	}
	return nil
}

func validateSNSHTTPOverrides(retry snsHTTPRetryOverrides, throttle *snsHTTPThrottlePolicy, request *snsHTTPRequestPolicy, raw bool) error {
	if err := retry.apply(&snsHTTPRetryPolicy{}); err != nil {
		return err
	}
	if throttle != nil && throttle.MaxReceivesPerSecond < 1 {
		return snsInvalid("DeliveryPolicy maxReceivesPerSecond must be positive")
	}
	if request != nil {
		contentType, params, err := mime.ParseMediaType(request.HeaderContentType)
		if err != nil || strings.ContainsAny(request.HeaderContentType, "\r\n") || len(params) > 1 || len(params) == 1 && !strings.EqualFold(params["charset"], "UTF-8") {
			return snsInvalid("Invalid DeliveryPolicy headerContentType")
		}
		switch contentType {
		case "text/plain", "application/json":
			return nil
		case "text/css", "text/csv", "text/html", "text/xml", "application/atom+xml", "application/octet-stream",
			"application/soap+xml", "application/x-www-form-urlencoded", "application/xhtml+xml", "application/xml":
			if raw {
				return nil
			}
		}
		return snsInvalid("DeliveryPolicy headerContentType is unsupported for the selected raw delivery mode")
	}
	return nil
}

func effectiveSNSHTTPPolicy(topicRaw, subscriptionRaw string, raw bool) (snsHTTPPolicy, bool, error) {
	policy := snsHTTPPolicy{
		Retry:   snsHTTPRetryPolicy{MinDelay: 20, MaxDelay: 20, Retries: 3, Backoff: "linear"},
		Request: snsHTTPRequestPolicy{HeaderContentType: snsDefaultContentType},
	}
	topic, err := snsDecodeHTTPPolicy[snsHTTPTopicOverrides](topicRaw)
	if err != nil {
		return policy, false, err
	}
	if err = validateSNSHTTPOverrides(topic.HTTP.Retry, topic.HTTP.Throttle, topic.HTTP.Request, false); err != nil {
		return policy, false, err
	}
	subscription, err := snsDecodeHTTPPolicy[snsHTTPSubscriptionOverrides](subscriptionRaw)
	if err != nil {
		return policy, false, err
	}
	if err = validateSNSHTTPOverrides(subscription.Retry, subscription.Throttle, subscription.Request, raw); err != nil {
		return policy, false, err
	}
	apply := func(retry snsHTTPRetryOverrides, throttle *snsHTTPThrottlePolicy, request *snsHTTPRequestPolicy) {
		// Both layers were validated above.
		_ = retry.apply(&policy.Retry)
		if throttle != nil {
			policy.Throttle = throttle
		}
		if request != nil {
			policy.Request = *request
		}
	}
	apply(topic.HTTP.Retry, topic.HTTP.Throttle, topic.HTTP.Request)
	if !topic.HTTP.DisableSubscriptionOverrides {
		apply(subscription.Retry, subscription.Throttle, subscription.Request)
	}
	retry := policy.Retry
	if retry.MinDelay > retry.MaxDelay || retry.Immediate+retry.PreBackoff+retry.PostBackoff > retry.Retries {
		return policy, false, snsInvalid("DeliveryPolicy has inconsistent retry ranges or phases")
	}
	total := time.Duration(0)
	for attempt := 1; attempt <= retry.Retries; attempt++ {
		delay, _ := retry.retryDelay(attempt)
		total += delay
	}
	if total > time.Hour {
		return policy, false, snsInvalid("DeliveryPolicy retry time exceeds 3600 seconds")
	}
	return policy, topic.HTTP.DisableSubscriptionOverrides, nil
}

func snsHTTPTopicEffectiveJSON(raw string) string {
	policy, disable, err := effectiveSNSHTTPPolicy(raw, "", false)
	if err != nil {
		return ""
	}
	// The topic shape uses default-prefixed field names. Subscriptions use the
	// resolved policy without that wrapper or the topic's override flag.
	value := struct {
		HTTP struct {
			Retry    snsHTTPRetryPolicy     `json:"defaultHealthyRetryPolicy"`
			Throttle *snsHTTPThrottlePolicy `json:"defaultThrottlePolicy,omitempty"`
			Request  snsHTTPRequestPolicy   `json:"defaultRequestPolicy"`
			Disable  bool                   `json:"disableSubscriptionOverrides"`
		} `json:"http"`
	}{}
	value.HTTP.Retry, value.HTTP.Throttle, value.HTTP.Request, value.HTTP.Disable = policy.Retry, policy.Throttle, policy.Request, disable
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func (policy snsHTTPRetryPolicy) retryDelay(retry int) (time.Duration, bool) {
	if retry < 1 || retry > policy.Retries {
		return 0, false
	}
	if retry <= policy.Immediate {
		return 0, true
	}
	minDelay := time.Duration(policy.MinDelay) * time.Second
	maxDelay := time.Duration(policy.MaxDelay) * time.Second
	if retry <= policy.Immediate+policy.PreBackoff {
		return minDelay, true
	}
	if retry > policy.Retries-policy.PostBackoff {
		return maxDelay, true
	}
	positions := policy.Retries - policy.Immediate - policy.PreBackoff - policy.PostBackoff
	if positions <= 1 || minDelay == maxDelay {
		return minDelay, true
	}
	position := float64(retry-policy.Immediate-policy.PreBackoff-1) / float64(positions-1)
	// AWS documents the endpoint delays and curves but does not specify their
	// exact formulas or jitter. Use deterministic interpolations for local
	// timing while preserving the four phases and declared delay bounds.
	weight := position
	switch policy.Backoff {
	case "arithmetic":
		weight = position * position
	case "geometric":
		weight = math.Expm1(position*math.Log(8)) / 7
	case "exponential":
		weight = math.Expm1(position*math.Log(64)) / 63
	}
	return minDelay + time.Duration(float64(maxDelay-minDelay)*weight), true
}
