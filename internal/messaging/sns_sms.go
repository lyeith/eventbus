package messaging

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// smsState is account/region state, guarded by snsState.mu. Origination numbers
// and opt-outs are provisioned by the carrier rather than SNS management APIs;
// they start empty. Keep their independent state instead of treating verified
// destination numbers as account-owned origination numbers.
type smsState struct {
	attributes  map[string]string
	sandbox     map[string]*smsSandboxNumber
	optedOut    map[string]bool
	lastOptIn   map[string]time.Time
	origination map[string]smsOriginationNumber
	now         func() time.Time
}

type smsSandboxNumber struct {
	status      string
	otp         string
	expiresAt   time.Time
	lastAttempt time.Time
	sends       []time.Time
}

type smsOriginationNumber struct {
	createdAt          time.Time
	phoneNumber        string
	status             string
	iso2CountryCode    string
	routeType          string
	numberCapabilities []string
}

var (
	smsSandboxPhonePattern  = regexp.MustCompile(`^(\+[0-9]{8,}|[0-9]{1,9})$`)
	smsE164Pattern          = regexp.MustCompile(`^\+[1-9][0-9]{1,14}$`)
	smsOTPPattern           = regexp.MustCompile(`^[0-9]{5,8}$`)
	smsDefaultSenderPattern = regexp.MustCompile(`^[A-Za-z0-9]{1,11}$`)
	smsMessageSenderPattern = regexp.MustCompile(`^[A-Za-z0-9-]{3,11}$`)
	smsOriginationPattern   = regexp.MustCompile(`^\+?[0-9]{5,14}$`)
	smsLetterPattern        = regexp.MustCompile(`[A-Za-z]`)
	smsRolePattern          = regexp.MustCompile(`^arn:aws(?:-[a-z0-9-]+)?:iam::[0-9]{12}:role/.+$`)
	smsBucketPattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
)

func (state *smsState) initialize() {
	if state.attributes == nil {
		state.attributes = map[string]string{
			"DefaultSMSType":                    "Promotional",
			"MonthlySpendLimit":                 "1",
			"DeliveryStatusSuccessSamplingRate": "0",
		}
	}
	if state.sandbox == nil {
		state.sandbox = make(map[string]*smsSandboxNumber)
	}
	if state.optedOut == nil {
		state.optedOut = make(map[string]bool)
	}
	if state.lastOptIn == nil {
		state.lastOptIn = make(map[string]time.Time)
	}
	if state.origination == nil {
		state.origination = make(map[string]smsOriginationNumber)
	}
}

func (state *smsState) timeNow() time.Time {
	if state.now != nil {
		return state.now().UTC()
	}
	return time.Now().UTC()
}

func (s *Handler) handleSNSSMSQuery(w http.ResponseWriter, r *http.Request, action string) bool {
	switch action {
	case "CheckIfPhoneNumberIsOptedOut", "OptInPhoneNumber", "ListPhoneNumbersOptedOut",
		"CreateSMSSandboxPhoneNumber", "DeleteSMSSandboxPhoneNumber", "VerifySMSSandboxPhoneNumber",
		"GetSMSAttributes", "SetSMSAttributes", "GetSMSSandboxAccountStatus",
		"ListSMSSandboxPhoneNumbers", "ListOriginationNumbers":
	default:
		return false
	}
	state := s.broker.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.sms.initialize()
	requestID := snsRequestID(w)
	result, err := s.broker.smsQueryLocked(&state.sms, r, action, requestID)
	if err != nil {
		if verification, ok := err.(*smsVerificationError); ok {
			smsWriteVerificationError(w, verification, requestID)
		} else {
			snsWriteError(w, err)
		}
		return true
	}
	smsWriteResult(w, action, result, requestID)
	return true
}

func (b *Broker) smsQueryLocked(state *smsState, r *http.Request, action, requestID string) (string, error) {
	switch action {
	case "GetSMSSandboxAccountStatus":
		return "<IsInSandbox>true</IsInSandbox>", nil
	case "CheckIfPhoneNumberIsOptedOut", "OptInPhoneNumber":
		phone, err := smsPhoneParameter(r, "phoneNumber", false)
		if err != nil {
			return "", err
		}
		if action == "CheckIfPhoneNumberIsOptedOut" {
			return fmt.Sprintf("<isOptedOut>%t</isOptedOut>", state.optedOut[phone]), nil
		}
		now := state.timeNow()
		if previous, ok := state.lastOptIn[phone]; ok && now.Sub(previous) < 30*24*time.Hour {
			return "", snsInvalid("A phone number can be opted in only once every 30 days")
		}
		delete(state.optedOut, phone)
		state.lastOptIn[phone] = now
		return "", nil
	case "GetSMSAttributes":
		requested := smsListValues(r, "attributes")
		if len(requested) == 0 {
			for key := range state.attributes {
				requested = append(requested, key)
			}
		}
		sort.Strings(requested)
		var result strings.Builder
		result.WriteString("<attributes>")
		for _, name := range requested {
			if !smsAttributeName(name) {
				return "", snsInvalid("Invalid SMS attribute: " + name)
			}
			if value, ok := state.attributes[name]; ok {
				fmt.Fprintf(&result, "<entry><key>%s</key><value>%s</value></entry>", awsprotocol.XMLEscape(name), awsprotocol.XMLEscape(value))
			}
		}
		result.WriteString("</attributes>")
		return result.String(), nil
	case "SetSMSAttributes":
		attributes := formMapValues(r, "attributes")
		if len(attributes) == 0 {
			return "", snsInvalid("attributes is required")
		}
		for name, value := range attributes {
			if err := smsValidateAttribute(name, value); err != nil {
				return "", err
			}
		}
		for name, value := range attributes {
			state.attributes[name] = value
		}
		return "", nil
	case "CreateSMSSandboxPhoneNumber":
		return b.smsCreateSandboxLocked(state, r, requestID)
	case "VerifySMSSandboxPhoneNumber":
		phone, err := smsPhoneParameter(r, "PhoneNumber", true)
		if err != nil {
			return "", err
		}
		otp := r.FormValue("OneTimePassword")
		if !smsOTPPattern.MatchString(otp) {
			return "", snsInvalid("OneTimePassword must contain 5 to 8 digits")
		}
		number := state.sandbox[phone]
		if number == nil {
			return "", &snsError{Status: http.StatusNotFound, Code: "ResourceNotFound", Message: "Phone number does not exist in the SMS sandbox"}
		}
		now := state.timeNow()
		number.lastAttempt = now
		if number.status == "Verified" || number.otp == "" || !now.Before(number.expiresAt) || subtle.ConstantTimeCompare([]byte(otp), []byte(number.otp)) != 1 {
			return "", &smsVerificationError{message: "The one-time password is invalid or expired", status: "FAILED"}
		}
		number.status = "Verified"
		number.otp = ""
		number.expiresAt = time.Time{}
		return "", nil
	case "DeleteSMSSandboxPhoneNumber":
		phone, err := smsPhoneParameter(r, "PhoneNumber", true)
		if err != nil {
			return "", err
		}
		number := state.sandbox[phone]
		if number == nil {
			return "", &snsError{Status: http.StatusNotFound, Code: "ResourceNotFound", Message: "Phone number does not exist in the SMS sandbox"}
		}
		if state.timeNow().Sub(number.lastAttempt) < 24*time.Hour {
			return "", &snsError{Status: http.StatusBadRequest, Code: "UserError", Message: "Phone numbers can be deleted only 24 hours after the last verification attempt"}
		}
		delete(state.sandbox, phone)
		return "", nil
	case "ListSMSSandboxPhoneNumbers":
		keys := make([]string, 0, len(state.sandbox))
		for phone := range state.sandbox {
			keys = append(keys, phone)
		}
		page, next, err := b.smsPage(r, action, keys, "NextToken", 100, 100)
		if err != nil {
			return "", err
		}
		var result strings.Builder
		result.WriteString("<PhoneNumbers>")
		for _, phone := range page {
			fmt.Fprintf(&result, "<member><PhoneNumber>%s</PhoneNumber><Status>%s</Status></member>", awsprotocol.XMLEscape(phone), state.sandbox[phone].status)
		}
		result.WriteString("</PhoneNumbers>")
		if next != "" {
			fmt.Fprintf(&result, "<NextToken>%s</NextToken>", next)
		}
		return result.String(), nil
	case "ListPhoneNumbersOptedOut":
		keys := make([]string, 0, len(state.optedOut))
		for phone, optedOut := range state.optedOut {
			if optedOut {
				keys = append(keys, phone)
			}
		}
		page, next, err := b.smsPage(r, action, keys, "nextToken", 100, 0)
		if err != nil {
			return "", err
		}
		var result strings.Builder
		result.WriteString("<phoneNumbers>")
		for _, phone := range page {
			fmt.Fprintf(&result, "<member>%s</member>", awsprotocol.XMLEscape(phone))
		}
		result.WriteString("</phoneNumbers>")
		if next != "" {
			fmt.Fprintf(&result, "<nextToken>%s</nextToken>", next)
		}
		return result.String(), nil
	case "ListOriginationNumbers":
		keys := make([]string, 0, len(state.origination))
		for phone := range state.origination {
			keys = append(keys, phone)
		}
		page, next, err := b.smsPage(r, action, keys, "NextToken", 30, 30)
		if err != nil {
			return "", err
		}
		var result strings.Builder
		result.WriteString("<PhoneNumbers>")
		for _, phone := range page {
			number := state.origination[phone]
			fmt.Fprintf(&result, "<member><CreatedAt>%s</CreatedAt><PhoneNumber>%s</PhoneNumber><Status>%s</Status><Iso2CountryCode>%s</Iso2CountryCode><RouteType>%s</RouteType><NumberCapabilities>", number.createdAt.UTC().Format(time.RFC3339Nano), awsprotocol.XMLEscape(number.phoneNumber), awsprotocol.XMLEscape(number.status), awsprotocol.XMLEscape(number.iso2CountryCode), awsprotocol.XMLEscape(number.routeType))
			for _, capability := range number.numberCapabilities {
				fmt.Fprintf(&result, "<member>%s</member>", awsprotocol.XMLEscape(capability))
			}
			result.WriteString("</NumberCapabilities></member>")
		}
		result.WriteString("</PhoneNumbers>")
		if next != "" {
			fmt.Fprintf(&result, "<NextToken>%s</NextToken>", next)
		}
		return result.String(), nil
	}
	return "", snsInvalid("Unknown SMS action")
}

func (b *Broker) smsCreateSandboxLocked(state *smsState, r *http.Request, requestID string) (string, error) {
	phone, err := smsPhoneParameter(r, "PhoneNumber", true)
	if err != nil {
		return "", err
	}
	language := r.FormValue("LanguageCode")
	if language == "" {
		language = "en-US"
	}
	switch language {
	case "en-US", "en-GB", "es-419", "es-ES", "de-DE", "fr-CA", "fr-FR", "it-IT", "ja-JP", "pt-BR", "kr-KR", "zh-CN", "zh-TW":
	default:
		return "", snsInvalid("Invalid LanguageCode")
	}
	if state.optedOut[phone] {
		return "", &snsError{Status: http.StatusBadRequest, Code: "OptedOut", Message: "The phone number has opted out of SMS messages"}
	}
	number := state.sandbox[phone]
	if number == nil && len(state.sandbox) >= 10 {
		return "", &snsError{Status: http.StatusBadRequest, Code: "UserError", Message: "The SMS sandbox supports at most 10 destination phone numbers"}
	}
	if number != nil && number.status == "Verified" {
		return "", &snsError{Status: http.StatusBadRequest, Code: "UserError", Message: "Phone number is already verified"}
	}
	now := state.timeNow()
	var recent []time.Time
	if number != nil {
		for _, sent := range number.sends {
			if now.Sub(sent) < 24*time.Hour {
				recent = append(recent, sent)
			}
		}
	}
	if len(recent) >= 6 {
		return "", &snsError{Status: http.StatusTooManyRequests, Code: "Throttled", Message: "The verification code can be resent only five times in 24 hours"}
	}
	code, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return "", &snsError{Status: http.StatusInternalServerError, Code: "InternalError", Message: "Could not generate a verification code"}
	}
	otp := fmt.Sprintf("%06d", code.Int64())
	if err := b.CaptureSNS(SNSCaptureRecord{
		Operation: "CreateSMSSandboxPhoneNumber", RequestID: requestID,
		PhoneNumber: phone, Message: "Your Amazon SNS verification code is " + otp,
		Deliveries: []SNSCaptureDelivery{{Protocol: "sms", Endpoint: phone, Status: "captured"}},
		Details:    map[string]any{"otp": otp, "language_code": language},
	}); err != nil {
		return "", &snsError{Status: http.StatusInternalServerError, Code: "InternalError", Message: "Could not capture the SMS verification message"}
	}
	if number == nil {
		number = &smsSandboxNumber{}
		state.sandbox[phone] = number
	}
	number.status = "Pending"
	number.otp = otp
	number.expiresAt = now.Add(15 * time.Minute)
	number.lastAttempt = now
	number.sends = append(recent, now)
	return "", nil
}

func (b *Broker) publishSMS(input SNSPublishInput) (SNSPublishResult, error) {
	deliveryMessage, err := smsDeliveryMessage(input.Message, input.MessageStructure)
	if err != nil {
		return SNSPublishResult{}, err
	}
	if !smsE164Pattern.MatchString(input.PhoneNumber) {
		return SNSPublishResult{}, snsInvalid("PhoneNumber must use E.164 format")
	}
	if err := smsValidateMessageAttributes(input.Attributes); err != nil {
		return SNSPublishResult{}, err
	}
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.sms.initialize()
	if state.sms.optedOut[input.PhoneNumber] {
		return SNSPublishResult{}, snsInvalid("The phone number has opted out of SMS messages")
	}
	if number := state.sms.sandbox[input.PhoneNumber]; number == nil || number.status != "Verified" {
		return SNSPublishResult{}, &snsError{Status: http.StatusForbidden, Code: "AuthorizationError", Message: "SMS sandbox phone number is not verified"}
	}
	messageID := uuid.New().String()
	operation := input.Operation
	if operation == "" {
		operation = "Publish"
	}
	if err := b.CaptureSNS(SNSCaptureRecord{
		Operation: operation, RequestID: input.RequestID, MessageID: messageID,
		PhoneNumber: input.PhoneNumber, Subject: input.Subject, Message: input.Message,
		MessageStructure: input.MessageStructure, MessageAttributes: input.Attributes,
		Details:        map[string]any{"delivery_message": deliveryMessage, "sms_attributes": state.sms.attributes},
		MessageGroupID: input.MessageGroupID, MessageDeduplicationID: input.MessageDeduplicationID,
		Deliveries: []SNSCaptureDelivery{{Protocol: "sms", Endpoint: input.PhoneNumber, Status: "captured", MessageID: messageID}},
	}); err != nil {
		return SNSPublishResult{}, &snsError{Status: http.StatusInternalServerError, Code: "InternalError", Message: "Could not capture the SMS message"}
	}
	return SNSPublishResult{MessageID: messageID}, nil
}

// smsCheckDeliveryLocked is also used by topic fanout. Publish accepts SMS
// messages without calling any carrier; unverified/opted-out recipients are a
// delivery failure in the capture, rather than an outbound network operation.
func smsCheckDeliveryLocked(state *smsState, phone string, attributes map[string]MessageAttribute) error {
	state.initialize()
	if !smsE164Pattern.MatchString(phone) {
		return snsInvalid("PhoneNumber must use E.164 format")
	}
	if err := smsValidateMessageAttributes(attributes); err != nil {
		return err
	}
	if state.optedOut[phone] {
		return snsInvalid("The phone number has opted out of SMS messages")
	}
	if number := state.sandbox[phone]; number == nil || number.status != "Verified" {
		return snsInvalid("SMS sandbox phone number is not verified")
	}
	return nil
}

// smsDeliveryMessage preserves the raw message in capture and validates the
// protocol-specific text against SMS's 1,600-character publish limit.
func smsDeliveryMessage(message, structure string) (string, error) {
	if structure == "json" {
		messages, err := snsProtocolMessages(message)
		if err != nil {
			return "", err
		}
		resolved, ok := messages["sms"]
		if !ok {
			resolved = messages["default"]
		}
		message = resolved
	}
	if utf8.RuneCountInString(message) > 1600 {
		return "", snsInvalid("SMS messages must contain at most 1600 characters")
	}
	return message, nil
}

func smsPhoneParameter(r *http.Request, name string, sandbox bool) (string, error) {
	phone := r.FormValue(name)
	if phone == "" {
		return "", snsInvalid(name + " is required")
	}
	valid := smsE164Pattern.MatchString(phone)
	if sandbox {
		valid = len(phone) <= 20 && smsSandboxPhonePattern.MatchString(phone)
	}
	if !valid {
		return "", snsInvalid("Invalid " + name)
	}
	return phone, nil
}

func smsAttributeName(name string) bool {
	switch name {
	case "MonthlySpendLimit", "DeliveryStatusIAMRole", "DeliveryStatusSuccessSamplingRate", "DefaultSenderID", "DefaultSMSType", "UsageReportS3Bucket":
		return true
	default:
		return false
	}
}

func smsValidateAttribute(name, value string) error {
	switch name {
	case "MonthlySpendLimit":
		if !smsNonNegativeDecimal(value) {
			return snsInvalid("MonthlySpendLimit must be a nonnegative finite number")
		}
	case "DeliveryStatusSuccessSamplingRate":
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 || parsed > 100 {
			return snsInvalid("DeliveryStatusSuccessSamplingRate must be an integer between 0 and 100")
		}
	case "DefaultSMSType":
		if value != "Promotional" && value != "Transactional" {
			return snsInvalid("DefaultSMSType must be Promotional or Transactional")
		}
	case "DefaultSenderID":
		if !smsDefaultSenderPattern.MatchString(value) || !smsLetterPattern.MatchString(value) {
			return snsInvalid("DefaultSenderID must contain 1 to 11 alphanumeric characters and at least one letter")
		}
	case "DeliveryStatusIAMRole":
		if value != "" && !smsRolePattern.MatchString(value) {
			return snsInvalid("DeliveryStatusIAMRole must be an IAM role ARN")
		}
	case "UsageReportS3Bucket":
		if value != "" && (!smsBucketPattern.MatchString(value) || strings.Contains(value, "..")) {
			return snsInvalid("UsageReportS3Bucket must be an S3 bucket name")
		}
	default:
		return snsInvalid("Invalid SMS attribute: " + name)
	}
	return nil
}

func smsValidateMessageAttributes(attributes map[string]MessageAttribute) error {
	for name, attribute := range attributes {
		known := name == "AWS.SNS.SMS.SenderID" || name == "AWS.SNS.SMS.SMSType" || name == "AWS.SNS.SMS.MaxPrice" || name == "AWS.MM.SMS.OriginationNumber" || name == "AWS.MM.SMS.EntityId" || name == "AWS.MM.SMS.TemplateId"
		if !known {
			continue
		}
		dataType := strings.SplitN(attribute.DataType, ".", 2)[0]
		if dataType != "String" && (name != "AWS.SNS.SMS.MaxPrice" || dataType != "Number") {
			return snsInvalid(name + " must have String data type (or Number for MaxPrice)")
		}
		value := attribute.StringValue
		switch name {
		case "AWS.SNS.SMS.SenderID":
			if !smsMessageSenderPattern.MatchString(value) || !smsLetterPattern.MatchString(value) {
				return snsInvalid("Invalid SMS SenderID")
			}
		case "AWS.SNS.SMS.SMSType":
			if value != "Promotional" && value != "Transactional" {
				return snsInvalid("Invalid SMS SMSType")
			}
		case "AWS.SNS.SMS.MaxPrice":
			if !smsNonNegativeDecimal(value) {
				return snsInvalid("Invalid SMS MaxPrice")
			}
		case "AWS.MM.SMS.OriginationNumber":
			if !smsOriginationPattern.MatchString(value) {
				return snsInvalid("Invalid SMS OriginationNumber")
			}
		case "AWS.MM.SMS.EntityId", "AWS.MM.SMS.TemplateId":
			if len(value) < 1 || len(value) > 50 {
				return snsInvalid("SMS entity/template IDs must contain 1 to 50 characters")
			}
		}
	}
	return nil
}

func smsNonNegativeDecimal(value string) bool {
	parsed, err := strconv.ParseFloat(value, 64)
	return err == nil && parsed >= 0 && !math.IsInf(parsed, 0) && !math.IsNaN(parsed)
}

func smsListValues(r *http.Request, prefix string) []string {
	var values []string
	for i := 1; i <= 100; i++ {
		name := fmt.Sprintf("%s.member.%d", prefix, i)
		if _, ok := r.Form[name]; !ok {
			break
		}
		values = append(values, r.FormValue(name))
	}
	return values
}

func (b *Broker) smsPage(r *http.Request, operation string, keys []string, tokenName string, defaultLimit, maxLimit int) ([]string, string, error) {
	limit := defaultLimit
	if raw := r.FormValue("MaxResults"); raw != "" && maxLimit > 0 {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > maxLimit {
			code := "InvalidParameter"
			if operation == "ListOriginationNumbers" {
				code = "ValidationException"
			}
			return nil, "", &snsError{Status: http.StatusBadRequest, Code: code, Message: fmt.Sprintf("MaxResults must be between 1 and %d", maxLimit)}
		}
		limit = parsed
	}
	sort.Strings(keys)
	start := 0
	after, err := snsParsePageToken(r.FormValue(tokenName), operation, b.accountID+":"+b.region)
	if err != nil {
		return nil, "", err
	}
	if after != "" {
		start = sort.Search(len(keys), func(i int) bool { return keys[i] > after })
	}

	end := min(start+limit, len(keys))
	if end == len(keys) {
		return keys[start:end], "", nil
	}
	return keys[start:end], snsPageToken(operation, b.accountID+":"+b.region, keys[end-1]), nil
}

type smsVerificationError struct{ message, status string }

func (err *smsVerificationError) Error() string { return err.message }

func smsWriteVerificationError(w http.ResponseWriter, err *smsVerificationError, requestID string) {
	body := fmt.Sprintf(`<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><Error><Type>Sender</Type><Code>VerificationException</Code><Message>%s</Message><Status>%s</Status></Error><RequestId>%s</RequestId></ErrorResponse>`, awsprotocol.XMLEscape(err.message), awsprotocol.XMLEscape(err.status), requestID)
	awsprotocol.XMLResponse(w, http.StatusBadRequest, body)
}

func smsWriteResult(w http.ResponseWriter, action, result, requestID string) {
	body := fmt.Sprintf(`<%sResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><%sResult>%s</%sResult><ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></%sResponse>`, action, action, result, action, requestID, action)
	awsprotocol.XMLResponse(w, http.StatusOK, body)
}
