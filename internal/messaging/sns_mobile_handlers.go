package messaging

import (
	"net/http"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// handleSNSMobileQuery owns all ten platform application/endpoint operations.
func (s *Handler) handleSNSMobileQuery(w http.ResponseWriter, r *http.Request, action string) bool {
	var result string
	var err error
	switch action {
	case "CreatePlatformApplication":
		var arn string
		arn, err = s.broker.mobileCreateApplication(r.FormValue("Name"), r.FormValue("Platform"), formMapValues(r, "Attributes"))
		result = mobileXMLElement("PlatformApplicationArn", arn)
	case "CreatePlatformEndpoint":
		var customUserData *string
		_ = r.ParseForm()
		if values, exists := r.Form["CustomUserData"]; exists && len(values) != 0 {
			customUserData = &values[0]
		}
		var arn string
		arn, err = s.broker.mobileCreateEndpoint(r.FormValue("PlatformApplicationArn"), r.FormValue("Token"), customUserData, formMapValues(r, "Attributes"))
		result = mobileXMLElement("EndpointArn", arn)
	case "DeleteEndpoint":
		err = s.broker.mobileDeleteEndpoint(r.FormValue("EndpointArn"))
	case "DeletePlatformApplication":
		err = s.broker.mobileDeleteApplication(r.FormValue("PlatformApplicationArn"))
	case "GetEndpointAttributes":
		var attributes map[string]string
		attributes, err = s.broker.mobileGetEndpointAttributes(r.FormValue("EndpointArn"))
		result = mobileXMLAttributes(attributes)
	case "GetPlatformApplicationAttributes":
		var attributes map[string]string
		attributes, err = s.broker.mobileGetApplicationAttributes(r.FormValue("PlatformApplicationArn"))
		result = mobileXMLAttributes(attributes)
	case "SetEndpointAttributes":
		err = s.broker.mobileSetEndpointAttributes(r.FormValue("EndpointArn"), formMapValues(r, "Attributes"))
	case "SetPlatformApplicationAttributes":
		err = s.broker.mobileSetApplicationAttributes(r.FormValue("PlatformApplicationArn"), formMapValues(r, "Attributes"))
	case "ListPlatformApplications":
		result, err = s.broker.mobileListApplicationsXML(r.FormValue("NextToken"))
	case "ListEndpointsByPlatformApplication":
		result, err = s.broker.mobileListEndpointsXML(r.FormValue("PlatformApplicationArn"), r.FormValue("NextToken"))
	default:
		return false
	}
	if err != nil {
		snsWriteError(w, err)
		return true
	}
	snsResponse(w, action, result)
	return true
}

func mobileXMLElement(name, value string) string {
	return "<" + name + ">" + awsprotocol.XMLEscape(value) + "</" + name + ">"
}

func mobileXMLAttributes(attributes map[string]string) string {
	return "<Attributes>" + snsMapXML(attributes) + "</Attributes>"
}

func (b *Broker) mobileListApplicationsXML(nextToken string) (string, error) {
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	keys := make([]string, 0, len(state.mobile.applications))
	for arn := range state.mobile.applications {
		keys = append(keys, arn)
	}
	page, next, err := mobilePage(keys, nextToken, "applications")
	if err != nil {
		return "", err
	}
	var result strings.Builder
	result.WriteString("<PlatformApplications>")
	for _, arn := range page {
		result.WriteString("<member>")
		result.WriteString(mobileXMLElement("PlatformApplicationArn", arn))
		result.WriteString(mobileXMLAttributes(mobilePublicApplicationAttributes(state.mobile.applications[arn])))
		result.WriteString("</member>")
	}
	result.WriteString("</PlatformApplications>")
	if next != "" {
		result.WriteString(mobileXMLElement("NextToken", next))
	}
	return result.String(), nil
}

func (b *Broker) mobileListEndpointsXML(applicationARN, nextToken string) (string, error) {
	if err := b.mobileValidateARN(applicationARN, "app"); err != nil {
		return "", err
	}
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.mobile.applications[applicationARN] == nil {
		return "", snsNotFound("Platform application does not exist")
	}
	keys := make([]string, 0)
	for arn, endpoint := range state.mobile.endpoints {
		if endpoint.applicationARN == applicationARN {
			keys = append(keys, arn)
		}
	}
	page, next, err := mobilePage(keys, nextToken, "endpoints:"+applicationARN)
	if err != nil {
		return "", err
	}
	var result strings.Builder
	result.WriteString("<Endpoints>")
	for _, arn := range page {
		result.WriteString("<member>")
		result.WriteString(mobileXMLElement("EndpointArn", arn))
		result.WriteString(mobileXMLAttributes(state.mobile.endpoints[arn].attributes))
		result.WriteString("</member>")
	}
	result.WriteString("</Endpoints>")
	if next != "" {
		result.WriteString(mobileXMLElement("NextToken", next))
	}
	return result.String(), nil
}
