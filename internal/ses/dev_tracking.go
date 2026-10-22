package ses

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"strings"
)

// Local representations share accepted-message expiry, eviction and byte limits.
// They are separate from submitted content and schema-version-1 capture records.
type devTrackingMessage struct {
	token   string
	html    string
	charset string
}

// Preparation retains references to the request's already-owned content, not
// rendered HTML copies. Rendering starts only after durable native acceptance.
type devTrackingSource struct {
	api          string
	input, email map[string]any
}
type devTrackingState struct {
	tokens map[string]string
}

// WithDevTracking enables local post-send representations and pixel resources.
// HTTP composition must expose ServeDevTracking separately from SES's AWS APIs.
func WithDevTracking() ManagerOption {
	return func(manager *SESManager) {
		manager.devTracking = &devTrackingState{tokens: make(map[string]string)}
	}
}

func (manager *SESManager) prepareDevTracking(api string, input, email map[string]any) *devTrackingMessage {
	html, charset := manager.devTrackingHTML(api, input, email)
	if html == "" {
		return nil
	}
	token := rand.Text()
	pixel := `<img src="` + DevTrackingPixelPrefix + token + `.gif" width="1" height="1" alt="">`
	const marker = "{{ses:openTracker}}"
	if before, after, found := strings.Cut(html, marker); found {
		html = before + pixel + strings.ReplaceAll(after, marker, "")
	} else {
		// Appending avoids mistaking closing-tag text in comments/scripts for
		// markup. Browsers place the trailing image in the document body.
		html += pixel
	}
	return &devTrackingMessage{token: token, html: html, charset: charset}
}

func (manager *SESManager) devTrackingHTML(api string, input, email map[string]any) (string, string) {
	if sesString(email["content_type"]) == "raw" {
		encoded := sesString(sesObject(input["RawMessage"])["Data"])
		if api == "v2" {
			encoded = sesString(sesObject(sesObject(input["Content"])["Raw"])["Data"])
		}
		message, err := mail.ReadMessage(base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded)))
		if err != nil {
			return "", ""
		}
		parts := 0
		return devMIMEHTML(message.Header.Get("Content-Type"), message.Header.Get("Content-Transfer-Encoding"),
			message.Header.Get("Content-Disposition"), message.Body, 0, &parts)
	}
	if sesString(email["content_type"]) != "template" {
		return sesString(email["html"]), "utf-8"
	}
	// Native capture rendering remains unchanged. Bind the provider marker only
	// in detached development rendering data, reusing the existing renderer.
	// Always start from template source: a user binding for the reserved name
	// may already have replaced it in the optional capture rendering.
	source := ""
	if api == "v1" {
		template, found := manager.template(sesString(email["template"]))
		if !found {
			return "", ""
		}
		source = template.HtmlPart
	} else {
		template := sesObject(sesObject(input["Content"])["Template"])
		if template == nil {
			template = sesObject(sesObject(input["DefaultContent"])["Template"])
		}
		if inline := sesObject(template["TemplateContent"]); inline != nil {
			source = sesString(inline["Html"])
		} else if content, found := manager.template(sesString(email["template"])); found {
			source = content.HtmlPart
		}
	}
	values, err := sesTemplateData(sesString(email["template_data"]))
	if err != nil {
		return "", ""
	}
	values["ses:openTracker"] = "{{ses:openTracker}}"
	data, _ := json.Marshal(values)
	rendered, err := sesRenderTemplate(SESTemplate{HtmlPart: source}, string(data))
	if err != nil {
		return "", ""
	}
	return sesString(rendered["html"]), "utf-8"
}

// Select a body HTML part without interpreting attachment HTML. All readers are
// bounded by native message quotas; the local recursion limit is independent of
// native acceptance and never changes the submitted message or API outcome.
func devMIMEHTML(contentType, encoding, disposition string, body io.Reader, depth int, parts *int) (string, string) {
	*parts++
	if depth > 32 || *parts > 500 {
		return "", ""
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return "", ""
	}
	if params["name"] != "" {
		return "", ""
	}
	if disposition != "" {
		kind, attributes, err := mime.ParseMediaType(disposition)
		if err != nil || strings.EqualFold(kind, "attachment") || attributes["filename"] != "" {
			return "", ""
		}
	}
	if strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		if params["boundary"] == "" {
			return "", ""
		}
		reader := multipart.NewReader(body, params["boundary"])
		for {
			part, err := reader.NextRawPart()
			if err != nil {
				return "", ""
			}
			html, charset := devMIMEHTML(part.Header.Get("Content-Type"), part.Header.Get("Content-Transfer-Encoding"),
				part.Header.Get("Content-Disposition"), part, depth+1, parts)
			_ = part.Close()
			if html != "" {
				return html, charset
			}
		}
	}
	if !strings.EqualFold(mediaType, "text/html") {
		return "", ""
	}
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "base64":
		body = base64.NewDecoder(base64.StdEncoding, body)
	case "quoted-printable":
		body = quotedprintable.NewReader(body)
	case "", "7bit", "8bit", "binary":
	default:
		return "", ""
	}
	data, err := io.ReadAll(io.LimitReader(body, sesV2MaxMessageBytes+1))
	if err != nil || int64(len(data)) > sesV2MaxMessageBytes {
		return "", ""
	}
	charset := params["charset"]
	if charset == "" {
		charset = "utf-8"
	}
	// Pixel markup is ASCII. Non-ASCII-compatible encodings cannot be amended
	// without transcoding and remain available only in original capture.
	lower := strings.ToLower(charset)
	if lower != "utf-8" && lower != "utf8" && lower != "us-ascii" && lower != "ascii" &&
		!strings.HasPrefix(lower, "iso-8859-") && !strings.HasPrefix(lower, "windows-") {
		return "", ""
	}
	return string(data), charset
}

func (manager *SESManager) retainDevTrackingLocked(accepted *acceptedMessage) {
	if manager.devTracking != nil && accepted.devTracking != nil {
		manager.devTracking.tokens[accepted.devTracking.token] = accepted.id
	}
}
func (manager *SESManager) removeDevTrackingLocked(accepted *acceptedMessage) {
	if manager.devTracking != nil && accepted.devTracking != nil {
		delete(manager.devTracking.tokens, accepted.devTracking.token)
		// Pending Send publication may still hold the accepted metadata. It
		// must not extend retired rendered-body lifetime through the whole bulk.
		accepted.devTracking = nil
	}
}
