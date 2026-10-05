package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/http"
	"net/mail"
	"net/textproto"
	"path/filepath"
	"strings"
)

// sesValidateRaw validates MIME without changing or copying the base64 payload
// into the normalized view. Both sending APIs retain it in the original request.
func sesValidateRaw(api, encoded string, limit int64) (map[string]any, *sesAPIError) {
	if encoded == "" {
		return nil, sesInvalid(api, "RawMessage.Data is required")
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, sesInvalid(api, "RawMessage.Data must be valid base64")
	}
	if int64(len(data)) > limit {
		return nil, sesInvalid(api, fmt.Sprintf("Raw message exceeds the %d MB size limit", limit/(1024*1024)))
	}
	if !bytes.Contains(data, []byte("\r\n\r\n")) && !bytes.Contains(data, []byte("\n\n")) {
		return nil, sesInvalid(api, "Raw message must contain headers and body separated by a blank line")
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSuffix(line, []byte("\r"))
		if len(line)+2 > 1000 {
			return nil, sesInvalid(api, "Raw message lines must not exceed 1000 characters including CRLF")
		}
	}
	message, err := mail.ReadMessage(bytes.NewReader(data))
	if err != nil {
		return nil, sesInvalid(api, "Invalid raw message headers: "+err.Error())
	}
	from, err := mail.ParseAddress(message.Header.Get("From"))
	if err != nil {
		return nil, sesInvalid(api, "Raw message requires a valid From header")
	}
	if apiErr := sesValidateAddress(api, "From", from.Address); apiErr != nil {
		return nil, apiErr
	}
	destination := map[string]any{}
	for _, header := range []struct{ name, field string }{{"To", "ToAddresses"}, {"Cc", "CcAddresses"}, {"Bcc", "BccAddresses"}} {
		var addresses []any
		for _, value := range message.Header[header.name] {
			parsed, err := mail.ParseAddressList(value)
			if err != nil {
				return nil, sesInvalid(api, "Invalid raw "+header.name+" address header")
			}
			for _, address := range parsed {
				if apiErr := sesValidateAddress(api, header.name, address.Address); apiErr != nil {
					return nil, apiErr
				}
				addresses = append(addresses, address.Address)
			}
		}
		if len(addresses) != 0 {
			destination[header.field] = addresses
		}
	}
	parts := 0
	if err := sesValidateMIMEBody(message.Header.Get("Content-Type"), message.Header.Get("Content-Transfer-Encoding"), message.Header.Get("Content-Disposition"), message.Body, &parts); err != nil {
		if rejected, ok := err.(sesUnsupportedAttachmentError); ok {
			return nil, &sesAPIError{Code: "MessageRejected", Message: rejected.Error(), Status: http.StatusBadRequest}
		}
		return nil, sesInvalid(api, "Invalid raw MIME message: "+err.Error())
	}
	headers := map[string]any{}
	for name, values := range message.Header {
		headers[name] = values
	}
	subject := message.Header.Get("Subject")
	if decoded, err := (&mime.WordDecoder{}).DecodeHeader(subject); err == nil {
		subject = decoded
	}
	return map[string]any{"from": from.Address, "destination": destination, "subject": subject, "headers": headers}, nil
}

type sesUnsupportedAttachmentError string

func (err sesUnsupportedAttachmentError) Error() string { return string(err) }

func sesValidateMIMEBody(contentType, encoding, disposition string, body io.Reader, count *int) error {
	*count++
	if *count > 500 {
		return fmt.Errorf("message exceeds 500 MIME parts")
	}
	mediaType := "text/plain"
	params := map[string]string{}
	if contentType != "" {
		var err error
		mediaType, params, err = mime.ParseMediaType(contentType)
		if err != nil {
			return err
		}
	}
	if filename := params["name"]; sesUnsupportedAttachment(filename) {
		return sesUnsupportedAttachmentError("Unsupported attachment file type: " + filename)
	}
	if disposition != "" {
		_, dispositionParams, err := mime.ParseMediaType(disposition)
		if err != nil {
			return err
		}
		if filename := dispositionParams["filename"]; sesUnsupportedAttachment(filename) {
			return sesUnsupportedAttachmentError("Unsupported attachment file type: " + filename)
		}
	}
	if !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		switch strings.ToLower(strings.TrimSpace(encoding)) {
		case "base64":
			body = base64.NewDecoder(base64.StdEncoding, body)
		case "quoted-printable":
			body = quotedprintable.NewReader(body)
		case "", "7bit", "8bit", "binary":
		default:
			return fmt.Errorf("unsupported MIME content-transfer-encoding: %s", encoding)
		}
		_, err := io.Copy(io.Discard, body)
		return err
	}
	if params["boundary"] == "" {
		return fmt.Errorf("multipart MIME content requires a boundary")
	}
	reader := multipart.NewReader(body, params["boundary"])
	found := false
	for {
		part, err := reader.NextRawPart()
		if err == io.EOF {
			if !found {
				return fmt.Errorf("multipart MIME body has no parts")
			}
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		err = sesValidateMIMEBody(part.Header.Get("Content-Type"), part.Header.Get("Content-Transfer-Encoding"), part.Header.Get("Content-Disposition"), part, count)
		_ = part.Close()
		if err != nil {
			return err
		}
	}
}

func sesUnsupportedAttachment(filename string) bool {
	extension := strings.ToLower(filepath.Ext(filename))
	const extensions = ".ade .adp .app .asp .bas .bat .cer .chm .cmd .com .cpl .crt .csh .der .exe .fxp .gadget .hlp .hta .inf .ins .isp .its .js .jse .ksh .lib .lnk .mad .maf .mag .mam .maq .mar .mas .mat .mau .mav .maw .mda .mdb .mde .mdt .mdw .mdz .msc .msh .msh1 .msh2 .mshxml .msh1xml .msh2xml .msi .msp .mst .ops .pcd .pif .plg .prf .prg .reg .scf .scr .sct .shb .shs .sys .ps1 .ps1xml .ps2 .ps2xml .psc1 .psc2 .tmp .url .vb .vbe .vbs .vps .vsmacros .vss .vst .vsw .vxd .ws .wsc .wsf .wsh .xnk"
	return extension != "" && strings.Contains(" "+extensions+" ", " "+extension+" ")
}

type sesSizeWriter int64

func (w *sesSizeWriter) Write(data []byte) (int, error) {
	*w += sesSizeWriter(len(data))
	return len(data), nil
}

func sesCheckMessageSize(api string, email, content map[string]any, limit int64) *sesAPIError {
	var counter sesSizeWriter
	// Count a MIME representation instead of the JSON/base64 transport. The
	// fixed boundary keeps the result stable without retaining a second copy.
	_, _ = fmt.Fprintf(&counter, "From: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\n", sesString(email["from"]), mime.QEncoding.Encode("UTF-8", sesString(email["subject"])))
	for _, key := range []string{"ToAddresses", "CcAddresses", "BccAddresses"} {
		_, _ = fmt.Fprintf(&counter, "%s: %s\r\n", key, strings.Join(sesStrings(sesObject(email["destination"])[key]), ", "))
	}
	for _, value := range sesAnyList(content["Headers"]) {
		header := sesObject(value)
		_, _ = fmt.Fprintf(&counter, "%s: %s\r\n", sesString(header["Name"]), sesString(header["Value"]))
	}
	mixed := multipart.NewWriter(&counter)
	_ = mixed.SetBoundary("eventbus-ses-mixed-boundary")
	_, _ = fmt.Fprintf(&counter, "Content-Type: multipart/mixed; boundary=%q\r\n\r\n", mixed.Boundary())
	bodyHeader := textproto.MIMEHeader{"Content-Type": {"multipart/alternative; boundary=eventbus-ses-body-boundary"}}
	bodyWriter, _ := mixed.CreatePart(bodyHeader)
	alternative := multipart.NewWriter(bodyWriter)
	_ = alternative.SetBoundary("eventbus-ses-body-boundary")
	for _, part := range []struct{ key, mediaType string }{{"text", "text/plain"}, {"html", "text/html"}, {"delivery_status", "message/delivery-status"}} {
		if data, present := email[part.key]; present && data != nil {
			header := textproto.MIMEHeader{"Content-Type": {part.mediaType + "; charset=UTF-8"}, "Content-Transfer-Encoding": {"quoted-printable"}}
			writer, _ := alternative.CreatePart(header)
			encoded := quotedprintable.NewWriter(writer)
			_, _ = io.WriteString(encoded, sesString(data))
			_ = encoded.Close()
		}
	}
	_ = alternative.Close()
	for _, value := range sesAnyList(content["Attachments"]) {
		attachment := sesObject(value)
		encodedData := sesString(attachment["RawContent"])
		data := base64.NewDecoder(base64.StdEncoding, strings.NewReader(encodedData))
		encoding := sesString(attachment["ContentTransferEncoding"])
		if encoding == "" {
			encoding = "SEVEN_BIT"
		}
		mediaType := sesString(attachment["ContentType"])
		if mediaType == "" {
			mediaType = mime.TypeByExtension(filepath.Ext(sesString(attachment["FileName"])))
			if mediaType == "" {
				mediaType = "application/octet-stream"
			}
		}
		disposition := strings.ToLower(sesString(attachment["ContentDisposition"]))
		if disposition == "" {
			disposition = "attachment"
		}
		mimeEncoding := map[string]string{"SEVEN_BIT": "7bit", "BASE64": "base64", "QUOTED_PRINTABLE": "quoted-printable"}[encoding]
		header := textproto.MIMEHeader{
			"Content-Type":              {mediaType},
			"Content-Disposition":       {mime.FormatMediaType(disposition, map[string]string{"filename": sesString(attachment["FileName"])})},
			"Content-Transfer-Encoding": {mimeEncoding},
		}
		if id := sesString(attachment["ContentId"]); id != "" {
			header.Set("Content-ID", "<"+id+">")
		}
		if description := sesString(attachment["ContentDescription"]); description != "" {
			header.Set("Content-Description", description)
		}
		writer, _ := mixed.CreatePart(header)
		switch encoding {
		case "BASE64":
			// MIME base64 is folded at 76 characters, unlike the JSON blob.
			decodedSize, _ := io.Copy(io.Discard, data)
			length := int64(base64.StdEncoding.EncodedLen(int(decodedSize)))
			counter += sesSizeWriter(length + 2*((length+75)/76))
		case "QUOTED_PRINTABLE":
			encoded := quotedprintable.NewWriter(writer)
			_, _ = io.Copy(encoded, data)
			_ = encoded.Close()
		default:
			_, _ = io.Copy(writer, data)
		}
	}
	_ = mixed.Close()
	if int64(counter) > limit {
		return &sesAPIError{Code: "MessageRejected", Message: fmt.Sprintf("Email message exceeds the maximum size of %d MiB after MIME encoding", limit/(1<<20)), Status: http.StatusBadRequest}
	}
	return nil
}

func sesAnyList(value any) []any {
	list, _ := value.([]any)
	return list
}
