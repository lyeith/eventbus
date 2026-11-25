package eventsource

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

const maxMappingTags = 50

func validateMappingTags(tags map[string]string) error {
	if len(tags) > maxMappingTags {
		return invalid("Event source mappings support up to 50 tags")
	}
	for key, value := range tags {
		if err := validateMappingTagKey(key); err != nil {
			return err
		}
		if !validMappingTagText(value, 256) {
			return invalid("Tag values must contain up to 256 supported characters")
		}
	}
	return nil
}

func validateMappingTagKey(key string) error {
	if key == "" || !validMappingTagText(key, 128) {
		return invalid("Tag keys must contain 1 through 128 supported characters")
	}
	if len(key) >= len("aws:") && strings.EqualFold(key[:len("aws:")], "aws:") {
		return invalid("The aws: tag key prefix is reserved")
	}
	return nil
}

func validMappingTagText(value string, limit int) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > limit {
		return false
	}
	for _, character := range value {
		if !unicode.IsLetter(character) && !unicode.IsNumber(character) &&
			!unicode.Is(unicode.Z, character) && !strings.ContainsRune("_.:/=+-@", character) {
			return false
		}
	}
	return true
}

// mappingTagEntry must be called with the service lock held. The full ARN
// comparison prevents a different account or region from using the same UUID.
func (s *Service) mappingTagEntry(arn string) (*entry, error) {
	_, id, _ := strings.Cut(arn, ":event-source-mapping:")
	item := s.entries[id]
	if item == nil || item.mapping.EventSourceMappingARN != arn {
		return nil, missing("Event source mapping not found")
	}
	return item, nil
}

func (s *Service) ListTags(arn string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, err := s.mappingTagEntry(arn)
	if err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(item.tags))
	maps.Copy(tags, item.tags)
	return tags, nil
}

func (s *Service) TagResource(arn string, tags map[string]string) error {
	if tags == nil {
		return invalid("Tags is required")
	}
	if err := validateMappingTags(tags); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return unavailable()
	}
	item, err := s.mappingTagEntry(arn)
	if err != nil {
		return err
	}
	if item.mapping.State == "Deleting" {
		return mappingTagConflict()
	}
	updated := make(map[string]string, len(item.tags)+len(tags))
	maps.Copy(updated, item.tags)
	maps.Copy(updated, tags)
	if len(updated) > maxMappingTags {
		return invalid("Event source mappings support up to 50 tags")
	}
	item.tags = updated
	return nil
}

func (s *Service) UntagResource(arn string, keys []string) error {
	if len(keys) < 1 || len(keys) > maxMappingTags {
		return invalid("Provide 1 through 50 tagKeys")
	}
	for _, key := range keys {
		if err := validateMappingTagKey(key); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return unavailable()
	}
	item, err := s.mappingTagEntry(arn)
	if err != nil {
		return err
	}
	if item.mapping.State == "Deleting" {
		return mappingTagConflict()
	}
	for _, key := range keys {
		delete(item.tags, key)
	}
	return nil
}

func mappingTagConflict() error {
	return &APIError{Code: "ResourceConflictException", Status: http.StatusConflict, Message: "Event source mapping is deleting"}
}

func (h *Handler) serveMappingTags(w http.ResponseWriter, r *http.Request) {
	arn := strings.TrimPrefix(r.URL.Path, awsprotocol.LambdaTagsPathPrefix)
	switch r.Method {
	case http.MethodGet:
		tags, err := h.service.ListTags(arn)
		if err != nil {
			writeError(w, err)
			return
		}
		_ = json.NewEncoder(w).Encode(struct {
			Tags map[string]string `json:"Tags"`
		}{Tags: tags})
	case http.MethodPost:
		var input struct {
			Tags map[string]*string `json:"Tags"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || input.Tags == nil {
			writeError(w, invalid("TagResource requires a Tags object"))
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			writeError(w, invalid("Request must contain one JSON object"))
			return
		}
		tags := make(map[string]string, len(input.Tags))
		for key, value := range input.Tags {
			if value == nil {
				writeError(w, invalid("Tag values must be strings"))
				return
			}
			tags[key] = *value
		}
		if err := h.service.TagResource(arn, tags); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeError(w, invalid("Invalid UntagResource query"))
			return
		}
		if err := h.service.UntagResource(arn, query["tagKeys"]); err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		writeError(w, &APIError{Code: "InvalidParameterValueException", Status: http.StatusMethodNotAllowed, Message: "Unsupported mapping tag method"})
	}
}
