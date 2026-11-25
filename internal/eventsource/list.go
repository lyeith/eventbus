package eventsource

import (
	"encoding/base64"
	"encoding/json"
	"sort"
)

const maxListMappingsPage = 100
const maxListMappingsItems = 10000

type ListInput struct {
	EventSourceARN string
	FunctionName   string
	Marker         string
	MaxItems       *int
}

type ListOutput struct {
	EventSourceMappings []Mapping `json:"EventSourceMappings"`
	NextMarker          string    `json:"NextMarker,omitempty"`
}

type mappingListCursor struct {
	Source   string `json:"source"`
	Function string `json:"function"`
	After    string `json:"after"`
}

// List reads the current mapping snapshots. A marker ties the next page to the
// same source and function selection and survives deletion of the last ID seen.
func (s *Service) List(input ListInput) (ListOutput, error) {
	pageSize := maxListMappingsPage
	if input.MaxItems != nil {
		items := *input.MaxItems
		if items < 1 || items > maxListMappingsItems {
			return ListOutput{}, invalid("MaxItems must be 1 through 10000")
		}
		pageSize = min(pageSize, items)
	}
	if input.EventSourceARN != "" {
		if err := s.validateSourceARN(input.EventSourceARN); err != nil {
			return ListOutput{}, err
		}
	}
	var functionARN string
	if input.FunctionName != "" {
		var err error
		functionARN, err = s.functionARN(input.FunctionName)
		if err != nil {
			return ListOutput{}, err
		}
	}
	cursor := mappingListCursor{Source: input.EventSourceARN, Function: functionARN}
	if input.Marker != "" {
		after, err := cursor.readMarker(input.Marker)
		if err != nil {
			return ListOutput{}, err
		}
		cursor.After = after
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.entries))
	for id, item := range s.entries {
		if id <= cursor.After ||
			(cursor.Source != "" && item.mapping.EventSourceARN != cursor.Source) ||
			(cursor.Function != "" && item.mapping.FunctionARN != cursor.Function) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	result := ListOutput{EventSourceMappings: make([]Mapping, 0, min(pageSize, len(ids)))}
	if len(ids) > pageSize {
		ids = ids[:pageSize]
		cursor.After = ids[len(ids)-1]
		result.NextMarker = cursor.marker()
	}
	for _, id := range ids {
		result.EventSourceMappings = append(result.EventSourceMappings, cloneMapping(s.entries[id].mapping))
	}
	return result, nil
}

func (cursor mappingListCursor) readMarker(raw string) (string, error) {
	// A marker contains only the supplied ARNs, a UUID and fixed framing.
	if len(raw) > base64.RawURLEncoding.EncodedLen(len(cursor.Source)+len(cursor.Function)+128) {
		return "", invalid("Invalid Marker")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(raw)
	var next mappingListCursor
	if err != nil || json.Unmarshal(encoded, &next) != nil ||
		next.Source != cursor.Source || next.Function != cursor.Function || !idPattern.MatchString(next.After) {
		return "", invalid("Invalid Marker")
	}
	return next.After, nil
}

func (cursor mappingListCursor) marker() string {
	data, _ := json.Marshal(cursor) // Only strings are encoded.
	return base64.RawURLEncoding.EncodeToString(data)
}
