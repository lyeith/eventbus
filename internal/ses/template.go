package ses

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var sesSubstitution = regexp.MustCompile(`\{\{\{?\s*([^{}]+?)\s*\}?\}\}`)

// This is an optional capture view. The request and original template data stay
// intact; unsupported rich Handlebars expressions do not change API acceptance.
func sesRenderTemplate(template SESTemplate, data string) (map[string]any, error) {
	var values map[string]any
	decoder := json.NewDecoder(strings.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("capture template data: %w", err)
	}
	if values == nil {
		return nil, errors.New("capture template data must be a JSON object")
	}
	result := map[string]any{}
	for field, text := range map[string]string{"subject": template.SubjectPart, "text": template.TextPart, "html": template.HtmlPart} {
		var renderErr error
		rendered := sesSubstitution.ReplaceAllStringFunc(text, func(match string) string {
			parts := sesSubstitution.FindStringSubmatch(match)
			path := strings.TrimSpace(parts[1])
			if strings.ContainsAny(path, "#/@() \t") {
				renderErr = fmt.Errorf("capture view does not render rich Handlebars expression %q", path)
				return match
			}
			var value any = values
			for _, component := range strings.Split(path, ".") {
				object, ok := value.(map[string]any)
				if !ok {
					renderErr = fmt.Errorf("capture substitution %q is unavailable", path)
					return match
				}
				value, ok = object[component]
				if !ok {
					renderErr = fmt.Errorf("capture substitution %q is unavailable", path)
					return match
				}
			}
			if value == nil {
				return ""
			}
			return fmt.Sprint(value)
		})
		if renderErr != nil {
			return nil, renderErr
		}
		result[field] = rendered
	}
	return result, nil
}
