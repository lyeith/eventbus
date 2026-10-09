package ses

type sesAPIError struct {
	Code, Message string
	Status        int
	Fields        map[string]any
}
type sesSendResult struct {
	Output map[string]any
	Emails []map[string]any
}
