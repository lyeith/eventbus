package ses

type sesAPIError struct {
	Code, Message string
	Status        int
}
type sesSendResult struct {
	Output map[string]any
	Emails []map[string]any
}
