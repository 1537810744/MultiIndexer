package response

// ApiResponse is the standard JSON envelope used by all API endpoints.
// Used for swagger documentation only — the actual response is built
// by util/http.Resp.
type ApiResponse struct {
	StatusCode int         `json:"status_code" example:"200"`
	Message    string      `json:"message" example:"success"`
	Data       interface{} `json:"data"`
	RequestID  string      `json:"request_id" example:"req-123456"`
}
