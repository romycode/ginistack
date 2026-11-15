package sqs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPRequestBodySizeLimit(t *testing.T) {
	t.Parallel()
	const limit = 1_638_400
	const payload = `{}`
	handler := NewHandler()

	for _, test := range []struct {
		name   string
		length int
		stream bool
		want   int
	}{
		{"known length at limit", limit, false, http.StatusOK},
		{"known length over limit", limit + 1, false, http.StatusRequestEntityTooLarge},
		{"stream at limit", limit, true, http.StatusOK},
		{"stream over limit", limit + 1, true, http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := payload + strings.Repeat(" ", test.length-len(payload))
			request := httptest.NewRequest(http.MethodPost, "http://localhost/", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/x-amz-json-1.0")
			request.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			if test.stream {
				request.ContentLength = -1
				request.TransferEncoding = []string{"chunked"}
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("got HTTP %d, want %d", response.Code, test.want)
			}
			if test.want == http.StatusRequestEntityTooLarge && response.Body.Len() != 0 {
				t.Errorf("got response body %q, want an empty body", response.Body.String())
			}
		})
	}
}
