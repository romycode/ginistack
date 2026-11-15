package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMalformedAWSQueryPreservesServiceError(t *testing.T) {
	handler := newHandler()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
		"Action=CreateQueue&Version=2012-11-05&QueueName=orders&Invalid=%zz",
	))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "Invalid AWS Query form.") {
		t.Fatalf("CreateQueue with a malformed form returned HTTP %d %s", response.Code, response.Body.String())
	}
}
