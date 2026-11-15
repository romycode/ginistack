package sqs

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueuePolicyUsesRequestTLS(t *testing.T) {
	t.Parallel()
	handler := NewHandler()
	call := func(scheme, operation string, input any) *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, scheme+"://localhost/", strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/x-amz-json-1.0")
		request.Header.Set("X-Amz-Target", "AmazonSQS."+operation)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	document := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"sqs:SendMessage","Resource":"*","Condition":{"Bool":{"aws:SecureTransport":"false"}}}]}`
	created := call("http", "CreateQueue", createInput{QueueName: "orders", Attributes: map[string]string{"Policy": document}})
	var address queueInput
	if created.Code != http.StatusOK || json.Unmarshal(created.Body.Bytes(), &address) != nil || address.QueueURL == "" {
		t.Fatalf("CreateQueue(orders) = HTTP %d %s, want queue URL", created.Code, created.Body.String())
	}

	for _, operation := range []struct {
		name  string
		input any
	}{
		{"SendMessage", map[string]any{"QueueUrl": address.QueueURL, "MessageBody": "single"}},
		{"SendMessageBatch", map[string]any{"QueueUrl": address.QueueURL, "Entries": []map[string]string{{"Id": "msg", "MessageBody": "batch"}}}},
	} {
		rejected := call("http", operation.name, operation.input)
		if rejected.Code != http.StatusForbidden || !strings.Contains(rejected.Body.String(), "AccessDenied") {
			t.Errorf("%s over HTTP = %d %s, want 403 AccessDenied", operation.name, rejected.Code, rejected.Body.String())
		}
		accepted := call("https", operation.name, operation.input)
		if accepted.Code != http.StatusOK {
			t.Errorf("%s over HTTPS = %d %s, want 200", operation.name, accepted.Code, accepted.Body.String())
		}
	}

	received := call("http", "ReceiveMessage", map[string]any{"QueueUrl": address.QueueURL, "MaxNumberOfMessages": 10})
	var output receiveOutput
	if received.Code != http.StatusOK || json.Unmarshal(received.Body.Bytes(), &output) != nil || len(output.Messages) != 2 {
		t.Fatalf("ReceiveMessage after HTTP and HTTPS sends = %d %s, want two messages from HTTPS", received.Code, received.Body.String())
	}
}
