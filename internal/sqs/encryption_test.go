package sqs

import (
	"context"
	"testing"
)

func TestManagedEncryptionRetainsMessageAcrossAttributeChange(t *testing.T) {
	const body = "á🍊日本語"
	api := &store{bodyCipher: newBodyCipher(), queues: make(map[queueKey]*queue)}
	key := queueKey{region: "us-east-1", name: "orders"}
	if failure := api.create(key, "arn:aws:sqs:us-east-1:000000000000:orders", map[string]string{"SqsManagedSseEnabled": "true"}, nil, false); failure != nil {
		t.Fatal(failure)
	}
	if _, failure := api.send(key, messageInput{MessageBody: body}, false); failure != nil {
		t.Fatal(failure)
	}
	q := api.queues[key]
	if stored := q.messages[0]; stored.body != "" || len(stored.encryptedBody) == 0 {
		t.Fatal("expected the enqueued body to be stored only as ciphertext")
	}
	config, failure := q.queueConfig.withAttributes(key.name, map[string]string{"SqsManagedSseEnabled": "false"}, false)
	if failure != nil {
		t.Fatal(failure)
	}
	q.queueConfig = config
	messages, failure := api.receive(context.Background(), key, 1, nil, nil, nil, nil, "")
	if failure != nil || len(messages) != 1 || messages[0].Body != body || messages[0].MD5OfBody != checksum(body) {
		t.Fatalf("receive after disabling encryption = %+v, %v", messages, failure)
	}
}
