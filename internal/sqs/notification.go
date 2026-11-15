package sqs

import (
	"strings"

	"github.com/romycode/ginistack/internal/policy"
)

type Notification struct {
	QueueARN, TopicARN, Body string
	GroupID, DeduplicationID *string
	Attributes               map[string]NotificationAttribute
}

type NotificationAttribute struct {
	DataType    string
	StringValue *string
	BinaryValue []byte
}

// DeliverNotification evaluates the queue policy as the SNS service principal.
func (r *Handler) DeliverNotification(notification Notification) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	q := r.store.targetQueue(notification.QueueARN)
	if q == nil {
		return
	}
	topic := strings.SplitN(notification.TopicARN, ":", 6)
	if len(topic) != 6 {
		return
	}
	context := map[string]string{
		"aws:SourceArn": notification.TopicARN, "aws:SourceAccount": topic[4],
		"aws:PrincipalIsAWSService": "true", "aws:SecureTransport": "true",
	}
	for name, value := range q.tags {
		context["aws:ResourceTag/"+name] = value
	}
	request := policy.Request{
		PrincipalService: "sns.amazonaws.com", Action: "sqs:SendMessage", Resource: q.arn, Context: context,
	}
	if q.accessPolicy.Evaluate(request) != policy.Allow {
		return
	}
	attributes := make(map[string]messageAttribute, len(notification.Attributes))
	for name, attribute := range notification.Attributes {
		attributes[name] = messageAttribute(attribute)
	}
	input := messageInput{
		MessageBody: notification.Body, MessageAttributes: attributes,
		MessageGroupID: notification.GroupID,
	}
	if q.fifo {
		input.MessageDeduplicationID = notification.DeduplicationID
	}
	_, _ = q.sendWithinLimit(input, maxMessageSize)
}
