package sqs

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
)

const accountID = "000000000000"

var queueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

type service struct {
	store store
}

type empty struct{}

type createInput struct {
	QueueName  string
	Attributes map[string]string
	Tags       map[string]string
}

type queueInput struct {
	QueueURL string `json:"QueueUrl"`
}

type sendInput struct {
	queueInput
	messageInput
}

type sendOutput struct {
	MessageID                    string `json:"MessageId"`
	MD5OfMessageBody             string
	MD5OfMessageAttributes       string `json:",omitempty"`
	MD5OfMessageSystemAttributes string `json:",omitempty"`
	SequenceNumber               string `json:",omitempty"`
}

type receiveInput struct {
	queueInput
	MaxNumberOfMessages         *int
	VisibilityTimeout           *int
	WaitTimeSeconds             *int
	MessageSystemAttributeNames []string
	AttributeNames              []string
	MessageAttributeNames       []string
	ReceiveRequestAttemptID     string `json:"ReceiveRequestAttemptId"`
}

type receiptInput struct {
	queueInput
	ReceiptHandle string
}

type changeVisibilityInput struct {
	receiptInput
	VisibilityTimeout *int
}

func (s *service) createQueue(request *http.Request, input createInput) (queueInput, *apiError) {
	if !validQueueName(input.QueueName) {
		return queueInput{}, &apiError{"InvalidParameterValue", "Invalid queue name.", 400}
	}
	config, failure := newQueueConfig(input.QueueName, input.Attributes)
	if failure != nil {
		return queueInput{}, failure
	}
	arn := "arn:aws:sqs:" + requestRegion(request) + ":" + accountID + ":" + input.QueueName
	if failure := s.store.create(input.QueueName, arn, config, input.Tags, input.Attributes["RedrivePolicy"]); failure != nil {
		return queueInput{}, failure
	}
	return queueAddress(request, input.QueueName), nil
}

func queueAddress(request *http.Request, name string) queueInput {
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	queueURL := url.URL{Scheme: scheme, Host: request.Host, Path: "/" + accountID + "/" + name}
	return queueInput{QueueURL: queueURL.String()}
}

type getQueueURLInput struct {
	QueueName              string
	QueueOwnerAWSAccountID string `json:"QueueOwnerAWSAccountId"`
}

func (s *service) getQueueURL(request *http.Request, input getQueueURLInput) (queueInput, *apiError) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if s.store.queues[input.QueueName] == nil || (input.QueueOwnerAWSAccountID != "" && input.QueueOwnerAWSAccountID != accountID) {
		return queueInput{}, &apiError{"AWS.SimpleQueueService.NonExistentQueue", missingQueue.message, 400}
	}
	return queueAddress(request, input.QueueName), nil
}

func (s *service) deleteQueue(_ *http.Request, input queueInput) (empty, *apiError) {
	name, failure := queueName(input)
	if failure != nil {
		return empty{}, failure
	}
	return empty{}, s.store.remove(name)
}

type getAttributesInput struct {
	queueInput
	AttributeNames []string
}

type getAttributesOutput struct {
	Attributes map[string]string `json:",omitempty"`
}

type listTagsOutput struct {
	Tags map[string]string `json:",omitempty"`
}

func (s *service) getQueueAttributes(_ *http.Request, input getAttributesInput) (getAttributesOutput, *apiError) {
	name, failure := queueName(input.queueInput)
	if failure != nil {
		return getAttributesOutput{}, failure
	}
	attributes, failure := s.store.queueAttributes(name, input.AttributeNames)
	return getAttributesOutput{Attributes: attributes}, failure
}

type setAttributesInput struct {
	queueInput
	Attributes map[string]string
}

func (s *service) setQueueAttributes(_ *http.Request, input setAttributesInput) (empty, *apiError) {
	name, failure := queueName(input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	return empty{}, s.store.configure(name, input.Attributes)
}

func (s *service) listQueueTags(_ *http.Request, input queueInput) (listTagsOutput, *apiError) {
	name, failure := queueName(input)
	if failure != nil {
		return listTagsOutput{}, failure
	}
	tags, failure := s.store.queueTags(name)
	return listTagsOutput{Tags: tags}, failure
}

func (s *service) sendMessage(_ *http.Request, input sendInput) (sendOutput, *apiError) {
	name, failure := queueName(input.queueInput)
	if failure != nil {
		return sendOutput{}, failure
	}
	return s.store.send(name, input.messageInput)
}

const maxMessageSize = 1 << 20

func validateMessage(body string, delay *int) *apiError {
	if len(body) == 0 || len(body) > maxMessageSize {
		return &apiError{"InvalidParameterValue", "MessageBody must contain between 1 and 1048576 bytes.", 400}
	}
	for _, character := range body {
		if !validMessageCharacter(character) {
			return &apiError{"InvalidMessageContents", fmt.Sprintf("Invalid binary character '#x%X' was found in the message body, the set of allowed characters is #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF].", character), 400}
		}
	}
	return validateRange("DelaySeconds", delay, 0, 900)
}

func (s *service) receiveMessage(request *http.Request, input receiveInput) (receiveOutput, *apiError) {
	name, failure := queueName(input.queueInput)
	if failure != nil {
		return receiveOutput{}, failure
	}
	if failure := validateRange("MaxNumberOfMessages", input.MaxNumberOfMessages, 1, 10); failure != nil {
		return receiveOutput{}, failure
	}
	if failure := validateRange("WaitTimeSeconds", input.WaitTimeSeconds, 0, 20); failure != nil {
		return receiveOutput{}, failure
	}
	if failure := validateRange("VisibilityTimeout", input.VisibilityTimeout, 0, 43200); failure != nil {
		return receiveOutput{}, failure
	}
	count := 1
	if input.MaxNumberOfMessages != nil {
		count = *input.MaxNumberOfMessages
	}
	attributes := slices.Concat(input.MessageSystemAttributeNames, input.AttributeNames)
	messages, failure := s.store.receive(request.Context(), name, count, input.VisibilityTimeout, input.WaitTimeSeconds, attributes, input.MessageAttributeNames, input.ReceiveRequestAttemptID)
	return receiveOutput{Messages: messages}, failure
}

type receiveOutput struct {
	Messages []delivery `json:",omitempty"`
}

func (s *service) deleteMessage(_ *http.Request, input receiptInput) (empty, *apiError) {
	name, failure := queueName(input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if input.ReceiptHandle == "" {
		return empty{}, missingParameter("ReceiptHandle")
	}
	return empty{}, s.store.deleteMessage(name, input.ReceiptHandle)
}

func (s *service) changeMessageVisibility(_ *http.Request, input changeVisibilityInput) (empty, *apiError) {
	name, failure := queueName(input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if input.ReceiptHandle == "" {
		return empty{}, missingParameter("ReceiptHandle")
	}
	if input.VisibilityTimeout == nil {
		return empty{}, missingParameter("VisibilityTimeout")
	}
	if failure := validateRange("VisibilityTimeout", input.VisibilityTimeout, 0, 43200); failure != nil {
		return empty{}, failure
	}
	return empty{}, s.store.changeVisibility(name, input.ReceiptHandle, durationOrDefault(input.VisibilityTimeout, 0))
}

func queueName(input queueInput) (string, *apiError) {
	if input.QueueURL == "" {
		return "", missingParameter("QueueUrl")
	}
	parsed, err := url.Parse(input.QueueURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", missingQueue
	}
	path, ok := strings.CutPrefix(parsed.Path, "/"+accountID+"/")
	if !ok || !validQueueName(path) {
		return "", missingQueue
	}
	return path, nil
}

func missingParameter(name string) *apiError {
	return &apiError{"MissingParameter", "The request must contain the parameter " + name + ".", 400}
}

func validateRange(name string, value *int, minimum, maximum int) *apiError {
	if value == nil || (*value >= minimum && *value <= maximum) {
		return nil
	}
	reason := fmt.Sprintf("Must be >= %d and <= %d, if provided.", minimum, maximum)
	if name == "MaxNumberOfMessages" {
		reason = "Must be between 1 and 10, if provided."
	}
	return &apiError{"InvalidParameterValue", fmt.Sprintf("Value %d for parameter %s is invalid. Reason: %s", *value, name, reason), 400}
}

func validMessageCharacter(value rune) bool {
	return value == '\t' || value == '\n' || value == '\r' ||
		value >= 0x20 && value <= 0xD7FF ||
		value >= 0xE000 && value <= 0xFFFD ||
		value >= 0x10000 && value <= 0x10FFFF
}
