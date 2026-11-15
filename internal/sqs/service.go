package sqs

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/romycode/ginistack/internal/awshttp"
)

const accountID = awshttp.AccountID
const localUserID = "AIDAGINISTACKLOCAL0000"
const localUserARN = "arn:aws:iam::" + accountID + ":user/ginistack-local"

var queueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

type service struct {
	store     store
	cursorKey []byte
}

type empty struct{}

type createInput struct {
	QueueName  string
	Attributes map[string]string
	Tags       map[string]string
}

type queueKey struct {
	region string
	name   string
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
		message := "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length"
		if input.Attributes["FifoQueue"] == "true" {
			message = "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length."
		}
		return queueInput{}, &apiError{"InvalidParameterValue", message, 400}
	}
	key := queueKey{region: awshttp.Region(request), name: input.QueueName}
	arn := "arn:aws:sqs:" + key.region + ":" + accountID + ":" + key.name
	if failure := s.store.create(key, arn, input.Attributes, input.Tags, request.TLS != nil); failure != nil {
		return queueInput{}, failure
	}
	return queueAddress(request, key), nil
}

func queueAddress(request *http.Request, key queueKey) queueInput {
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	queueURL := url.URL{Scheme: scheme, Host: request.Host, Path: "/" + key.region + "/" + accountID + "/" + key.name}
	return queueInput{QueueURL: queueURL.String()}
}

type getQueueURLInput struct {
	QueueName              string
	QueueOwnerAWSAccountID string `json:"QueueOwnerAWSAccountId"`
}

func (s *service) getQueueURL(request *http.Request, input getQueueURLInput) (queueInput, *apiError) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	key := queueKey{region: awshttp.Region(request), name: input.QueueName}
	if s.store.queues[key] == nil || (input.QueueOwnerAWSAccountID != "" && input.QueueOwnerAWSAccountID != accountID) {
		return queueInput{}, missingQueue
	}
	return queueAddress(request, key), nil
}

func (s *service) deleteQueue(request *http.Request, input queueInput) (empty, *apiError) {
	name, failure := queueName(request, input)
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

func (s *service) getQueueAttributes(request *http.Request, input getAttributesInput) (getAttributesOutput, *apiError) {
	name, failure := queueName(request, input.queueInput)
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

func (s *service) setQueueAttributes(request *http.Request, input setAttributesInput) (empty, *apiError) {
	name, failure := queueName(request, input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if len(input.Attributes) == 0 {
		return empty{}, missingParameter("Attribute.Name")
	}
	return empty{}, s.store.configure(name, input.Attributes)
}

func (s *service) listQueueTags(request *http.Request, input queueInput) (listTagsOutput, *apiError) {
	name, failure := queueName(request, input)
	if failure != nil {
		return listTagsOutput{}, failure
	}
	tags, failure := s.store.queueTags(name)
	return listTagsOutput{Tags: tags}, failure
}

func (s *service) sendMessage(request *http.Request, input sendInput) (sendOutput, *apiError) {
	name, failure := queueName(request, input.queueInput)
	if failure != nil {
		return sendOutput{}, failure
	}
	return s.store.send(name, input.messageInput, request.TLS != nil)
}

const maxMessageSize = 1 << 20

func validateMessage(body string, delay *int) *apiError {
	if body == "" {
		return missingParameter("MessageBody")
	}
	for _, character := range body {
		if !validMessageCharacter(character) {
			return &apiError{"InvalidMessageContents", fmt.Sprintf("Invalid binary character '#x%X' was found in the message body, the set of allowed characters is #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF].", character), 400}
		}
	}
	if delay != nil && (*delay < 0 || *delay > 900) {
		return &apiError{"InvalidParameterValue", fmt.Sprintf("Value %d for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and <= 900.", *delay), 400}
	}
	return nil
}

func (s *service) receiveMessage(request *http.Request, input receiveInput) (receiveOutput, *apiError) {
	name, failure := queueName(request, input.queueInput)
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

func (s *service) deleteMessage(request *http.Request, input receiptInput) (empty, *apiError) {
	name, failure := queueName(request, input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if input.ReceiptHandle == "" {
		return empty{}, missingParameter("ReceiptHandle")
	}
	return empty{}, s.store.deleteMessage(name, input.ReceiptHandle)
}

func (s *service) changeMessageVisibility(request *http.Request, input changeVisibilityInput) (empty, *apiError) {
	name, failure := queueName(request, input.queueInput)
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

func queueName(request *http.Request, input queueInput) (queueKey, *apiError) {
	if input.QueueURL == "" {
		return queueKey{}, missingParameter("QueueUrl")
	}
	parsed, err := url.Parse(input.QueueURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return queueKey{}, missingQueue
	}
	path := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	region := awshttp.Region(request)
	if len(path) == 3 {
		if path[0] != region {
			return queueKey{}, missingQueue
		}
		path = path[1:]
	}
	if len(path) != 2 || path[0] != accountID || !validQueueName(path[1]) {
		return queueKey{}, missingQueue
	}
	return queueKey{region: region, name: path[1]}, nil
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
	if name == "VisibilityTimeout" {
		reason = "VisibilityTimeout must be an integer between 0 and 43200."
	}
	return &apiError{"InvalidParameterValue", fmt.Sprintf("Value %d for parameter %s is invalid. Reason: %s", *value, name, reason), 400}
}

func validMessageCharacter(value rune) bool {
	return value == '\t' || value == '\n' || value == '\r' ||
		value >= 0x20 && value <= 0xD7FF ||
		value >= 0xE000 && value <= 0xFFFD ||
		value >= 0x10000 && value <= 0x10FFFF
}
