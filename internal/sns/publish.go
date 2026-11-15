package sns

import (
	"bytes"
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/romycode/ginistack/internal/policy"
	"github.com/romycode/ginistack/internal/sqs"
)

type publishResult struct {
	MessageID      string `xml:"MessageId"`
	SequenceNumber string `xml:",omitempty"`
}

type deduplicationKey struct{ id, group string }

type deduplicationRecord struct {
	result publishResult
	expiry time.Time
}

type envelopeAttribute struct{ Type, Value string }

type notificationEnvelope struct {
	Type              string
	MessageID         string `json:"MessageId"`
	TopicARN          string `json:"TopicArn"`
	Subject           string `json:",omitempty"`
	Message           string
	Timestamp         string
	SignatureVersion  string `json:",omitempty"`
	Signature         string `json:",omitempty"`
	SigningCertURL    string `json:",omitempty"`
	UnsubscribeURL    string
	MessageAttributes map[string]envelopeAttribute `json:",omitempty"`
}

func (s *service) publish(region, origin string, form url.Values) (publishResult, *apiError) {
	arn, target := form.Get("TopicArn"), form.Get("TargetArn")
	if arn != "" && target != "" {
		return publishResult{}, invalid("TopicArn Reason: Both TopicArn and TargetArn specified. Use only one or the other.")
	}
	if arn == "" {
		arn = target
	}
	if arn == "" {
		return publishResult{}, invalid("TopicArn or TargetArn Reason: no value for required parameter")
	}
	if failure := checkTopicARN(region, arn); failure != nil {
		return publishResult{}, failure
	}
	input, failure := preparePublish(form)
	if failure != nil {
		return publishResult{}, failure
	}
	results, failure := s.publishMessages(arn, origin, []publishInput{input}, false)
	if failure != nil {
		return publishResult{}, failure
	}
	return results[0], nil
}

type publishInput struct {
	message, body, subject string
	group, deduplicationID string
	hasDeduplicationID     bool
	size                   int
	attributes             map[string]sqs.NotificationAttribute
	envelopeAttributes     map[string]envelopeAttribute
}

func preparePublish(form url.Values) (publishInput, *apiError) {
	message, subject := form.Get("Message"), form.Get("Subject")
	if message == "" {
		return publishInput{}, invalid("Empty message")
	}
	if !utf8.ValidString(message) {
		return publishInput{}, invalid("Message")
	}
	if form.Has("Subject") && (strings.TrimSpace(subject) == "" || !utf8.ValidString(subject) || utf8.RuneCountInString(subject) > 100 ||
		strings.ContainsFunc(subject, func(r rune) bool { return r != '\t' && unicode.IsControl(r) })) {
		return publishInput{}, invalid("Subject")
	}
	attributes, envelopeAttributes, size, failure := publishAttributes(form)
	if failure != nil {
		return publishInput{}, failure
	}
	body, failure := protocolMessage(message, form.Get("MessageStructure"))
	if failure != nil {
		return publishInput{}, failure
	}
	group, deduplicationID := form.Get("MessageGroupId"), form.Get("MessageDeduplicationId")
	if group != "" && !validFIFOToken(group) {
		return publishInput{}, invalidFIFOToken("MessageGroupId")
	}

	return publishInput{
		message: message, body: body, subject: subject,
		group: group, deduplicationID: deduplicationID, hasDeduplicationID: form.Has("MessageDeduplicationId"),
		size: len(message) + size, attributes: attributes, envelopeAttributes: envelopeAttributes,
	}, nil
}

func (s *service) publishMessages(arn, origin string, inputs []publishInput, batch bool) ([]publishResult, *apiError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topics[arn]
	if t == nil {
		return nil, topicNotFound
	}
	if t.accessPolicy.Evaluate(policy.Request{
		PrincipalARN: localSNSUserARN, PrincipalAccount: localAccountID,
		Action: "sns:Publish", Resource: arn,
	}) == policy.Deny {
		return nil, &apiError{"AuthorizationError", "The topic policy denies sns:Publish.", 403}
	}

	var size int
	for _, input := range inputs {
		size += input.size
	}
	if size > t.messageSizeLimit() {
		if batch {
			return nil, &apiError{"BatchRequestTooLong", "The length of all the messages put together is more than the limit.", 400}
		}
		return nil, invalid("Message too long")
	}

	fifo := t.attributes["FifoTopic"] == "true"
	now := time.Now()
	sequence := t.sequence
	staged := make(map[deduplicationKey]deduplicationRecord)
	results := make([]publishResult, 0, len(inputs))
	var deliveries []sqs.Notification
	for _, input := range inputs {
		var key deduplicationKey
		if fifo {
			var failure *apiError
			key, failure = t.fifoKey(input.message, input.group, input.deduplicationID)
			if failure != nil {
				return nil, failure
			}
			if saved, duplicate := staged[key]; duplicate {
				results = append(results, saved.result)
				continue
			}
			if saved, duplicate := t.deduplication[key]; duplicate && now.Before(saved.expiry) {
				results = append(results, saved.result)
				continue
			}
		} else if input.hasDeduplicationID {
			return nil, invalid("MessageDeduplicationId Reason: The request includes MessageDeduplicationId parameter that is not valid for this topic type")
		}

		result := publishResult{MessageID: uuid()}
		if fifo {
			sequence++
			result.SequenceNumber = fmt.Sprintf("%039d", sequence)
			staged[key] = deduplicationRecord{result, now.Add(5 * time.Minute)}
		}
		prepared, failure := s.prepareDeliveries(t, arn, origin, input, result.MessageID, fifo, now)
		if failure != nil {
			return nil, failure
		}
		results = append(results, result)
		deliveries = append(deliveries, prepared...)
	}
	if fifo {
		for key, record := range t.deduplication {
			if !now.Before(record.expiry) {
				delete(t.deduplication, key)
			}
		}
		maps.Copy(t.deduplication, staged)
		t.sequence = sequence
	}
	for _, delivery := range deliveries {
		s.deliver(delivery)
	}
	return results, nil
}

func (s *service) prepareDeliveries(t *topic, arn, origin string, input publishInput, messageID string, fifo bool, now time.Time) ([]sqs.Notification, *apiError) {
	var base sqs.Notification
	base.TopicARN = arn
	if input.group != "" {
		base.GroupID = &input.group
	}
	if fifo {
		// SNS has already deduplicated this publish. Give each accepted publish a distinct ID
		// at SQS, even when the FIFO topic and queue have different deduplication scopes.
		base.DeduplicationID = &messageID
	}
	envelope := notificationEnvelope{
		Type: "Notification", MessageID: messageID, TopicARN: arn, Subject: input.subject,
		Message: input.body, Timestamp: now.UTC().Format("2006-01-02T15:04:05.000Z"), MessageAttributes: input.envelopeAttributes,
	}
	var deliveries []sqs.Notification
	for _, sub := range t.subs {
		if !sub.filter.matches(input.envelopeAttributes) {
			continue
		}
		delivery := base
		delivery.QueueARN = sub.Endpoint
		if sub.attributes["RawMessageDelivery"] == "true" {
			if len(input.attributes) > 10 {
				continue
			}
			delivery.Body, delivery.Attributes = input.body, input.attributes
		} else {
			if !fifo && envelope.Signature == "" {
				version := t.attributes["SignatureVersion"]
				if version == "" {
					version = "1"
				}
				if err := s.sign(&envelope, version, origin); err != nil {
					return nil, &apiError{"InternalError", "Unable to sign the notification.", 500}
				}
			}
			envelope.UnsubscribeURL = origin + "/?Action=Unsubscribe&SubscriptionArn=" + sub.ARN
			delivery.Body = envelope.body()
		}
		deliveries = append(deliveries, delivery)
	}
	return deliveries, nil
}

func (n notificationEnvelope) body() string {
	var body strings.Builder
	var quoted bytes.Buffer
	encoder := json.NewEncoder(&quoted)
	encoder.SetEscapeHTML(false)
	quote := func(value string) string {
		quoted.Reset()
		_ = encoder.Encode(value)
		return strings.TrimSuffix(quoted.String(), "\n")
	}
	body.WriteString("{")
	writeField := func(name, value string) {
		if body.Len() > 1 {
			body.WriteByte(',')
		}
		body.WriteString("\n  " + quote(name) + " : " + quote(value))
	}
	writeField("Type", n.Type)
	writeField("MessageId", n.MessageID)
	writeField("TopicArn", n.TopicARN)
	if n.Subject != "" {
		writeField("Subject", n.Subject)
	}
	writeField("Message", n.Message)
	writeField("Timestamp", n.Timestamp)
	if n.Signature != "" {
		writeField("SignatureVersion", n.SignatureVersion)
		writeField("Signature", n.Signature)
		writeField("SigningCertURL", n.SigningCertURL)
	}
	writeField("UnsubscribeURL", n.UnsubscribeURL)
	if len(n.MessageAttributes) != 0 {
		names := slices.Sorted(maps.Keys(n.MessageAttributes))
		body.WriteString(",\n  \"MessageAttributes\" : {\n")
		for i, name := range names {
			if i != 0 {
				body.WriteString(",\n")
			}
			attribute := n.MessageAttributes[name]
			body.WriteString("    " + quote(name) + " : {\"Type\":" + quote(attribute.Type) + ",\"Value\":" + quote(attribute.Value) + "}")
		}
		body.WriteString("\n  }")
	}
	body.WriteString("\n}")
	return body.String()
}

func (t *topic) fifoKey(message, group, id string) (deduplicationKey, *apiError) {
	if group == "" {
		return deduplicationKey{}, invalid("The MessageGroupId parameter is required for FIFO topics")
	}
	if id == "" {
		if t.attributes["ContentBasedDeduplication"] != "true" {
			return deduplicationKey{}, invalid("The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")
		}
		sum := sha256.Sum256([]byte(message))
		id = hex.EncodeToString(sum[:])
	}
	if !validFIFOToken(id) {
		return deduplicationKey{}, invalidFIFOToken("MessageDeduplicationId")
	}
	key := deduplicationKey{id: id}
	if t.attributes["FifoThroughputScope"] == "MessageGroup" {
		key.group = group
	}
	return key, nil
}

func invalidFIFOToken(name string) *apiError {
	return invalid("The " + name + " parameter can only include alphanumeric and punctuation characters. 1 to 128 in length")
}

func validFIFOToken(value string) bool {
	return len(value) <= 128 && !strings.ContainsFunc(value, func(r rune) bool { return r < '!' || r > '~' })
}

func protocolMessage(message, structure string) (string, *apiError) {
	if structure == "" {
		return message, nil
	}
	if structure != "json" {
		return "", invalid("MessageStructure")
	}
	var values map[string]json.RawMessage
	if json.Unmarshal([]byte(message), &values) != nil {
		return "", invalid("Message Structure - JSON message body failed to parse")
	}
	var fallback *string
	if json.Unmarshal(values["default"], &fallback) != nil || fallback == nil {
		return "", invalid("Message Structure - No default entry in JSON message body")
	}
	var specific *string
	if json.Unmarshal(values["sqs"], &specific) == nil && specific != nil {
		return *specific, nil
	}
	return *fallback, nil
}

var attributeNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)

func publishAttributes(form url.Values) (map[string]sqs.NotificationAttribute, map[string]envelopeAttribute, int, *apiError) {
	values := make(map[string]url.Values)
	for path, items := range form {
		tail, present := strings.CutPrefix(path, "MessageAttributes.entry.")
		if !present {
			continue
		}
		index, field, found := strings.Cut(tail, ".")
		if !found {
			return nil, nil, 0, invalid("MessageAttributes")
		}
		if values[index] == nil {
			values[index] = make(url.Values)
		}
		values[index][field] = items
	}
	raw := make(map[string]sqs.NotificationAttribute, len(values))
	envelope := make(map[string]envelopeAttribute, len(values))
	var size int
	for _, fields := range values {
		name, dataType := fields.Get("Name"), fields.Get("Value.DataType")
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.") {
			return nil, nil, 0, &apiError{"ParameterValueInvalid", "Message attribute names starting with 'AWS.' or 'Amazon.' are reserved for use by Amazon.", 400}
		}
		if !attributeNamePattern.MatchString(name) || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
			return nil, nil, 0, &apiError{"ParameterValueInvalid", "The message attribute name '" + name + "' is invalid.", 400}
		}
		if _, duplicate := raw[name]; duplicate {
			return nil, nil, 0, invalid("MessageAttributes")
		}
		var attribute sqs.NotificationAttribute
		attribute.DataType = dataType
		value := fields.Get("Value.StringValue")
		baseType, _, _ := strings.Cut(dataType, ".")
		var valueSize int
		switch baseType {
		case "String", "Number":
			if value == "" || !utf8.ValidString(value) || fields.Has("Value.BinaryValue") {
				return nil, nil, 0, &apiError{"ParameterValueInvalid", "The message attribute '" + name + "' must contain non-empty message attribute value for message attribute type '" + dataType + "'.", 400}
			}
			if baseType == "Number" {
				if failure := validateNumberAttribute(name, value); failure != nil {
					return nil, nil, 0, failure
				}
			}
			attribute.StringValue = &value
			valueSize = len(value)
		case "Binary":
			value = fields.Get("Value.BinaryValue")
			decoded, err := base64.StdEncoding.DecodeString(value)
			if err != nil || len(decoded) == 0 || fields.Has("Value.StringValue") {
				return nil, nil, 0, invalid("MessageAttributes")
			}
			attribute.BinaryValue = decoded
			valueSize = len(decoded)
		default:
			return nil, nil, 0, invalid("MessageAttributes")
		}
		raw[name] = attribute
		envelope[name] = envelopeAttribute{dataType, value}
		size += len(name) + len(dataType) + valueSize
	}
	return raw, envelope, size, nil
}

var decimalAttributePattern = regexp.MustCompile(`^[+-]?([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$`)

func validateNumberAttribute(name, value string) *apiError {
	parts := decimalAttributePattern.FindStringSubmatch(value)
	if parts == nil || parts[1]+parts[2] == "" {
		return &apiError{"ParameterValueInvalid", "Could not cast message attribute '" + name + "' value to number.", 400}
	}
	digits := strings.TrimLeft(parts[1]+parts[2], "0")
	if digits == "" {
		return nil
	}
	label := fmt.Sprintf("Message attribute '%s' number value [%s]", name, value)
	if len(strings.TrimRight(digits, "0")) > 38 {
		return &apiError{"ParameterValueInvalid", label + " must have less than 38 precision digits.", 400}
	}
	var exponent int
	var err error
	if parts[3] != "" {
		exponent, err = strconv.Atoi(parts[3])
	}
	if err != nil || exponent < -2*len(value)-128 || exponent > 2*len(value)+126 {
		return &apiError{"ParameterValueInvalid", label + " scale must be 10^-128 and 10^126.", 400}
	}
	magnitude := len(digits) - len(parts[2]) + exponent - 1
	if magnitude < -128 || magnitude >= 126 {
		return &apiError{"ParameterValueInvalid", label + " scale must be 10^-128 and 10^126.", 400}
	}
	return nil
}

func (s *service) sign(envelope *notificationEnvelope, version, origin string) error {
	signing, err := s.signing()
	if err != nil {
		return err
	}
	text := "Message\n" + envelope.Message + "\nMessageId\n" + envelope.MessageID + "\n"
	if envelope.Subject != "" {
		text += "Subject\n" + envelope.Subject + "\n"
	}
	text += "Timestamp\n" + envelope.Timestamp + "\nTopicArn\n" + envelope.TopicARN + "\nType\n" + envelope.Type + "\n"
	var hash crypto.Hash
	var digest []byte
	if version == "2" {
		sum := sha256.Sum256([]byte(text))
		hash, digest = crypto.SHA256, sum[:]
	} else {
		sum := sha1.Sum([]byte(text))
		hash, digest = crypto.SHA1, sum[:]
	}
	signature, err := rsa.SignPKCS1v15(nil, signing.key, hash, digest)
	if err != nil {
		return err
	}
	envelope.SignatureVersion = version
	envelope.Signature = base64.StdEncoding.EncodeToString(signature)
	envelope.SigningCertURL = origin + SigningCertPath
	return nil
}
