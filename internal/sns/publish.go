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
	if failure := checkTopicARN(region, arn); failure != nil {
		return publishResult{}, failure
	}
	message, subject := form.Get("Message"), form.Get("Subject")
	if message == "" || !utf8.ValidString(message) {
		return publishResult{}, invalid("Message")
	}
	if form.Has("Subject") && (strings.TrimSpace(subject) == "" || !utf8.ValidString(subject) || utf8.RuneCountInString(subject) > 100 ||
		strings.ContainsFunc(subject, func(r rune) bool { return r != '\t' && unicode.IsControl(r) })) {
		return publishResult{}, invalid("Subject")
	}
	attributes, envelopeAttributes, size, failure := publishAttributes(form)
	if failure != nil {
		return publishResult{}, failure
	}
	if len(message)+size > 256*1024 {
		return publishResult{}, invalid("Message too long")
	}
	body, failure := protocolMessage(message, form.Get("MessageStructure"))
	if failure != nil {
		return publishResult{}, failure
	}
	group, deduplicationID := form.Get("MessageGroupId"), form.Get("MessageDeduplicationId")
	if group != "" && !validFIFOToken(group) {
		return publishResult{}, invalid("MessageGroupId")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.topics[arn]
	if t == nil {
		return publishResult{}, topicNotFound
	}
	if t.accessPolicy.Evaluate(policy.Request{
		PrincipalARN: localSNSUserARN, PrincipalAccount: localAccountID,
		Action: "sns:Publish", Resource: arn,
	}) == policy.Deny {
		return publishResult{}, &apiError{"AuthorizationError", "The topic policy denies sns:Publish.", 403}
	}
	fifo := t.attributes["FifoTopic"] == "true"
	var key deduplicationKey
	now := time.Now()
	if fifo {
		var failure *apiError
		key, failure = t.fifoKey(message, group, deduplicationID)
		if failure != nil {
			return publishResult{}, failure
		}
		for key, record := range t.deduplication {
			if !now.Before(record.expiry) {
				delete(t.deduplication, key)
			}
		}
		if saved, duplicate := t.deduplication[key]; duplicate {
			return saved.result, nil
		}
	} else if form.Has("MessageDeduplicationId") {
		return publishResult{}, invalid("MessageDeduplicationId Reason: The request includes MessageDeduplicationId parameter that is not valid for this topic type")
	}

	result := publishResult{MessageID: uuid()}
	var base sqs.Notification
	base.TopicARN = arn
	if group != "" {
		base.GroupID = &group
	}
	if fifo {
		// SNS has already deduplicated this publish. Give each accepted publish a distinct ID
		// at SQS, even when the FIFO topic and queue have different deduplication scopes.
		base.DeduplicationID = &result.MessageID
	}
	envelope := notificationEnvelope{
		Type: "Notification", MessageID: result.MessageID, TopicARN: arn, Subject: subject,
		Message: body, Timestamp: now.UTC().Format("2006-01-02T15:04:05.000Z"), MessageAttributes: envelopeAttributes,
	}
	var deliveries []sqs.Notification
	for _, sub := range t.subs {
		if !sub.filter.matches(envelopeAttributes) {
			continue
		}
		delivery := base
		delivery.QueueARN = sub.Endpoint
		if sub.attributes["RawMessageDelivery"] == "true" {
			if len(attributes) > 10 {
				continue
			}
			delivery.Body, delivery.Attributes = body, attributes
		} else {
			if !fifo && envelope.Signature == "" {
				version := t.attributes["SignatureVersion"]
				if version == "" {
					version = "1"
				}
				if err := s.sign(&envelope, version, origin); err != nil {
					return publishResult{}, &apiError{"InternalError", "Unable to sign the notification.", 500}
				}
			}
			envelope.UnsubscribeURL = origin + "/?Action=Unsubscribe&SubscriptionArn=" + sub.ARN
			delivery.Body = envelope.body()
		}
		deliveries = append(deliveries, delivery)
	}
	if fifo {
		t.sequence++
		result.SequenceNumber = fmt.Sprintf("%039d", t.sequence)
		t.deduplication[key] = deduplicationRecord{result, now.Add(5 * time.Minute)}
	}
	for _, delivery := range deliveries {
		s.deliver(delivery)
	}
	return result, nil
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
		return deduplicationKey{}, invalid("MessageDeduplicationId")
	}
	key := deduplicationKey{id: id}
	if t.attributes["FifoThroughputScope"] == "MessageGroup" {
		key.group = group
	}
	return key, nil
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
