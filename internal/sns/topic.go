package sns

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/romycode/ginistack/internal/awshttp"
	"github.com/romycode/ginistack/internal/policy"
)

var topicName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

const (
	defaultMessageSize = 256 * 1024
	minimumMessageSize = 1024
	maximumMessageSize = 1024 * 1024
)

type topic struct {
	attributes    map[string]string
	tags          map[string]string
	subs          map[string]*subscription
	accessPolicy  *policy.Document
	sequence      uint64
	deduplication map[deduplicationKey]deduplicationRecord
}

type topicARN struct {
	ARN string `xml:"TopicArn"`
}

func (s *service) createTopic(region string, form url.Values) (topicARN, *apiError) {
	name := form.Get("Name")
	base, fifo := strings.CutSuffix(name, ".fifo")
	if len(name) > 256 || !topicName.MatchString(base) {
		return topicARN{}, invalid("Topic Name")
	}
	requested, failure := queryMap(form, "Attributes.entry", "key", "value")
	if failure != nil {
		return topicARN{}, failure
	}
	if value, present := requested["FifoTopic"]; (fifo && value != "true") || (present && !fifo && value != "false") {
		return topicARN{}, invalid("Topic Name")
	}
	tags, failure := queryTags(form)
	if failure != nil {
		return topicARN{}, failure
	}
	if failure := validateTags(tags); failure != nil {
		return topicARN{}, failure
	}
	attributes := map[string]string{"DisplayName": ""}
	if fifo {
		attributes["FifoTopic"] = "true"
		attributes["FifoThroughputScope"] = "Topic"
		attributes["ContentBasedDeduplication"] = "false"
	}
	candidate := &topic{
		attributes: attributes, tags: tags, subs: make(map[string]*subscription),
	}
	if fifo {
		candidate.deduplication = make(map[deduplicationKey]deduplicationRecord)
	}
	for key, value := range requested {
		if key == "FifoTopic" {
			continue
		}
		if failure := candidate.setAttribute(key, value); failure != nil {
			return topicARN{}, failure
		}
	}
	result := topicARN{topicPrefix(region) + name}
	if existing := s.topics[result.ARN]; existing != nil {
		for key := range requested {
			if key != "FifoTopic" && existing.attributes[key] != attributes[key] {
				return topicARN{}, invalid("Attributes Reason: Topic already exists with different attributes")
			}
		}
		if len(tags) != 0 && !maps.Equal(tags, existing.tags) {
			return topicARN{}, invalid("Tags Reason: Topic already exists with different tags")
		}
		return result, nil
	}
	s.topics[result.ARN] = candidate
	return result, nil
}

func topicPrefix(region string) string {
	return "arn:aws:sns:" + region + ":" + awshttp.AccountID + ":"
}

func checkTopicARN(region, arn string) *apiError {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 {
		return invalid(fmt.Sprintf("TopicArn Reason: An ARN must have at least 6 elements, not %d", len(parts)))
	}
	name, _ := strings.CutSuffix(parts[5], ".fifo")
	if !strings.HasPrefix(arn, topicPrefix(region)) || !topicName.MatchString(name) {
		return invalid("TopicArn")
	}
	return nil
}

func (s *service) topicOperation(region, action string, form url.Values) (any, *apiError) {
	arn := form.Get("TopicArn")
	if failure := checkTopicARN(region, arn); failure != nil {
		return nil, failure
	}
	if action == "DeleteTopic" {
		if t := s.topics[arn]; t != nil {
			for sub := range t.subs {
				delete(s.subs, sub)
			}
		}
		delete(s.topics, arn)
		return nil, nil
	}
	t := s.topics[arn]
	if t == nil {
		return nil, topicNotFound
	}
	if action == "SetTopicAttributes" {
		return nil, t.setAttribute(form.Get("AttributeName"), form.Get("AttributeValue"))
	}
	attributes := maps.Clone(t.attributes)
	if t.accessPolicy == nil {
		attributes["Policy"] = encodeTopicPolicy(defaultTopicPolicy(arn))
	}
	attributes["Owner"] = awshttp.AccountID
	attributes["TopicArn"] = arn
	attributes["SubscriptionsConfirmed"] = strconv.Itoa(len(t.subs))
	attributes["SubscriptionsPending"] = "0"
	attributes["SubscriptionsDeleted"] = "0"
	return attributesResult{listEntries(attributes)}, nil
}

func (t *topic) setAttribute(name, value string) *apiError {
	attributes := t.attributes
	switch name {
	case "DisplayName":
		if !utf8.ValidString(value) || utf8.RuneCountInString(value) > 100 {
			return invalid("DisplayName")
		}
	case "MaximumMessageSize":
		size, err := strconv.Atoi(value)
		if err != nil || size < minimumMessageSize || size > maximumMessageSize {
			return invalid(fmt.Sprintf("MaximumMessageSize: %s is not an integer between %d and %d bytes", value, minimumMessageSize, maximumMessageSize))
		}
		value = strconv.Itoa(size)
	case "Policy":
		if value == "" {
			delete(attributes, name)
			t.accessPolicy = nil
			return nil
		}
		document, err := policy.Parse(value)
		if err != nil {
			return invalid("Policy Error: null")
		}
		t.accessPolicy = document
	case "SignatureVersion":
		if value != "1" && value != "2" {
			return invalid("SignatureVersion: Invalid value '[" + value + "]'. Must be 1 or 2.")
		}
	case "TracingConfig":
		if value != "Active" && value != "PassThrough" {
			return invalid("Invalid tracing config value: " + value)
		}
	case "FifoThroughputScope":
		if attributes["FifoTopic"] != "true" {
			return invalid("AttributeName")
		}
		if value != "Topic" && value != "MessageGroup" {
			return invalid("Invalid FifoThroughputScope: " + value)
		}
		if attributes[name] == "MessageGroup" && value == "Topic" {
			return invalid("Modifying FifoThroughputScope from MessageGroup to Topic is not supported")
		}
	case "ContentBasedDeduplication":
		if attributes["FifoTopic"] != "true" || (value != "true" && value != "false") {
			return invalid("ContentBasedDeduplication")
		}
	default:
		return invalid("AttributeName")
	}
	attributes[name] = value
	return nil
}

func (t *topic) messageSizeLimit() int {
	if size, _ := strconv.Atoi(t.attributes["MaximumMessageSize"]); size > 0 {
		return size
	}
	return defaultMessageSize
}

func (s *service) listTopics(region, token string) (any, *apiError) {
	prefix := topicPrefix(region)
	after, failure := s.cursorStart(prefix, token)
	if failure != nil {
		return nil, failure
	}
	var arns []string
	for arn := range s.topics {
		if arn > after && strings.HasPrefix(arn, prefix) {
			arns = append(arns, arn)
		}
	}
	slices.Sort(arns)
	arns, next := s.page(prefix, arns)
	result := struct {
		Topics    []topicARN `xml:"Topics>member"`
		NextToken string     `xml:",omitempty"`
	}{NextToken: next}
	for _, arn := range arns {
		result.Topics = append(result.Topics, topicARN{arn})
	}
	return result, nil
}

func (s *service) page(scope string, sorted []string) ([]string, string) {
	const pageSize = 100
	if len(sorted) <= pageSize {
		return sorted, ""
	}
	payload := []byte(scope + "\x00" + sorted[pageSize-1])
	mac := hmac.New(sha256.New, s.cursorKey[:])
	mac.Write(payload)
	return sorted[:pageSize], base64.RawURLEncoding.EncodeToString(append(mac.Sum(nil), payload...))
}

func (s *service) cursorStart(scope, token string) (string, *apiError) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) <= sha256.Size {
		return "", invalid("NextToken")
	}
	signature, payload := raw[:sha256.Size], raw[sha256.Size:]
	mac := hmac.New(sha256.New, s.cursorKey[:])
	mac.Write(payload)
	after, scopeOK := strings.CutPrefix(string(payload), scope+"\x00")
	if !scopeOK || !hmac.Equal(signature, mac.Sum(nil)) {
		return "", invalid("NextToken")
	}
	return after, nil
}

type resourceTag struct{ Key, Value string }

func queryTags(form url.Values) (map[string]string, *apiError) {
	for _, field := range slices.Sorted(maps.Keys(form)) {
		tail, tagged := strings.CutPrefix(field, "Tags.member.")
		index, keyField := strings.CutSuffix(tail, ".Key")
		n, err := strconv.Atoi(index)
		if !tagged || !keyField || err != nil || n < 1 {
			continue
		}
		if key := form.Get(field); utf8.RuneCountInString(key) > 128 {
			return nil, &apiError{"ValidationError", fmt.Sprintf("1 validation error detected: Value '%s' at 'tags.%d.member.key' failed to satisfy constraint: Member must have length less than or equal to 128", key, n), 400}
		}
	}
	return queryMap(form, "Tags.member", "Key", "Value")
}

func validateTags(tags map[string]string) *apiError {
	if len(tags) > 50 {
		return &apiError{"TagLimitExceeded", "Could not complete request: tag quota of per resource exceeded", 400}
	}
	for key, value := range tags {
		if strings.HasPrefix(strings.ToLower(key), "aws:") {
			return invalid("Tags Reason: Caller is an end user and not allowed to mutate system tags")
		}
		if key == "" || utf8.RuneCountInString(value) > 256 {
			return invalid("Tags")
		}
	}
	return nil
}

func (s *service) tagOperation(region, action string, form url.Values) (any, *apiError) {
	arn := form.Get("ResourceArn")
	if failure := checkTopicARN(region, arn); failure != nil {
		return nil, failure
	}
	t := s.topics[arn]
	if t == nil {
		return nil, &apiError{"ResourceNotFound", "Resource does not exist", 404}
	}
	switch action {
	case "ListTagsForResource":
		result := struct {
			Tags []resourceTag `xml:"Tags>member"`
		}{}
		for _, item := range listEntries(t.tags) {
			result.Tags = append(result.Tags, resourceTag{item.Key, item.Value})
		}
		return result, nil
	case "TagResource":
		change, failure := queryTags(form)
		if failure != nil {
			return nil, failure
		}
		tags := maps.Clone(t.tags)
		maps.Copy(tags, change)
		if failure := validateTags(tags); failure != nil {
			return nil, failure
		}
		t.tags = tags
	case "UntagResource":
		for param := range form {
			index, member := strings.CutPrefix(param, "TagKeys.member.")
			if n, err := strconv.Atoi(index); member && err == nil && n > 0 {
				delete(t.tags, form.Get(param))
			}
		}
	}
	return struct{}{}, nil
}
