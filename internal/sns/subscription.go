package sns

import (
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/romycode/ginistack/internal/awshttp"
	"github.com/romycode/ginistack/internal/policy"
)

type subscription struct {
	ARN         string `xml:"SubscriptionArn"`
	TopicARN    string `xml:"TopicArn"`
	Owner       string
	Protocol    string
	Endpoint    string
	attributes  map[string]string
	filter      filterPolicy
	filterScope string
}

type subscriptionARN struct {
	ARN string `xml:"SubscriptionArn"`
}

func (s *service) subscribe(region string, form url.Values) (subscriptionARN, *apiError) {
	arn := form.Get("TopicArn")
	if failure := checkTopicARN(region, arn); failure != nil {
		return subscriptionARN{}, failure
	}
	t := s.topics[arn]
	if t == nil {
		return subscriptionARN{}, topicNotFound
	}
	protocol := form.Get("Protocol")
	if failure := t.authorize(arn, "SNS:Subscribe", map[string]string{
		"sns:Protocol": protocol, "sns:Endpoint": form.Get("Endpoint"), "aws:SourceOwner": localAccountID,
	}); failure != nil {
		return subscriptionARN{}, failure
	}
	if protocol != "sqs" {
		switch protocol {
		case "http", "https", "email", "email-json", "sms", "application", "lambda", "firehose":
		default:
			return subscriptionARN{}, invalid("Amazon SNS does not support this protocol string: " + protocol)
		}
		return subscriptionARN{}, &apiError{"NotImplemented", "Ginistack supports SQS subscriptions.", 501}
	}
	endpoint := form.Get("Endpoint")
	parts := strings.SplitN(endpoint, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "sqs" || !policy.IsAccountID(parts[4]) {
		return subscriptionARN{}, invalid("SQS endpoint ARN")
	}
	base, fifoQueue := strings.CutSuffix(parts[5], ".fifo")
	if !topicName.MatchString(base) || parts[3] == "" || len(parts[5]) > 80 {
		return subscriptionARN{}, invalid("SQS endpoint ARN")
	}
	if fifoQueue && t.attributes["FifoTopic"] != "true" {
		return subscriptionARN{}, invalid("Invalid parameter: Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics")
	}
	requested, failure := queryMap(form, "Attributes.entry", "key", "value")
	if failure != nil {
		return subscriptionARN{}, failure
	}
	candidate := &subscription{
		attributes:  map[string]string{"RawMessageDelivery": "false"},
		filterScope: filterMessageAttributes,
	}
	if scope, present := requested["FilterPolicyScope"]; present {
		if failure := candidate.setAttribute("FilterPolicyScope", scope); failure != nil {
			return subscriptionARN{}, failure
		}
	}
	for name, value := range requested {
		if name == "FilterPolicyScope" {
			continue
		}
		if failure := candidate.setAttribute(name, value); failure != nil {
			if name == "FilterPolicy" && failure != attributeFilterNested {
				return subscriptionARN{}, invalid("Attributes Reason: " + strings.TrimPrefix(failure.Message, "Invalid parameter: "))
			}
			return subscriptionARN{}, failure
		}
	}
	for _, existing := range t.subs {
		if existing.Endpoint == endpoint {
			for name := range requested {
				if name == "FilterPolicyScope" && candidate.filterScope != existing.filterScope ||
					name != "FilterPolicyScope" && candidate.attributes[name] != existing.attributes[name] {
					return subscriptionARN{}, invalid("Attributes Reason: Subscription already exists with different attributes")
				}
			}
			return subscriptionARN{existing.ARN}, nil
		}
	}
	candidate.ARN = arn + ":" + uuid()
	candidate.TopicARN = arn
	candidate.Owner = awshttp.AccountID
	candidate.Protocol = "sqs"
	candidate.Endpoint = endpoint
	t.subs[candidate.ARN] = candidate
	s.subs[candidate.ARN] = candidate
	return subscriptionARN{candidate.ARN}, nil
}

func (sub *subscription) setAttribute(name, value string) *apiError {
	switch name {
	case "RawMessageDelivery":
		normalized := strings.ToLower(value)
		if normalized != "true" && normalized != "false" {
			return invalid("RawMessageDelivery: Invalid value [" + value + "]. Must be true or false.")
		}
		sub.attributes[name] = normalized
	case "FilterPolicy":
		filter, normalized, failure := parseFilterPolicy(value, sub.filterScope)
		if failure != nil {
			return failure
		}
		sub.filter = filter
		if normalized == "" {
			delete(sub.attributes, name)
		} else {
			sub.attributes[name] = normalized
		}
	case "FilterPolicyScope":
		if value != filterMessageAttributes && value != filterMessageBody {
			return invalid("FilterPolicyScope: Invalid value [" + value + "]. Please use either MessageBody or MessageAttributes")
		}
		if value == filterMessageAttributes && sub.filter.nested {
			return attributeFilterNested
		}
		sub.filterScope = value
	default:
		return invalid("AttributeName")
	}
	return nil
}

func (s *service) subscriptionOperation(region, action string, form url.Values) (any, *apiError) {
	arn := form.Get("SubscriptionArn")
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 6 {
		return nil, invalid(fmt.Sprintf("SubscriptionArn Reason: An ARN must have at least 6 elements, not %d", len(parts)))
	}
	if len(parts) != 7 || parts[6] == "" || !strings.HasPrefix(arn, topicPrefix(region)) {
		return nil, invalid("SubscriptionArn")
	}
	sub := s.subs[arn]
	if action == "Unsubscribe" {
		if sub != nil {
			delete(s.topics[sub.TopicARN].subs, arn)
			delete(s.subs, arn)
		}
		return nil, nil
	}
	if sub == nil {
		return nil, &apiError{"NotFound", "Subscription does not exist", 404}
	}
	if action == "SetSubscriptionAttributes" {
		return nil, sub.setAttribute(form.Get("AttributeName"), form.Get("AttributeValue"))
	}
	attributes := maps.Clone(sub.attributes)
	if sub.filter.match != nil {
		attributes["FilterPolicyScope"] = sub.filterScope
	}
	attributes["SubscriptionArn"] = sub.ARN
	attributes["TopicArn"] = sub.TopicARN
	attributes["Endpoint"] = sub.Endpoint
	attributes["Protocol"] = sub.Protocol
	attributes["Owner"] = sub.Owner
	attributes["PendingConfirmation"] = "false"
	attributes["ConfirmationWasAuthenticated"] = "true"
	return attributesResult{listEntries(attributes)}, nil
}

func (s *service) listSubscriptions(region, action string, form url.Values) (any, *apiError) {
	prefix := topicPrefix(region)
	subs := s.subs
	scope := "subscriptions:" + prefix
	if action == "ListSubscriptionsByTopic" {
		arn := form.Get("TopicArn")
		if failure := checkTopicARN(region, arn); failure != nil {
			return nil, failure
		}
		t := s.topics[arn]
		if t == nil {
			return nil, topicNotFound
		}
		subs = t.subs
		scope = "subscriptions:" + arn
	}
	after, failure := s.cursorStart(scope, form.Get("NextToken"))
	if failure != nil {
		return nil, failure
	}
	var arns []string
	for arn := range subs {
		if arn > after && strings.HasPrefix(arn, prefix) {
			arns = append(arns, arn)
		}
	}
	slices.Sort(arns)
	arns, next := s.page(scope, arns)
	result := struct {
		Subscriptions []subscription `xml:"Subscriptions>member"`
		NextToken     string         `xml:",omitempty"`
	}{NextToken: next}
	for _, arn := range arns {
		result.Subscriptions = append(result.Subscriptions, *subs[arn])
	}
	return result, nil
}
