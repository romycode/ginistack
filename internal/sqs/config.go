package sqs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type queueConfig struct {
	visibility      time.Duration
	wait            time.Duration
	delay           time.Duration
	retention       time.Duration
	maximumSize     int
	fifo            bool
	contentDedup    bool
	dedupScope      string
	throughputLimit string
	redrive         redrivePolicy
	redriveAllow    redriveAllow
}

type redrivePolicy struct {
	TargetARN string `json:"deadLetterTargetArn"`
	Count     int    `json:"maxReceiveCount"`
}

func validQueueName(name string) bool {
	base, fifo := strings.CutSuffix(name, ".fifo")
	return (!fifo || len(name) <= 80) && queueNamePattern.MatchString(base)
}

func newQueueConfig(name string, attributes map[string]string) (queueConfig, *apiError) {
	config := queueConfig{visibility: 30 * time.Second, retention: 4 * 24 * time.Hour, maximumSize: maxMessageSize, dedupScope: "queue", throughputLimit: "perQueue"}
	return config.withAttributes(name, attributes, true)
}

func (c queueConfig) withAttributes(name string, attributes map[string]string, creating bool) (queueConfig, *apiError) {
	for key, value := range attributes {
		if !creating && ((key == "ContentBasedDeduplication" && !c.fifo) || key == "FifoQueue") {
			return c, unknownQueueAttribute(key)
		}
		var err error
		switch key {
		case "VisibilityTimeout", "ReceiveMessageWaitTimeSeconds":
			maximum := 43200
			if key == "ReceiveMessageWaitTimeSeconds" {
				maximum = 20
			}
			seconds, conversion := strconv.Atoi(value)
			if conversion != nil || seconds < 0 || seconds > maximum {
				return c, invalidQueueAttribute(key)
			}
			if key == "VisibilityTimeout" {
				c.visibility = time.Duration(seconds) * time.Second
			} else {
				c.wait = time.Duration(seconds) * time.Second
			}
		case "DelaySeconds", "MessageRetentionPeriod":
			seconds, conversion := strconv.Atoi(value)
			if conversion != nil {
				return c, invalidQueueAttribute(key)
			}
			if key == "DelaySeconds" {
				if seconds < 0 || seconds > 900 {
					return c, invalidQueueAttribute(key)
				}
				c.delay = time.Duration(seconds) * time.Second
			} else {
				if seconds < 60 || seconds > 1209600 {
					return c, invalidQueueAttribute(key)
				}
				c.retention = time.Duration(seconds) * time.Second
			}
		case "MaximumMessageSize":
			var size int
			size, err = strconv.Atoi(value)
			if err != nil || size < 1024 || size > maxMessageSize {
				return c, invalidQueueAttribute(key)
			}
			c.maximumSize = size
		case "FifoQueue":

			c.fifo, err = strconv.ParseBool(value)
		case "ContentBasedDeduplication":
			c.contentDedup, err = strconv.ParseBool(value)
		case "DeduplicationScope":
			if value != "queue" && value != "messageGroup" {
				return c, invalidQueueAttribute(key)
			}
			c.dedupScope = value
		case "FifoThroughputLimit":
			if value != "perQueue" && value != "perMessageGroupId" {
				return c, invalidQueueAttribute(key)
			}
			c.throughputLimit = value
		case "RedriveAllowPolicy":
			policy, failure := parseRedriveAllow(value)
			if failure != nil {
				return c, failure
			}
			c.redriveAllow = policy
		case "RedrivePolicy":
			c.redrive = redrivePolicy{}
			if value != "" {
				var document struct {
					TargetARN string          `json:"deadLetterTargetArn"`
					Count     json.RawMessage `json:"maxReceiveCount"`
				}
				if json.Unmarshal([]byte(value), &document) != nil || document.TargetARN == "" {
					return c, invalidQueueAttribute(key)
				}
				if len(document.Count) == 0 {
					return c, invalidPolicy(key, value, "Redrive policy does not contain mandatory attribute: maxReceiveCount.")
				}
				rawCount := strings.Trim(string(document.Count), "\"")
				count, conversion := strconv.Atoi(rawCount)
				if conversion != nil || count < 1 || count > 1000 {
					return c, invalidPolicy(key, value, "Invalid value for maxReceiveCount: "+rawCount+", valid values are from 1 to 1000 both inclusive.")
				}
				c.redrive = redrivePolicy{TargetARN: document.TargetARN, Count: count}
			}
		default:
			if knownQueueAttribute(key) {
				return c, notImplemented
			}
			return c, unknownQueueAttribute(key)
		}
		if err != nil {
			return c, invalidQueueAttribute(key)
		}
	}
	if c.fifo && !strings.HasSuffix(name, ".fifo") {
		return c, &apiError{"InvalidParameterValue", "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length.", 400}
	}
	if !c.fifo && strings.HasSuffix(name, ".fifo") {
		return c, &apiError{"InvalidParameterValue", "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length", 400}
	}
	if c.throughputLimit == "perMessageGroupId" && c.dedupScope != "messageGroup" {
		return c, &apiError{"InvalidAttributeValue", "Invalid value for the parameter FifoThroughputLimit. Reason: To set FifoThroughputLimit to perMessageGroupId, the DeduplicationScope must be messageGroup.", 400}
	}
	if !c.fifo {
		for _, key := range []string{"ContentBasedDeduplication", "DeduplicationScope", "FifoThroughputLimit"} {
			if _, present := attributes[key]; present {
				return c, invalidQueueAttribute(key)
			}
		}
	}
	return c, nil
}

func (c queueConfig) changedAttribute(next queueConfig) string {
	switch {
	case c.visibility != next.visibility:
		return "VisibilityTimeout"
	case c.wait != next.wait:
		return "ReceiveMessageWaitTimeSeconds"
	case c.delay != next.delay:
		return "DelaySeconds"
	case c.retention != next.retention:
		return "MessageRetentionPeriod"
	case c.maximumSize != next.maximumSize:
		return "MaximumMessageSize"
	case c.fifo != next.fifo:
		return "FifoQueue"
	case c.contentDedup != next.contentDedup:
		return "ContentBasedDeduplication"
	case c.dedupScope != next.dedupScope:
		return "DeduplicationScope"
	case c.throughputLimit != next.throughputLimit:
		return "FifoThroughputLimit"
	case c.redrive != next.redrive:
		return "RedrivePolicy"
	case c.redriveAllow != next.redriveAllow:
		return "RedriveAllowPolicy"
	default:
		return ""
	}
}

func knownQueueAttribute(key string) bool {
	switch key {
	case "Policy", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds", "SqsManagedSseEnabled", "CreatedTimestamp", "LastModifiedTimestamp":
		return true
	}
	return false
}

func unknownQueueAttribute(key string) *apiError {
	return &apiError{"InvalidAttributeName", "Unknown Attribute " + key + ".", 400}
}

func invalidQueueAttribute(key string) *apiError {
	return &apiError{"InvalidAttributeValue", "Invalid value for the parameter " + key + ".", 400}
}

func requestRegion(request *http.Request) string {
	_, credentials, present := strings.Cut(request.Header.Get("Authorization"), "Credential=")
	if present {
		parts := strings.SplitN(credentials, "/", 5)
		if len(parts) == 5 && queueNamePattern.MatchString(parts[2]) {
			return parts[2]
		}
	}
	return "us-east-1"
}

func (s *store) configure(name queueKey, attributes map[string]string) *apiError {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return missingQueue
	}
	config, failure := q.queueConfig.withAttributes(name.name, attributes, false)
	if failure != nil {
		return failure
	}
	if failure := s.validateRedrive(q.arn, config, attributes["RedrivePolicy"], attributes["RedriveAllowPolicy"]); failure != nil {
		return failure
	}
	if q.fifo && config.delay != q.delay {
		for _, message := range q.messages {
			if message.receiveCount == 0 {
				message.visibleAt = message.sentAt.Add(config.delay)
			}
		}
	}
	q.queueConfig = config
	q.signal()
	return nil
}

func (s *store) targetQueue(arn string) *queue {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sqs" || parts[4] != accountID {
		return nil
	}
	q := s.queues[queueKey{region: parts[3], name: parts[5]}]
	if q != nil && q.arn == arn {
		return q
	}
	return nil
}

func (s *store) validateRedrive(sourceARN string, config queueConfig, raw, rawAllow string) *apiError {
	for _, source := range strings.Fields(config.redriveAllow.sources) {
		if !sameRegion(source, sourceARN) {
			return invalidPolicy("RedriveAllowPolicy", rawAllow, "Amazon SQS can't create the redrive allow policy. All source queue ARNs must be in the same region as the current SQS queue.")
		}
	}
	if config.redrive.TargetARN == "" {
		return nil
	}
	sourceParts := strings.SplitN(sourceARN, ":", 6)
	targetParts := strings.SplitN(config.redrive.TargetARN, ":", 6)
	if len(targetParts) == 6 && len(sourceParts) == 6 && targetParts[3] != sourceParts[3] {
		return &apiError{"InvalidParameterValue", "Value " + raw + " for parameter RedrivePolicy is invalid. Reason: Dead-letter target must be in same region as the source.", 400}
	}
	target := s.targetQueue(config.redrive.TargetARN)
	if target == nil {
		return invalidPolicy("RedrivePolicy", raw, "Dead letter target does not exist.")
	}
	if target.fifo != config.fifo {
		return invalidPolicy("RedrivePolicy", raw, "Dead-letter queue must be same type of queue as the source..")
	}
	if target.arn == sourceARN {
		return invalidQueueAttribute("RedrivePolicy")
	}
	if !target.redriveAllow.permits(sourceARN) {
		canonical := fmt.Sprintf("RedrivePolicy(maxReceiveCount=%d, deadLetterTargetArn=%s)", config.redrive.Count, config.redrive.TargetARN)
		return invalidPolicy("RedrivePolicy", canonical, "The value for the dead-letter queue's RedriveAllowPolicy attribute prevents you from completing this request.")
	}
	return nil
}
