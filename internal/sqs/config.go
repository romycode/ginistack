package sqs

import (
	"encoding/json"
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
	fifo            bool
	contentDedup    bool
	dedupScope      string
	throughputLimit string
	redrive         redrivePolicy
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
	config := queueConfig{visibility: 30 * time.Second, retention: 4 * 24 * time.Hour, dedupScope: "queue", throughputLimit: "perQueue"}
	return config.withAttributes(name, attributes, true)
}

func (c queueConfig) withAttributes(name string, attributes map[string]string, creating bool) (queueConfig, *apiError) {
	for key, value := range attributes {
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
		case "FifoQueue":
			if !creating {
				return c, invalidQueueAttribute(key)
			}
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
				count, conversion := strconv.Atoi(strings.Trim(string(document.Count), "\""))
				if conversion != nil || count < 1 || count > 1000 {
					return c, invalidQueueAttribute(key)
				}
				c.redrive = redrivePolicy{TargetARN: document.TargetARN, Count: count}
			}
		default:
			return c, notImplemented
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

func (s *store) configure(name string, attributes map[string]string) *apiError {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return missingQueue
	}
	config, failure := q.queueConfig.withAttributes(name, attributes, false)
	if failure != nil {
		return failure
	}
	if failure := s.validateRedrive(q.arn, config, attributes["RedrivePolicy"]); failure != nil {
		return failure
	}
	q.queueConfig = config
	q.signal()
	return nil
}

func (s *store) targetQueue(arn string) *queue {
	_, name, ok := strings.Cut(arn, ":"+accountID+":")
	if !ok {
		return nil
	}
	q := s.queues[name]
	if q != nil && q.arn == arn {
		return q
	}
	return nil
}

func (s *store) validateRedrive(sourceARN string, config queueConfig, raw string) *apiError {
	if config.redrive.TargetARN == "" {
		return nil
	}
	sourceParts := strings.SplitN(sourceARN, ":", 6)
	targetParts := strings.SplitN(config.redrive.TargetARN, ":", 6)
	if len(targetParts) == 6 && len(sourceParts) == 6 && targetParts[3] != sourceParts[3] {
		return &apiError{"InvalidParameterValue", "Value " + raw + " for parameter RedrivePolicy is invalid. Reason: Dead-letter target must be in same region as the source.", 400}
	}
	target := s.targetQueue(config.redrive.TargetARN)
	if target == nil || target.fifo != config.fifo || target.arn == sourceARN {
		return invalidQueueAttribute("RedrivePolicy")
	}
	return nil
}
