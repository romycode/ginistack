package sqs

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/romycode/ginistack/internal/awshttp"
)

type listQueuesInput struct {
	QueueNamePrefix string
	MaxResults      *int
	NextToken       string
}

type listQueuesOutput struct {
	QueueURLs []string `json:"QueueUrls,omitempty"`
	NextToken string   `json:",omitempty"`
}

type queueCursor struct {
	Prefix string
	After  string
}

func (s *service) listQueues(request *http.Request, input listQueuesInput) (listQueuesOutput, *apiError) {
	limit := 1000
	if input.MaxResults != nil {
		limit = *input.MaxResults
		if limit < 1 || limit > 1000 {
			return listQueuesOutput{}, &apiError{"InvalidParameterValue",
				"Value for parameter MaxResults is invalid. Reason: MaxResults must be an integer between 1 and 1000.", 400}
		}
	}
	region := awshttp.Region(request)
	scope := region + "/" + input.QueueNamePrefix
	cursor := queueCursor{Prefix: scope}
	if input.NextToken != "" {
		var ok bool
		cursor, ok = s.readQueueCursor(input.NextToken)
		if !ok || cursor.Prefix != scope {
			return listQueuesOutput{}, &apiError{"InvalidParameterValue", "Invalid or expired next token.", 400}
		}
	}

	s.store.mu.Lock()
	names := make([]string, 0, len(s.store.queues))
	for key := range s.store.queues {
		if key.region == region && key.name > cursor.After && strings.HasPrefix(key.name, input.QueueNamePrefix) {
			names = append(names, key.name)
		}
	}
	s.store.mu.Unlock()
	slices.Sort(names)

	var output listQueuesOutput
	if len(names) > limit {
		names = names[:limit]
		if input.MaxResults != nil {
			cursor.After = names[len(names)-1]
			output.NextToken = s.makeQueueCursor(cursor)
		}
	}
	for _, name := range names {
		output.QueueURLs = append(output.QueueURLs, queueAddress(request, queueKey{region: region, name: name}).QueueURL)
	}
	return output, nil
}

func (s *service) makeQueueCursor(cursor queueCursor) string {
	payload, _ := json.Marshal(cursor)
	mac := hmac.New(sha256.New, s.cursorKey)
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(append(mac.Sum(nil), payload...))
}

func (s *service) readQueueCursor(encoded string) (queueCursor, bool) {
	var cursor queueCursor
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) <= sha256.Size {
		return cursor, false
	}
	signature, payload := raw[:sha256.Size], raw[sha256.Size:]
	mac := hmac.New(sha256.New, s.cursorKey)
	_, _ = mac.Write(payload)
	valid := hmac.Equal(signature, mac.Sum(nil)) && json.Unmarshal(payload, &cursor) == nil
	return cursor, valid
}

func (s *service) purgeQueue(request *http.Request, input queueInput) (empty, *apiError) {
	name, failure := queueName(request, input)
	if failure != nil {
		return empty{}, failure
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	q := s.store.queues[name]
	if q == nil {
		return empty{}, missingQueue
	}
	now := time.Now()
	if now.Before(q.lastPurge.Add(time.Minute)) {
		return empty{}, &apiError{"AWS.SimpleQueueService.PurgeQueueInProgress",
			"Only one PurgeQueue operation on " + name.name + " is allowed every 60 seconds.", 400}
	}
	q.lastPurge = now
	q.messages = nil
	clear(q.receipts)
	clear(q.deletedReceipts)
	clear(q.dedup)
	clear(q.attempts)
	q.signal()
	return empty{}, nil
}

type tagQueueInput struct {
	queueInput
	Tags map[string]string
}

type untagQueueInput struct {
	queueInput
	TagKeys []string
}

func (s *service) tagQueue(request *http.Request, input tagQueueInput) (empty, *apiError) {
	name, failure := queueName(request, input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if len(input.Tags) == 0 {
		return empty{}, missingParameter("Tags")
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	q := s.store.queues[name]
	if q == nil {
		return empty{}, missingQueue
	}
	tags := make(map[string]string, len(q.tags)+len(input.Tags))
	maps.Copy(tags, q.tags)
	maps.Copy(tags, input.Tags)
	if failure := validateQueueTags(name.name, tags); failure != nil {
		return empty{}, failure
	}
	q.tags = tags
	return empty{}, nil
}

func (s *service) untagQueue(request *http.Request, input untagQueueInput) (empty, *apiError) {
	name, failure := queueName(request, input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if len(input.TagKeys) == 0 {
		return empty{}, missingParameter("TagKeys")
	}
	for _, key := range input.TagKeys {
		if failure := validateTagKey(key); failure != nil {
			return empty{}, failure
		}
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	q := s.store.queues[name]
	if q == nil {
		return empty{}, missingQueue
	}
	tags := maps.Clone(q.tags)
	for _, key := range input.TagKeys {
		delete(tags, key)
	}
	q.tags = tags
	return empty{}, nil
}

func validateQueueTags(name string, tags map[string]string) *apiError {
	if len(tags) > 50 {
		return &apiError{"InvalidParameterValue", "Too many tags added for queue " + name + ".", 400}
	}
	for key, value := range tags {
		if failure := validateTagKey(key); failure != nil {
			return failure
		}
		if utf8.RuneCountInString(value) > 256 {
			return &apiError{"InvalidParameterValue", "Tag values must be between 0 and 256 characters in length.", 400}
		}
	}
	return nil
}

func validateTagKey(key string) *apiError {
	if length := utf8.RuneCountInString(key); length < 1 || length > 128 {
		return &apiError{"InvalidParameterValue", "Tag keys must be between 1 and 128 characters in length.", 400}
	}
	if strings.HasPrefix(strings.ToLower(key), "aws:") {
		return &apiError{"InvalidParameterValue", "Caller is an end user and not allowed to mutate system tags", 400}
	}
	return nil
}
