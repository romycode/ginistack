package sqs

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type store struct {
	mu           sync.Mutex
	queues       map[queueKey]*queue
	deleted      map[queueKey]time.Time
	incarnations map[string]deletedIncarnation
}

type deletedIncarnation struct {
	key     queueKey
	expires time.Time
}

type queue struct {
	queueConfig
	key             queueKey
	incarnation     string
	deletedReceipts map[string]time.Time
	arn             string
	lastPurge       time.Time
	dedup           map[dedupKey]dedupRecord
	attempts        map[string]receiveAttempt
	sequence        uint64
	tags            map[string]string
	messages        []*message
	moves           []*moveTask
	receipts        map[string]*message
	changed         chan struct{}
}

type message struct {
	id               string
	body             string
	md5              string
	visibleAt        time.Time
	sentAt           time.Time
	retentionFrom    time.Time
	firstReadAt      time.Time
	lastReadAt       time.Time
	receiveCount     int
	receipt          string
	receipts         []string
	groupID          string
	dedupID          string
	sequence         string
	sourceARN        string
	traceHeader      string
	customAttributes map[string]messageAttribute
}

type delivery struct {
	MessageID              string `json:"MessageId"`
	ReceiptHandle          string
	MD5OfBody              string
	Body                   string
	Attributes             map[string]string           `json:",omitempty"`
	MessageAttributes      map[string]messageAttribute `json:",omitempty"`
	MD5OfMessageAttributes string                      `json:",omitempty"`
}

func token() string {
	var data [16]byte
	_, _ = rand.Read(data[:])
	return hex.EncodeToString(data[:])
}

func checksum(body string) string {
	digest := md5.Sum([]byte(body))
	return hex.EncodeToString(digest[:])
}

// signal wakes waiting readers. The caller holds the store mutex.
func (q *queue) signal() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func (s *store) create(name queueKey, arn string, attributes, tags map[string]string) *apiError {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for key, until := range s.deleted {
		if !now.Before(until) {
			delete(s.deleted, key)
		}
	}
	for id, entry := range s.incarnations {
		if !now.Before(entry.expires) {
			delete(s.incarnations, id)
		}
	}
	if _, deleted := s.deleted[name]; deleted {
		return &apiError{"AWS.SimpleQueueService.QueueDeletedRecently", "You must wait 60 seconds after deleting a queue before you can create another with the same name.", 400}
	}
	if failure := validateQueueTags(name.name, tags); failure != nil {
		return failure
	}
	if q := s.queues[name]; q != nil {
		config, failure := q.queueConfig.withAttributes(name.name, attributes, true)
		if failure != nil {
			return failure
		}
		if attribute := q.queueConfig.changedAttribute(config); attribute != "" {
			return &apiError{"QueueAlreadyExists", "A queue already exists with the same name and a different value for attribute " + attribute, 400}
		}
		if len(tags) != 0 && !maps.Equal(q.tags, tags) {
			return &apiError{"QueueAlreadyExists", "A queue already exists with the same name and a different value for tags ", 400}
		}
		return nil
	}
	config, failure := newQueueConfig(name.name, attributes)
	if failure != nil {
		return failure
	}
	if failure := s.validateRedrive(arn, config, attributes["RedrivePolicy"], attributes["RedriveAllowPolicy"]); failure != nil {
		return failure
	}
	s.queues[name] = &queue{
		queueConfig:     config,
		key:             name,
		incarnation:     token(),
		arn:             arn,
		dedup:           make(map[dedupKey]dedupRecord),
		attempts:        make(map[string]receiveAttempt),
		tags:            tags,
		receipts:        make(map[string]*message),
		deletedReceipts: make(map[string]time.Time),
		changed:         make(chan struct{}),
	}
	return nil
}

func (s *store) remove(name queueKey) *apiError {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q := s.queues[name]; q != nil {
		delete(s.queues, name)
		for _, task := range q.moves {
			if task.Status == moveRunning {
				task.Status = "CANCELLED"
				close(task.stop)
			}
		}
		now := time.Now()
		s.deleted[name] = now.Add(time.Minute)
		s.incarnations[q.incarnation] = deletedIncarnation{key: name, expires: now.Add(12 * time.Hour)}
		q.signal()
		return nil
	}
	return missingQueue
}

func (s *store) queueAttributes(name queueKey, requested []string) (map[string]string, *apiError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return nil, missingQueue
	}
	all := map[string]string{
		"MaximumMessageSize":            strconv.Itoa(q.maximumSize),
		"VisibilityTimeout":             strconv.Itoa(int(q.visibility / time.Second)),
		"ReceiveMessageWaitTimeSeconds": strconv.Itoa(int(q.wait / time.Second)),
	}
	all["QueueArn"] = q.arn
	all["DelaySeconds"] = strconv.Itoa(int(q.delay / time.Second))
	all["MessageRetentionPeriod"] = strconv.Itoa(int(q.retention / time.Second))
	if q.fifo {
		all["FifoQueue"] = "true"
		all["ContentBasedDeduplication"] = strconv.FormatBool(q.contentDedup)
		all["DeduplicationScope"] = q.dedupScope
		all["FifoThroughputLimit"] = q.throughputLimit
	} else {
		all["DeduplicationScope"] = "queue"
		all["FifoThroughputLimit"] = "perQueue"
	}
	if q.redriveAllow.permission != "" {
		all["RedriveAllowPolicy"] = q.redriveAllow.json()
	}
	if q.redrive.TargetARN != "" {
		payload, _ := json.Marshal(q.redrive)
		all["RedrivePolicy"] = string(payload)
	}
	now := time.Now()
	q.expireMessages(now)
	var available, delayed, inFlight int
	for _, message := range q.messages {
		switch {
		case !now.Before(message.visibleAt):
			available++
		case message.receiveCount == 0:
			delayed++
		default:
			inFlight++
		}
	}
	all["ApproximateNumberOfMessages"] = strconv.Itoa(available)
	all["ApproximateNumberOfMessagesDelayed"] = strconv.Itoa(delayed)
	all["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(inFlight)
	attributes := make(map[string]string)
	for _, key := range requested {
		if key == "All" {
			maps.Copy(attributes, all)
			continue
		}
		value, exists := all[key]
		if !exists && (key == "RedrivePolicy" || key == "RedriveAllowPolicy") {
			continue
		}
		if !exists {
			if knownQueueAttribute(key) {
				return nil, notImplemented
			}
			return nil, unknownQueueAttribute(key)
		}
		attributes[key] = value
	}
	return attributes, nil
}

func (s *store) queueTags(name queueKey) (map[string]string, *apiError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return nil, missingQueue
	}
	// Tag mutations replace the map under the store lock.
	return q.tags, nil
}

func (s *store) send(name queueKey, input messageInput) (sendOutput, *apiError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return sendOutput{}, missingQueue
	}
	return q.send(input)
}

func (q *queue) send(input messageInput) (sendOutput, *apiError) {
	if failure := q.validateFIFO(input); failure != nil {
		return sendOutput{}, failure
	}
	if failure := validateMessage(input.MessageBody, input.DelaySeconds); failure != nil {
		return sendOutput{}, failure
	}
	if failure := validateMessageAttributes(input.MessageAttributes); failure != nil {
		return sendOutput{}, failure
	}
	if len(input.MessageBody)+attributeSize(input.MessageAttributes) > q.maximumSize {
		return sendOutput{}, &apiError{"InvalidParameterValue", fmt.Sprintf("One or more parameters are invalid. Reason: Message must be shorter than %d bytes.", q.maximumSize), 400}
	}
	trace, failure := messageTraceHeader(input.MessageSystemAttributes)
	if failure != nil {
		return sendOutput{}, failure
	}
	now := time.Now()
	m := &message{
		id: token(), body: input.MessageBody, md5: checksum(input.MessageBody),
		sentAt: now, retentionFrom: now, visibleAt: now.Add(durationOrDefault(input.DelaySeconds, q.delay)),
		customAttributes: input.MessageAttributes,
	}
	if input.MessageGroupID != nil {
		m.groupID = *input.MessageGroupID
	}
	m.traceHeader = trace
	output := sendOutput{
		MessageID: m.id, MD5OfMessageBody: m.md5,
		MD5OfMessageAttributes:       attributeDigest(input.MessageAttributes),
		MD5OfMessageSystemAttributes: attributeDigest(input.MessageSystemAttributes),
	}
	if q.fifo {
		for key, record := range q.dedup {
			if !now.Before(record.expires) {
				delete(q.dedup, key)
			}
		}
		key := q.fifoKey(input)
		if saved, exists := q.dedup[key]; exists {
			output.MessageID = saved.output.MessageID
			output.SequenceNumber = saved.output.SequenceNumber
			return output, nil
		}
		q.sequence++
		m.sequence = fifoSequence(q.sequence)
		m.dedupID = key.id
		output.SequenceNumber = m.sequence
		q.dedup[key] = dedupRecord{expires: now.Add(5 * time.Minute), output: output}
	}
	q.messages = append(q.messages, m)
	q.signal()
	return output, nil
}

func (s *store) receive(ctx context.Context, name queueKey, count int, visibility, wait *int, systemAttributes, requestedAttributes []string, attemptID string) ([]delivery, *apiError) {
	s.mu.Lock()
	q := s.queues[name]
	if q == nil {
		s.mu.Unlock()
		return nil, missingQueue
	}
	if attemptID != "" && (!q.fifo || !validFIFOToken(attemptID)) {
		s.mu.Unlock()
		return nil, invalidFIFOToken("ReceiveRequestAttemptId", attemptID)
	}
	start := time.Now()
	deadline := start.Add(durationOrDefault(wait, q.wait))
	timeout := durationOrDefault(visibility, q.visibility)

	for {
		if ctx.Err() != nil {
			s.mu.Unlock()
			return nil, nil
		}
		if s.queues[name] != q {
			s.mu.Unlock()
			return nil, missingQueue
		}
		now := time.Now()
		next := deadline
		q.expireMessages(now)
		if attemptID != "" {
			messages, failure := q.replay(attemptID, now, timeout, systemAttributes, requestedAttributes)
			if len(messages) != 0 || failure != nil {
				s.mu.Unlock()
				return messages, failure
			}
		}
		s.moveDeadLetters(q, now)
		var messages []delivery
		blockedGroups := make(map[string]bool)
		for _, m := range q.messages {
			if q.fifo && blockedGroups[m.groupID] {
				continue
			}
			if now.Before(m.visibleAt) {
				if q.fifo {
					blockedGroups[m.groupID] = true
				}
				next = nextOrEarlier(next, m.visibleAt)
				continue
			}
			m.receipt = q.incarnation + "." + token()
			m.receipts = append(m.receipts, m.receipt)
			q.receipts[m.receipt] = m
			m.visibleAt = now.Add(timeout)
			m.receiveCount++
			m.lastReadAt = now
			if m.receiveCount == 1 {
				m.firstReadAt = now
			}
			messages = append(messages, m.delivery(systemAttributes, requestedAttributes))
			if len(messages) == count {
				break
			}
		}
		if attemptID != "" && len(messages) != 0 {
			attempt := receiveAttempt{expires: now.Add(5 * time.Minute), deadline: now.Add(timeout), receipts: make([]string, 0, len(messages))}
			for _, message := range messages {
				attempt.receipts = append(attempt.receipts, message.ReceiptHandle)
			}
			q.attempts[attemptID] = attempt
		}
		changed := q.changed
		s.mu.Unlock()
		if len(messages) != 0 || !now.Before(deadline) {
			return messages, nil
		}

		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
		s.mu.Lock()
	}
}

func (m *message) delivery(systemAttributes, requestedAttributes []string) delivery {
	selected := selectMessageAttributes(m.customAttributes, requestedAttributes)
	return delivery{
		MessageID: m.id, ReceiptHandle: m.receipt, MD5OfBody: m.md5, Body: m.body,
		Attributes:        m.systemAttributes(systemAttributes),
		MessageAttributes: selected, MD5OfMessageAttributes: attributeDigest(selected),
	}
}

func (m *message) systemAttributes(requested []string) map[string]string {
	if len(requested) == 0 {
		return nil
	}
	values := map[string]string{
		"ApproximateReceiveCount":          strconv.Itoa(m.receiveCount),
		"ApproximateFirstReceiveTimestamp": strconv.FormatInt(m.firstReadAt.UnixMilli(), 10),
		"SentTimestamp":                    strconv.FormatInt(m.sentAt.UnixMilli(), 10),
	}
	for name, value := range map[string]string{
		"MessageGroupId": m.groupID, "MessageDeduplicationId": m.dedupID,
		"SequenceNumber": m.sequence, "DeadLetterQueueSourceArn": m.sourceARN, "AWSTraceHeader": m.traceHeader,
	} {
		if value != "" {
			values[name] = value
		}
	}
	attributes := make(map[string]string)
	for _, name := range requested {
		if name == "All" {
			return values
		}
		if value, ok := values[name]; ok {
			attributes[name] = value
		}
	}
	return attributes
}

func nextOrEarlier(next, candidate time.Time) time.Time {
	if candidate.Before(next) {
		return candidate
	}
	return next
}

func (s *store) deleteMessage(name queueKey, receipt string) *apiError {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return missingQueue
	}
	if failure := s.foreignReceipt(q, receipt); failure != nil {
		return failure
	}
	return q.deleteMessage(receipt)
}

func (s *store) foreignReceipt(queue *queue, receipt string) *apiError {
	if queue.receipts[receipt] != nil || queue.deletedReceipts[receipt].After(time.Now()) {
		return nil
	}
	if id, _, found := strings.Cut(receipt, "."); found {
		if entry, exists := s.incarnations[id]; exists && entry.key == queue.key && entry.expires.After(time.Now()) {
			return missingQueue
		}
	}
	for _, other := range s.queues {
		if other.key.region == queue.key.region && other != queue && other.receipts[receipt] != nil {
			return &apiError{"ReceiptHandleIsInvalid", fmt.Sprintf("The receipt handle %q is not valid for this queue.", receipt), 400}
		}
	}
	return nil
}

func (q *queue) deleteMessage(receipt string) *apiError {
	now := time.Now()
	for handle, expiry := range q.deletedReceipts {
		if !now.Before(expiry) {
			delete(q.deletedReceipts, handle)
		}
	}
	m := q.receipts[receipt]
	if m == nil {
		if q.deletedReceipts[receipt].After(now) {
			return nil
		}
		return invalidReceipt
	}
	// SQS accepts an old receipt handle, but may leave the current delivery intact.
	if m.receipt != receipt {
		if q.fifo {
			return expiredReceipt(receipt)
		}
		return nil
	}
	for i, candidate := range q.messages {
		if candidate == m {
			for _, handle := range m.receipts {
				q.deletedReceipts[handle] = now.Add(12 * time.Hour)
			}
			q.removeMessage(i)
			break
		}
	}
	return nil
}

func (q *queue) expireMessages(now time.Time) {
	for i := 0; i < len(q.messages); {
		if now.Before(q.messages[i].retentionFrom.Add(q.retention)) {
			i++
			continue
		}
		q.removeMessage(i)
	}
}

func (q *queue) removeMessage(index int) {
	for _, handle := range q.messages[index].receipts {
		delete(q.receipts, handle)
	}
	copy(q.messages[index:], q.messages[index+1:])
	q.messages[len(q.messages)-1] = nil
	q.messages = q.messages[:len(q.messages)-1]
	q.signal()
}

func (s *store) changeVisibility(name queueKey, receipt string, timeout time.Duration) *apiError {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return missingQueue
	}
	if failure := s.foreignReceipt(q, receipt); failure != nil {
		return failure
	}
	return q.changeVisibility(receipt, timeout)
}

func (q *queue) changeVisibility(receipt string, timeout time.Duration) *apiError {
	now := time.Now()
	m := q.receipts[receipt]
	if m == nil {
		if q.deletedReceipts[receipt].After(now) {
			return &apiError{"InvalidParameterValue", "Value " + receipt + " for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change.", 400}
		}
		return invalidReceipt
	}
	if m.receipt != receipt {
		return expiredReceipt(receipt)
	}
	deadline := now.Add(timeout)
	if deadline.After(m.lastReadAt.Add(12 * time.Hour)) {
		return &apiError{"InvalidParameterValue", fmt.Sprintf("Value %d for parameter VisibilityTimeout is invalid. Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds].", int(timeout/time.Second)), 400}
	}
	q.updateVisibility(m, deadline)
	return nil
}

func expiredReceipt(receipt string) *apiError {
	return &apiError{"InvalidParameterValue", "Value " + receipt + " for parameter ReceiptHandle is invalid. Reason: The receipt handle has expired.", 400}
}

func (q *queue) updateVisibility(m *message, deadline time.Time) {
	m.visibleAt = deadline
	if q.fifo && !deadline.After(time.Now()) {
		slices.SortStableFunc(q.messages, func(a, b *message) int {
			if (a.groupID == m.groupID) == (b.groupID == m.groupID) {
				return 0
			}
			if a.groupID == m.groupID {
				return 1
			}
			return -1
		})
	}
	q.signal()
}

func durationOrDefault(seconds *int, fallback time.Duration) time.Duration {
	if seconds == nil {
		return fallback
	}
	return time.Duration(*seconds) * time.Second
}
