package sqs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

type dedupKey struct {
	group string
	id    string
}

type dedupRecord struct {
	expires time.Time
	output  sendOutput
}

func (q *queue) validateFIFO(input messageInput) *apiError {
	if !q.fifo {
		if input.MessageGroupID != nil && (*input.MessageGroupID == "" || !validFIFOToken(*input.MessageGroupID)) {
			return invalidFIFOToken("MessageGroupId", *input.MessageGroupID)
		}
		if input.MessageDeduplicationID != nil {
			return &apiError{"InvalidParameterValue", "Value " + *input.MessageDeduplicationID + " for parameter MessageDeduplicationId is invalid. Reason: The request include parameter that is not valid for this queue type.", 400}
		}
		return nil
	}
	if input.MessageGroupID == nil || *input.MessageGroupID == "" {
		return missingParameter("MessageGroupId")
	}
	if input.DelaySeconds != nil && *input.DelaySeconds != 0 {
		return &apiError{"InvalidParameterValue", "Value " + strconv.Itoa(*input.DelaySeconds) + " for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.", 400}
	}
	if input.MessageDeduplicationID == nil && !q.contentDedup {
		return &apiError{"InvalidParameterValue", "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly", 400}
	}
	if !validFIFOToken(*input.MessageGroupID) {
		return invalidFIFOToken("MessageGroupId", *input.MessageGroupID)
	}
	if input.MessageDeduplicationID != nil && (*input.MessageDeduplicationID == "" || !validFIFOToken(*input.MessageDeduplicationID)) {
		return invalidFIFOToken("MessageDeduplicationId", *input.MessageDeduplicationID)
	}
	return nil
}

func fifoSequence(sequence uint64) string {
	return fmt.Sprintf("%039d", sequence)
}

func invalidFIFOToken(name, value string) *apiError {
	return &apiError{"InvalidParameterValue",
		fmt.Sprintf("Value %s for parameter %s is invalid. Reason: %s can only include alphanumeric and punctuation characters. 1 to 128 in length.", value, name, name), 400}
}

func validFIFOToken(value string) bool {
	if len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < '!' || character > '~' {
			return false
		}
	}
	return true
}

func (q *queue) fifoKey(input messageInput) dedupKey {
	var key dedupKey
	if input.MessageDeduplicationID != nil {
		key.id = *input.MessageDeduplicationID
	}
	if q.dedupScope == "messageGroup" {
		key.group = *input.MessageGroupID
	}
	if key.id == "" {
		digest := sha256.Sum256([]byte(input.MessageBody))
		key.id = hex.EncodeToString(digest[:])
	}
	return key
}

func (s *store) moveDeadLetters(source *queue, now time.Time) {
	if source.redrive.Count == 0 {
		return
	}
	target := s.targetQueue(source.redrive.TargetARN)
	if target == nil || target == source || target.fifo != source.fifo || !target.redriveAllow.permits(source.arn) {
		return
	}
	for i := 0; i < len(source.messages); {
		m := source.messages[i]
		if m.receiveCount < source.redrive.Count || now.Before(m.visibleAt) {
			i++
			continue
		}
		source.removeMessage(i)
		m.sourceARN = source.arn
		m.receiveCount = 0
		m.firstReadAt = time.Time{}
		m.receipt = ""
		m.receipts = nil
		m.visibleAt = now
		if target.fifo {
			m.retentionFrom = now
			m.dedupID = m.id
			target.sequence++
			m.sequence = fifoSequence(target.sequence)
		}
		target.messages = append(target.messages, m)
		target.signal()
	}
}

type receiveAttempt struct {
	expires  time.Time
	deadline time.Time
	receipts []string
}

func (q *queue) replay(attemptID string, now time.Time, timeout time.Duration, systemAttributes, requestedAttributes []string) ([]delivery, *apiError) {
	for id, attempt := range q.attempts {
		if !now.Before(attempt.expires) {
			delete(q.attempts, id)
		}
	}
	attempt, exists := q.attempts[attemptID]
	if !exists {
		return nil, nil
	}
	messages := make([]delivery, 0, len(attempt.receipts))
	for _, receipt := range attempt.receipts {
		m := q.receipts[receipt]
		if m == nil || m.receipt != receipt || !m.visibleAt.Equal(attempt.deadline) {
			return nil, &apiError{"InvalidParameterValue", "Value " + attemptID + " for parameter ReceiveRequestAttemptId is invalid. Reason: The receiveRequestAttemptId is not valid anymore since the receive call is not retryable.", 400}
		}
		body, failure := m.openBody(q.bodyCipher)
		if failure != nil {
			return nil, failure
		}
		messages = append(messages, m.delivery(body, systemAttributes, requestedAttributes))
	}
	attempt.deadline = now.Add(timeout)
	q.attempts[attemptID] = attempt
	for _, receipt := range attempt.receipts {
		q.receipts[receipt].visibleAt = attempt.deadline
	}
	q.signal()
	return messages, nil
}
