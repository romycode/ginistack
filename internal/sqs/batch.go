package sqs

import (
	"fmt"
	"net/http"
	"time"
)

type batchInput[Entry any] struct {
	queueInput
	Entries []Entry
}

type batchID struct {
	ID string `json:"Id"`
}

func (entry batchID) id() string {
	return entry.ID
}

type sendBatchEntry struct {
	batchID
	messageInput
}

type sendBatchResult struct {
	batchID
	sendOutput
}

type receiptBatchEntry struct {
	batchID
	ReceiptHandle string
}

type visibilityBatchEntry struct {
	receiptBatchEntry
	VisibilityTimeout *int
}

type batchFailure struct {
	batchID
	Code        string
	Message     string
	SenderFault bool
}

type batchOutput[Result any] struct {
	Successful []Result
	Failed     []batchFailure
}

func (s *service) sendMessageBatch(_ *http.Request, input batchInput[sendBatchEntry]) (batchOutput[sendBatchResult], *apiError) {
	total := 0
	for _, entry := range input.Entries {
		total += len(entry.MessageBody) + attributeSize(entry.MessageAttributes)
	}
	if total > maxMessageSize {
		return batchOutput[sendBatchResult]{}, batchRequestError("BatchRequestTooLong",
			fmt.Sprintf("Batch requests cannot be longer than %d bytes. You have sent %d bytes.", maxMessageSize, total))
	}
	return s.runBatch("SendMessageBatch", input, func(q *queue) *apiError {
		for _, entry := range input.Entries {
			if failure := q.validateFIFO(entry.messageInput); failure != nil {
				if failure.code == "MissingParameter" {
					return &apiError{"InvalidParameterValue", failure.message, failure.status}
				}
				return failure
			}
		}
		return nil
	}, func(q *queue, index int, entry sendBatchEntry) (sendBatchResult, *apiError) {
		if entry.MessageBody == "" {
			return sendBatchResult{}, &apiError{"EmptyValue",
				fmt.Sprintf("No Value Found for SendMessageBatchRequestEntry.%d.MessageBody.", index+1), 400}
		}
		output, failure := q.send(entry.messageInput)
		if failure != nil {
			return sendBatchResult{}, failure
		}
		return sendBatchResult{batchID: entry.batchID, sendOutput: output}, nil
	})
}

func (s *service) deleteMessageBatch(_ *http.Request, input batchInput[receiptBatchEntry]) (batchOutput[batchID], *apiError) {
	deleted := make(map[string]struct{})
	return s.runBatch("DeleteMessageBatch", input, nil, func(q *queue, _ int, entry receiptBatchEntry) (batchID, *apiError) {
		receipt := entry.ReceiptHandle
		if _, handled := deleted[receipt]; handled {
			return entry.batchID, nil
		}
		if failure := q.deleteMessage(receipt); failure != nil {
			for _, other := range s.store.queues {
				if other != q && other.receipts[receipt] != nil {
					return entry.batchID, &apiError{"ReceiptHandleIsInvalid",
						fmt.Sprintf("The receipt handle %q is not valid for this queue.", receipt), 400}
				}
			}
			return entry.batchID, failure
		}
		deleted[receipt] = struct{}{}
		return entry.batchID, nil
	})
}

func (s *service) changeMessageVisibilityBatch(_ *http.Request, input batchInput[visibilityBatchEntry]) (batchOutput[batchID], *apiError) {
	updated := make(map[string]*message)
	return s.runBatch("ChangeMessageVisibilityBatch", input, nil, func(q *queue, _ int, entry visibilityBatchEntry) (batchID, *apiError) {
		if entry.VisibilityTimeout == nil {
			return entry.batchID, missingParameter("VisibilityTimeout")
		}
		if failure := validateRange("VisibilityTimeout", entry.VisibilityTimeout, 0, 43200); failure != nil {
			return entry.batchID, failure
		}
		receipt := entry.ReceiptHandle
		timeout := durationOrDefault(entry.VisibilityTimeout, 0)
		if m := updated[receipt]; m != nil {
			q.updateVisibility(m, time.Now().Add(timeout))
			return entry.batchID, nil
		}
		if failure := q.changeVisibility(receipt, timeout); failure != nil {
			return entry.batchID, failure
		}
		updated[receipt] = q.receipts[receipt]
		return entry.batchID, nil
	})
}

// runBatch validates the request before applying any entry. It holds the store
// lock so queue deletion or recreation cannot redirect entries mid-request.
func (s *service) runBatch[Entry interface{ id() string }, Result any](
	operation string, input batchInput[Entry], preflight func(*queue) *apiError, apply func(*queue, int, Entry) (Result, *apiError),
) (batchOutput[Result], *apiError) {
	if failure := validateBatch(operation, input.Entries); failure != nil {
		return batchOutput[Result]{}, failure
	}
	name, failure := queueName(input.queueInput)
	if failure != nil {
		return batchOutput[Result]{}, failure
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	q := s.store.queues[name]
	if q == nil {
		return batchOutput[Result]{}, missingQueue
	}
	if preflight != nil {
		if failure := preflight(q); failure != nil {
			return batchOutput[Result]{}, failure
		}
	}
	output := batchOutput[Result]{
		Successful: make([]Result, 0, len(input.Entries)),
		Failed:     []batchFailure{},
	}
	for index, entry := range input.Entries {
		result, failure := apply(q, index, entry)
		if failure != nil {
			output.Failed = append(output.Failed, batchFailure{
				batchID: batchID{ID: entry.id()},
				Code:    failure.code, Message: failure.message, SenderFault: failure.status < 500,
			})
			continue
		}
		output.Successful = append(output.Successful, result)
	}
	return output, nil
}

func validateBatch[Entry interface{ id() string }](operation string, entries []Entry) *apiError {
	const maxEntries = 10
	if len(entries) == 0 {
		return batchRequestError("EmptyBatchRequest", "There should be at least one "+operation+"RequestEntry in the request.")
	}
	if len(entries) > maxEntries {
		return batchRequestError("TooManyEntriesInBatchRequest",
			fmt.Sprintf("Maximum number of entries per request are %d. You have sent %d.", maxEntries, len(entries)))
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		id := entry.id()
		if !queueNamePattern.MatchString(id) {
			return batchRequestError("InvalidBatchEntryId",
				"A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.")
		}
		if _, duplicate := seen[id]; duplicate {
			return batchRequestError("BatchEntryIdsNotDistinct", fmt.Sprintf("Id %s repeated.", id))
		}
		seen[id] = struct{}{}
	}
	return nil
}

func batchRequestError(code, message string) *apiError {
	return &apiError{"AWS.SimpleQueueService." + code, message, 400}
}
