package sqs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/romycode/ginistack/internal/awshttp"
)

type redriveAllow struct {
	permission string
	sources    string
}

type redriveAllowDocument struct {
	Permission string   `json:"redrivePermission"`
	Sources    []string `json:"sourceQueueArns,omitempty"`
}

func parseRedriveAllow(raw string) (redriveAllow, *apiError) {
	if raw == "" {
		return redriveAllow{}, nil
	}
	var doc redriveAllowDocument
	err := json.Unmarshal([]byte(raw), &doc)
	if err != nil || !slices.Contains([]string{"allowAll", "denyAll", "byQueue"}, doc.Permission) {
		return redriveAllow{}, &apiError{"InvalidAttributeValue", "Invalid value for the parameter RedriveAllowPolicy. Reason: Amazon SQS can't create the redrive allow policy, as it’s in an unsupported format.", 400}
	}
	prefix := "Amazon SQS can't create the redrive allow policy. "
	switch {
	case len(doc.Sources) > 10:
		return redriveAllow{}, invalidPolicy("RedriveAllowPolicy", raw, prefix+"The maximum number of source queue ARNs you can specify is 10.")
	case doc.Permission == "byQueue" && len(doc.Sources) == 0:
		return redriveAllow{}, invalidPolicy("RedriveAllowPolicy", raw, prefix+"When you specify the byQueue permission type, you must also specify one or more sourceQueueArns values.")
	case doc.Permission != "byQueue" && len(doc.Sources) != 0:
		return redriveAllow{}, invalidPolicy("RedriveAllowPolicy", raw, prefix+"To specify one or more source queue ARNs, you must set redrivePermission to byQueue.")
	}
	for _, source := range doc.Sources {
		parts := strings.SplitN(source, ":", 6)
		if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sqs" || !validQueueName(parts[5]) || strings.Contains(source, "\n") {
			return redriveAllow{}, invalidQueueAttribute("RedriveAllowPolicy")
		}
	}
	return redriveAllow{permission: doc.Permission, sources: strings.Join(doc.Sources, "\n")}, nil
}

func (policy redriveAllow) permits(arn string) bool {
	switch policy.permission {
	case "denyAll":
		return false
	case "byQueue":
		return slices.Contains(strings.Split(policy.sources, "\n"), arn)
	default:
		return true
	}
}

func (policy redriveAllow) json() string {
	doc := redriveAllowDocument{Permission: policy.permission}
	if policy.sources != "" {
		doc.Sources = strings.Split(policy.sources, "\n")
	}
	raw, _ := json.Marshal(doc)
	return string(raw)
}

func invalidPolicy(name, raw, reason string) *apiError {
	return &apiError{"InvalidParameterValue", fmt.Sprintf("Value %s for parameter %s is invalid. Reason: %s", raw, name, reason), 400}
}

type deadLetterSourcesInput struct {
	queueInput
	MaxResults *int
	NextToken  string
}

type deadLetterSourcesOutput struct {
	QueueURLs []string `json:"queueUrls,omitempty"`
	NextToken string   `json:",omitempty"`
}

func (s *service) listDeadLetterSources(r *http.Request, input deadLetterSourcesInput) (deadLetterSourcesOutput, *apiError) {
	name, failure := queueName(r, input.queueInput)
	if failure != nil {
		return deadLetterSourcesOutput{}, failure
	}
	limit := 1000
	if input.MaxResults != nil {
		limit = *input.MaxResults
		if limit < 1 || limit > 1000 {
			return deadLetterSourcesOutput{}, &apiError{"InvalidParameterValue", "Value for parameter MaxResults is invalid. Reason: MaxResults must be an integer between 1 and 1000.", 400}
		}
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	destination := s.store.queues[name]
	if destination == nil {
		return deadLetterSourcesOutput{}, missingQueue
	}
	cursor := queueCursor{Prefix: destination.arn}
	if input.NextToken != "" {
		var ok bool
		cursor, ok = s.readQueueCursor(input.NextToken)
		if !ok || cursor.Prefix != destination.arn {
			return deadLetterSourcesOutput{}, &apiError{"InvalidParameterValue", "Invalid or expired next token.", 400}
		}
	}
	var names []string
	for sourceName, q := range s.store.queues {
		if q.redrive.TargetARN == destination.arn && sourceName.name > cursor.After {
			names = append(names, sourceName.name)
		}
	}
	slices.Sort(names)
	var output deadLetterSourcesOutput
	if len(names) > limit {
		names = names[:limit]
		if input.MaxResults != nil {
			cursor.After = names[len(names)-1]
			output.NextToken = s.makeQueueCursor(cursor)
		}
	}
	for _, sourceName := range names {
		output.QueueURLs = append(output.QueueURLs, queueAddress(r, queueKey{region: name.region, name: sourceName}).QueueURL)
	}
	return output, nil
}

const moveRunning = "RUNNING"

type startMoveInput struct {
	SourceARN                    string `json:"SourceArn"`
	DestinationARN               string `json:"DestinationArn"`
	MaxNumberOfMessagesPerSecond *int
}

type moveHandle struct {
	TaskHandle string `json:",omitempty"`
}

type moveProgress struct {
	moveHandle
	Status                       string
	SourceARN                    string `json:"SourceArn"`
	DestinationARN               string `json:"DestinationArn,omitempty"`
	MaxNumberOfMessagesPerSecond *int   `json:",omitempty"`
	Moved                        int    `json:"ApproximateNumberOfMessagesMoved"`
	ToMove                       int    `json:"ApproximateNumberOfMessagesToMove"`
	FailureReason                string `json:",omitempty"`
	StartedTimestamp             int64
}

type moveTask struct {
	moveProgress
	stop chan struct{}
	rate int
}

type listMoveInput struct {
	SourceARN  string `json:"SourceArn"`
	MaxResults *int
}

type listMoveOutput struct {
	Results []moveProgress
}

type cancelMoveOutput struct {
	Moved int `json:"ApproximateNumberOfMessagesMoved"`
}

func (s *service) startMessageMove(request *http.Request, input startMoveInput) (moveHandle, *apiError) {
	rate := 500
	if input.MaxNumberOfMessagesPerSecond != nil {
		rate = *input.MaxNumberOfMessagesPerSecond
		if rate < 1 || rate > 500 {
			return moveHandle{}, &apiError{"InvalidParameterValue", "Value for parameter MaxNumberOfMessagesPerSecond is invalid. Reason: You must enter a number that's between 1 and 500.", 400}
		}
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	source := s.store.targetQueue(input.SourceARN)
	if source == nil || !arnInRegion(input.SourceARN, awshttp.Region(request)) {
		return moveHandle{}, &apiError{"ResourceNotFoundException", "Source queue does not exist.", 400}
	}
	isDeadLetterQueue := false
	for _, candidate := range s.store.queues {
		if candidate.redrive.TargetARN == source.arn {
			isDeadLetterQueue = true
			break
		}
	}
	if !isDeadLetterQueue {
		return moveHandle{}, &apiError{"InvalidParameterValue", "Source queue must be configured as a Dead Letter Queue.", 400}
	}
	if input.DestinationARN != "" {
		if input.DestinationARN == source.arn {
			return moveHandle{}, &apiError{"InvalidParameterValue", "Source queue arn and destination queue arn cannot be the same.", 400}
		}
		destination := s.store.targetQueue(input.DestinationARN)
		if destination == nil || !sameRegion(input.SourceARN, input.DestinationARN) {
			return moveHandle{}, &apiError{"ResourceNotFoundException", "The resource that you specified for the DestinationArn parameter doesn't exist.", 400}
		}
		if source.fifo != destination.fifo {
			return moveHandle{}, &apiError{"InvalidParameterValue", "The source queue and destination queue must be of the same queue type.", 400}
		}
	}
	if len(source.moves) > 0 && source.moves[0].Status == moveRunning {
		return moveHandle{}, &apiError{"InvalidParameterValue", "There is already a task running. Only one active task is allowed for a source queue arn at a given time.", 400}
	}
	now := time.Now()
	source.expireMessages(now)
	task := &moveTask{
		moveProgress: moveProgress{
			moveHandle: moveHandle{TaskHandle: token()}, Status: moveRunning,
			SourceARN: source.arn, DestinationARN: input.DestinationARN,
			MaxNumberOfMessagesPerSecond: input.MaxNumberOfMessagesPerSecond, ToMove: len(source.messages), StartedTimestamp: now.UnixMilli(),
		},
		stop: make(chan struct{}),
		rate: rate,
	}
	source.moves = append([]*moveTask{task}, source.moves...)
	if len(source.moves) > 10 {
		source.moves = source.moves[:10]
	}
	go s.store.runMoveTask(source, task)
	return task.moveHandle, nil
}

func arnInRegion(arn, region string) bool {
	parts := strings.SplitN(arn, ":", 6)
	return len(parts) == 6 && parts[3] == region
}

func sameRegion(a, b string) bool {
	left, right := strings.SplitN(a, ":", 6), strings.SplitN(b, ":", 6)
	return len(left) == 6 && len(right) == 6 && left[3] == right[3]
}

func (s *store) runMoveTask(source *queue, task *moveTask) {
	ticker := time.NewTicker(time.Second / time.Duration(task.rate))
	defer ticker.Stop()
	for {
		select {
		case <-task.stop:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			if task.Status == moveRunning {
				s.moveOneMessage(source, task, now)
			}
			finished := task.Status != moveRunning
			s.mu.Unlock()
			if finished {
				return
			}
		}
	}
}

func (s *store) moveOneMessage(source *queue, task *moveTask, now time.Time) {
	source.expireMessages(now)
	if task.Moved >= task.ToMove || len(source.messages) == 0 {
		task.Status = "COMPLETED"
		return
	}
	for i, m := range source.messages {
		if now.Before(m.visibleAt) {
			continue
		}
		destinationARN := task.DestinationARN
		if destinationARN == "" {
			destinationARN = m.sourceARN
			if destinationARN == "" {
				task.Status = "FAILED"
				task.FailureReason = "CouldNotDetermineMessageSource"
				return
			}
		}
		destination := s.targetQueue(destinationARN)
		if destination == nil {
			task.Status = "FAILED"
			task.FailureReason = "DestinationQueueDoesNotExist"
			return
		}
		source.removeMessage(i)
		m.id = token()
		m.sentAt = now
		m.retentionFrom = now
		m.receiveCount = 0
		m.firstReadAt = time.Time{}
		m.receipt = ""
		m.receipts = nil
		m.sourceARN = ""
		m.visibleAt = now.Add(destination.delay)
		if destination.fifo {
			destination.sequence++
			m.sequence = fifoSequence(destination.sequence)
			m.dedupID = m.id
		}
		destination.messages = append(destination.messages, m)
		destination.signal()
		task.Moved++
		return
	}
}

func (s *service) listMessageMoves(request *http.Request, input listMoveInput) (listMoveOutput, *apiError) {
	limit := 10
	if input.MaxResults != nil {
		limit = *input.MaxResults
		if limit < 1 || limit > 10 {
			return listMoveOutput{}, &apiError{"InvalidParameterValue", "Value for parameter MaxResults is invalid. Reason: You must enter a number that's between 1 and 10.", 400}
		}
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	source := s.store.targetQueue(input.SourceARN)
	if source == nil || !arnInRegion(input.SourceARN, awshttp.Region(request)) {
		return listMoveOutput{}, &apiError{"ResourceNotFoundException", "Source queue does not exist.", 400}
	}
	results := make([]moveProgress, 0, min(limit, len(source.moves)))
	for _, task := range source.moves[:min(limit, len(source.moves))] {
		status := task.moveProgress
		if status.Status != moveRunning {
			status.TaskHandle = ""
		}
		results = append(results, status)
	}
	return listMoveOutput{Results: results}, nil
}

func (s *service) cancelMessageMove(request *http.Request, input moveHandle) (cancelMoveOutput, *apiError) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	for key, source := range s.store.queues {
		if key.region != awshttp.Region(request) {
			continue
		}
		for _, task := range source.moves {
			if task.TaskHandle == input.TaskHandle {
				if task.Status != moveRunning {
					return cancelMoveOutput{}, &apiError{"InvalidParameterValue", "Only running tasks can be cancelled.", 400}
				}
				task.Status = "CANCELLED"
				close(task.stop)
				return cancelMoveOutput{Moved: task.Moved}, nil
			}
		}
	}
	return cancelMoveOutput{}, &apiError{"ResourceNotFoundException", "Task does not exist.", 400}
}
