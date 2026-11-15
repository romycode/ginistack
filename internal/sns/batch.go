package sns

import (
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const (
	maxBatchEntries  = 10
	maxBatchIDLength = 80
)

type batchEntryResult struct {
	ID string `xml:"Id"`
	publishResult
}

type batchResult struct {
	Successful []batchEntryResult `xml:"Successful>member"`
	Failed     struct{}
}

func (s *service) publishBatch(region, origin string, form url.Values) (batchResult, *apiError) {
	entries, failure := batchEntries(form)
	if failure != nil {
		return batchResult{}, failure
	}
	inputs := make([]publishInput, len(entries))
	ids := make(map[string]struct{}, len(entries))
	for index, entry := range entries {
		id := entry.Get("Id")
		if len(id) > maxBatchIDLength {
			return batchResult{}, batchError("InvalidBatchEntryId", "The Id of a batch entry in the batch request is too long: "+id)
		}
		if !topicName.MatchString(id) {
			return batchResult{}, batchError("InvalidBatchEntryId", "The Id of a batch entry in the batch request contains an impermissible character: "+id)
		}
		if _, duplicate := ids[id]; duplicate {
			return batchResult{}, batchError("BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.")
		}
		ids[id] = struct{}{}
		input, failure := preparePublish(entry)
		if failure != nil {
			return batchResult{}, failure
		}
		inputs[index] = input
	}

	arn := form.Get("TopicArn")
	if failure := checkTopicARN(region, arn); failure != nil {
		return batchResult{}, failure
	}
	published, failure := s.publishMessages(arn, origin, inputs, true)
	if failure != nil {
		return batchResult{}, failure
	}
	result := batchResult{Successful: make([]batchEntryResult, len(published))}
	for index, item := range published {
		result.Successful[index] = batchEntryResult{ID: entries[index].Get("Id"), publishResult: item}
	}
	return result, nil
}

func batchEntries(form url.Values) ([]url.Values, *apiError) {
	const prefix = "PublishBatchRequestEntries.member."
	rows := make(map[int]url.Values)
	for path, values := range form {
		tail, ok := strings.CutPrefix(path, prefix)
		if !ok {
			continue
		}
		index, field, found := strings.Cut(tail, ".")
		n, err := strconv.Atoi(index)
		if err != nil || !found || n < 1 || field == "" || strconv.Itoa(n) != index {
			return nil, invalid("PublishBatchRequestEntries")
		}
		if rows[n] == nil {
			rows[n] = make(url.Values)
			if len(rows) > maxBatchEntries {
				return nil, batchError("TooManyEntriesInBatchRequest", "The batch request contains more entries than permissible.")
			}
		}
		rows[n][field] = values
	}
	if len(rows) == 0 {
		return nil, batchError("EmptyBatchRequest", "The batch request doesn't contain any entries.")
	}
	entries := make([]url.Values, 0, len(rows))
	for _, index := range slices.Sorted(maps.Keys(rows)) {
		entries = append(entries, rows[index])
	}
	return entries, nil
}

func batchError(code, message string) *apiError {
	return &apiError{code, message, http.StatusBadRequest}
}
