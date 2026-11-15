package sqs

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

type apiError struct {
	code    string
	message string
	status  int
}

var (
	missingQueue   = &apiError{"AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.", 400}
	invalidReceipt = &apiError{"ReceiptHandleIsInvalid", "The input receipt handle is invalid.", 400}
	notImplemented = &apiError{"NotImplemented", "Ginistack does not implement this SQS feature.", 501}
)

const maxRequestBody = 8 << 20

type router struct {
	operations map[string]http.HandlerFunc
}

func NewHandler() http.Handler {
	api := &service{store: store{queues: make(map[queueKey]*queue), deleted: make(map[queueKey]time.Time), incarnations: make(map[string]deletedIncarnation)}, cursorKey: []byte(token())}
	r := &router{operations: make(map[string]http.HandlerFunc)}
	r.register("CreateQueue", api.createQueue)
	r.register("GetQueueUrl", api.getQueueURL)
	r.register("DeleteQueue", api.deleteQueue)
	r.register("ListQueues", api.listQueues)
	r.register("PurgeQueue", api.purgeQueue)
	r.register("ListDeadLetterSourceQueues", api.listDeadLetterSources)
	r.register("StartMessageMoveTask", api.startMessageMove)
	r.register("ListMessageMoveTasks", api.listMessageMoves)
	r.register("CancelMessageMoveTask", api.cancelMessageMove)
	r.register("GetQueueAttributes", api.getQueueAttributes)
	r.register("SetQueueAttributes", api.setQueueAttributes)
	r.register("AddPermission", api.addPermission)
	r.register("RemovePermission", api.removePermission)
	r.register("ListQueueTags", api.listQueueTags)
	r.register("TagQueue", api.tagQueue)
	r.register("UntagQueue", api.untagQueue)
	r.register("SendMessage", api.sendMessage)
	r.register("ReceiveMessage", api.receiveMessage)
	r.register("DeleteMessage", api.deleteMessage)
	r.register("ChangeMessageVisibility", api.changeMessageVisibility)
	r.register("SendMessageBatch", api.sendMessageBatch)
	r.register("DeleteMessageBatch", api.deleteMessageBatch)
	r.register("ChangeMessageVisibilityBatch", api.changeMessageVisibilityBatch)
	return r
}

// register uses one typed operation for both wire protocols.
func (r *router) register[Input any, Output any](name string, run func(*http.Request, Input) (Output, *apiError)) {
	r.operations["AmazonSQS."+name] = func(w http.ResponseWriter, request *http.Request) {
		var input Input
		query := isQueryRequest(request)
		writeFailure := writeError
		if query {
			writeFailure = writeQueryError
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, maxRequestBody))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeFailure(w, decodeError(err))
			return
		}
		if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
			writeFailure(w, decodeError(err))
			return
		}
		output, failure := run(request, input)
		if failure != nil {
			writeFailure(w, failure)
			return
		}
		if query {
			writeQueryResult(w, name, output)
			return
		}
		_ = json.NewEncoder(w).Encode(output)
	}
}

func (r *router) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("x-amzn-RequestId", token())
	if isQueryRequest(request) {
		r.serveQuery(w, request)
		return
	}
	mediaType, _, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if mediaType != "application/x-amz-json-1.0" {
		w.Header().Set("Content-Type", "text/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, "<UnknownOperationException/>\n")
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	target := request.Header.Get("X-Amz-Target")
	if request.Method != http.MethodPost || !strings.HasPrefix(target, "AmazonSQS.") {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, "{\"__type\":\"com.amazon.coral.service#UnknownOperationException\"}\n")
		return
	}
	operation := r.operations[target]
	if operation == nil {
		writeError(w, notImplemented)
		return
	}
	operation(w, request)
}

func isQueryRequest(request *http.Request) bool {
	mediaType, _, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
	return (request.Method == http.MethodPost && mediaType == "application/x-www-form-urlencoded") ||
		(request.Method == http.MethodGet && request.URL.Query().Has("Action"))
}

func decodeError(err error) *apiError {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return &apiError{"RequestTooLong", "The request body exceeds the local limit.", 413}
	}
	return &apiError{"SerializationException", "Invalid or unsupported SQS request fields.", 400}
}

func writeError(w http.ResponseWriter, failure *apiError) {
	code := failure.code
	switch failure.code {
	case "AWS.SimpleQueueService.NonExistentQueue":
		w.Header().Set("x-amzn-query-error", failure.code+";Sender")
		code = "com.amazonaws.sqs#QueueDoesNotExist"
	case "InvalidParameterValue":
		w.Header().Set("x-amzn-query-error", failure.code+";Sender")
		code = "com.amazon.coral.service#InvalidParameterValueException"
	default:
		w.Header().Set("x-amzn-errortype", failure.code)
	}
	w.WriteHeader(failure.status)
	_ = json.NewEncoder(w).Encode(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{Type: code, Message: failure.message})
}
