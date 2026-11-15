package sqs

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/romycode/ginistack/internal/awshttp"
)

type apiError struct {
	code    string
	message string
	status  int
}

var (
	missingQueue       = &apiError{"AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.", 400}
	invalidReceipt     = &apiError{"ReceiptHandleIsInvalid", "The input receipt handle is invalid.", 400}
	notImplemented     = &apiError{"NotImplemented", "Ginistack does not implement this SQS feature.", 501}
	serializationError = &apiError{"SerializationException", "Invalid or unsupported SQS request fields.", 400}
)

type Handler struct {
	operations map[string]http.HandlerFunc
	store      *store
}

func NewHandler() *Handler {
	api := &service{store: store{bodyCipher: newBodyCipher(), queues: make(map[queueKey]*queue), deleted: make(map[queueKey]time.Time), incarnations: make(map[string]deletedIncarnation)}, cursorKey: []byte(token())}
	r := &Handler{operations: make(map[string]http.HandlerFunc), store: &api.store}
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
func (r *Handler) register[Input any, Output any](name string, run func(*http.Request, Input) (Output, *apiError)) {
	r.operations["AmazonSQS."+name] = func(w http.ResponseWriter, request *http.Request) {
		var input Input
		query := awshttp.IsQuery(request)
		writeFailure := writeError
		if query {
			writeFailure = writeQueryError
		}
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeFailure(w, serializationError)
			return
		}
		if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
			writeFailure(w, serializationError)
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

func (r *Handler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method == http.MethodGet && request.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !awshttp.AcceptBody(w, request) {
		return
	}
	w.Header().Set("x-amzn-RequestId", token())
	if awshttp.IsQuery(request) {
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
