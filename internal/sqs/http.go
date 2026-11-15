package sqs

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type apiError struct {
	code    string
	message string
	status  int
}

var (
	missingQueue   = &apiError{"QueueDoesNotExist", "The specified queue does not exist.", 400}
	invalidReceipt = &apiError{"ReceiptHandleIsInvalid", "The input receipt handle is invalid.", 400}
	notImplemented = &apiError{"NotImplemented", "Ginistack does not implement this SQS feature.", 501}
)

const maxRequestBody = 8 << 20

type router struct {
	operations map[string]http.HandlerFunc
}

func NewHandler() http.Handler {
	api := &service{store: store{queues: make(map[string]*queue)}}
	r := &router{operations: make(map[string]http.HandlerFunc)}
	r.register("CreateQueue", api.createQueue)
	r.register("GetQueueUrl", api.getQueueURL)
	r.register("DeleteQueue", api.deleteQueue)
	r.register("GetQueueAttributes", api.getQueueAttributes)
	r.register("SetQueueAttributes", api.setQueueAttributes)
	r.register("ListQueueTags", api.listQueueTags)
	r.register("SendMessage", api.sendMessage)
	r.register("ReceiveMessage", api.receiveMessage)
	r.register("DeleteMessage", api.deleteMessage)
	r.register("ChangeMessageVisibility", api.changeMessageVisibility)
	r.register("SendMessageBatch", api.sendMessageBatch)
	r.register("DeleteMessageBatch", api.deleteMessageBatch)
	r.register("ChangeMessageVisibilityBatch", api.changeMessageVisibilityBatch)
	return r
}

// register adapts a typed SQS operation to the AWS JSON protocol.
func (r *router) register[Input any, Output any](name string, run func(*http.Request, Input) (Output, *apiError)) {
	r.operations["AmazonSQS."+name] = func(w http.ResponseWriter, request *http.Request) {
		var input Input
		decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, maxRequestBody))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeDecodeError(w, err)
			return
		}
		if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
			writeDecodeError(w, err)
			return
		}
		output, failure := run(request, input)
		if failure != nil {
			writeError(w, failure)
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
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-RequestId", token())
	target := request.Header.Get("X-Amz-Target")
	if request.Method != http.MethodPost || !strings.HasPrefix(target, "AmazonSQS.") {
		writeError(w, &apiError{"UnknownOperationException", "Expected an AWS JSON SQS operation.", 400})
		return
	}
	operation := r.operations[target]
	if operation == nil {
		writeError(w, notImplemented)
		return
	}
	operation(w, request)
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, &apiError{"RequestTooLong", "The request body exceeds the local limit.", 413})
		return
	}
	writeError(w, &apiError{"SerializationException", "Invalid or unsupported SQS request fields.", 400})
}

func writeError(w http.ResponseWriter, failure *apiError) {
	w.Header().Set("x-amzn-errortype", failure.code)
	w.WriteHeader(failure.status)
	_ = json.NewEncoder(w).Encode(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{Type: failure.code, Message: failure.message})
}
