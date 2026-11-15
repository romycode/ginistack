// Package awshttp implements the shared AWS HTTP transport conventions.
package awshttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
)

// MaxRequestBody is the wire limit observed in AWS Emulator.
const MaxRequestBody = 1600 * 1024

const AccountID = "000000000000"

var regionPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

func IsQuery(request *http.Request) bool {
	mediaType, _, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
	return (request.Method == http.MethodPost && mediaType == "application/x-www-form-urlencoded") ||
		(request.Method == http.MethodGet && request.URL.Query().Has("Action"))
}

type formErrorKey struct{}

// ParseForm preserves a parsing error when both the gateway and a service inspect
// the same form. The second net/http ParseForm call may return nil for a partial form.
func ParseForm(request *http.Request) error {
	if err, ok := request.Context().Value(formErrorKey{}).(error); ok {
		return err
	}
	err := request.ParseForm()
	if err != nil {
		*request = *request.WithContext(context.WithValue(request.Context(), formErrorKey{}, err))
	}
	return err
}

func AcceptBody(w http.ResponseWriter, request *http.Request) bool {
	if request.ContentLength > MaxRequestBody {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return false
	}
	if request.ContentLength >= 0 {
		return true
	}

	// Count streamed bodies before dispatch, even if the payload is malformed.
	limited := http.MaxBytesReader(w, request.Body, MaxRequestBody)
	defer limited.Close()
	payload, err := io.ReadAll(limited)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		return false
	}
	request.Body = io.NopCloser(bytes.NewReader(payload))
	request.ContentLength = int64(len(payload))
	return true
}

func Region(request *http.Request) string {
	_, credentials, present := strings.Cut(request.Header.Get("Authorization"), "Credential=")
	if present {
		parts := strings.SplitN(credentials, "/", 5)
		if len(parts) == 5 && regionPattern.MatchString(parts[2]) {
			return parts[2]
		}
	}
	return "us-east-1"
}
