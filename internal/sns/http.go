package sns

import (
	"crypto/rand"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/romycode/ginistack/internal/awshttp"
	"github.com/romycode/ginistack/internal/sqs"
)

const (
	QueryVersion    = "2010-03-31"
	namespace       = "http://sns.amazonaws.com/doc/" + QueryVersion + "/"
	localAccountID  = awshttp.AccountID
	localSNSUserARN = "arn:aws:iam::" + localAccountID + ":user/ginistack-local"
)

type service struct {
	mu        sync.Mutex
	topics    map[string]*topic
	subs      map[string]*subscription
	deliver   func(sqs.Notification)
	signing   func() (*signer, error)
	cursorKey [32]byte
}

func NewHandler(deliver func(sqs.Notification)) http.Handler {
	s := &service{topics: make(map[string]*topic), subs: make(map[string]*subscription), deliver: deliver, signing: sync.OnceValues(newSigner)}
	rand.Read(s.cursorKey[:])
	return s
}

type apiError struct {
	Code    string
	Message string
	status  int
}

func invalid(message string) *apiError {
	return &apiError{"InvalidParameter", "Invalid parameter: " + message, http.StatusBadRequest}
}

var topicNotFound = &apiError{"NotFound", "Topic does not exist", http.StatusNotFound}

func (s *service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == SigningCertPath {
		s.writeCertificate(w)
		return
	}
	if !awshttp.AcceptBody(w, r) {
		return
	}
	requestID := uuid()
	w.Header().Set("x-amzn-RequestId", requestID)
	w.Header().Set("Content-Type", "text/xml")
	if awshttp.ParseForm(r) != nil || !MatchesQuery(r) {
		writeError(w, requestID, invalid("Version"))
		return
	}
	action := r.Form.Get("Action")
	region := awshttp.Region(r)
	if action == "Unsubscribe" && r.Form.Get("Version") == "" {
		parts := strings.SplitN(r.Form.Get("SubscriptionArn"), ":", 7)
		if len(parts) == 7 {
			region = parts[3]
		}
	}
	var result any
	var failure *apiError
	switch action {
	case "Publish":
		result, failure = s.publish(region, requestOrigin(r), r.Form)
	case "PublishBatch":
		result, failure = s.publishBatch(region, requestOrigin(r), r.Form)
	default:
		result, failure = s.execute(region, action, r.Form)
	}
	if failure != nil {
		writeError(w, requestID, failure)
		return
	}
	enc := xml.NewEncoder(w)
	start := xml.StartElement{Name: xml.Name{Space: namespace, Local: action + "Response"}}
	_ = enc.EncodeToken(start)
	if result != nil {
		_ = enc.EncodeElement(result, xml.StartElement{Name: xml.Name{Local: action + "Result"}})
	}
	_ = enc.EncodeElement(struct {
		RequestID string `xml:"RequestId"`
	}{requestID}, xml.StartElement{Name: xml.Name{Local: "ResponseMetadata"}})
	_ = enc.EncodeToken(start.End())
	_ = enc.Flush()
}

// MatchesQuery identifies SNS Query requests and the unsubscribe link in notifications.
// The caller has already parsed the form.
func MatchesQuery(r *http.Request) bool {
	version := r.Form.Get("Version")
	return version == QueryVersion ||
		(version == "" && r.Method == http.MethodGet && r.Form.Get("Action") == "Unsubscribe")
}

func (s *service) execute(region, action string, form url.Values) (any, *apiError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch action {
	case "CreateTopic":
		return s.createTopic(region, form)
	case "ListTopics":
		return s.listTopics(region, form.Get("NextToken"))
	case "GetTopicAttributes", "SetTopicAttributes", "DeleteTopic":
		return s.topicOperation(region, action, form)
	case "ListTagsForResource", "TagResource", "UntagResource":
		return s.tagOperation(region, action, form)
	case "Subscribe":
		return s.subscribe(region, form)
	case "AddPermission", "RemovePermission":
		return s.changePermission(region, action, form)
	case "GetSubscriptionAttributes", "SetSubscriptionAttributes", "Unsubscribe":
		return s.subscriptionOperation(region, action, form)
	case "ListSubscriptions", "ListSubscriptionsByTopic":
		return s.listSubscriptions(region, action, form)
	default:
		return nil, &apiError{"NotImplemented", "Ginistack does not implement this SNS action.", http.StatusNotImplemented}
	}
}

func writeError(w http.ResponseWriter, requestID string, failure *apiError) {
	w.WriteHeader(failure.status)
	response := struct {
		XMLName   xml.Name
		Error     struct{ Type, Code, Message string }
		RequestID string `xml:"RequestId"`
	}{XMLName: xml.Name{Space: namespace, Local: "ErrorResponse"}, RequestID: requestID}
	response.Error.Type = "Sender"
	response.Error.Code = failure.Code
	response.Error.Message = failure.Message
	_ = xml.NewEncoder(w).Encode(response)
}

func uuid() string {
	var value [16]byte
	rand.Read(value[:])
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[:4], value[4:6], value[6:8], value[8:10], value[10:])
}

type entry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type attributesResult struct {
	Attributes []entry `xml:"Attributes>entry"`
}

func listEntries(values map[string]string) []entry {
	entries := make([]entry, 0, len(values))
	for key, value := range values {
		entries = append(entries, entry{key, value})
	}
	slices.SortFunc(entries, func(a, b entry) int { return strings.Compare(a.Key, b.Key) })
	return entries
}

func queryMap(form url.Values, prefix, keyField, valueField string) (map[string]string, *apiError) {
	type pair struct{ key, value *string }
	pairs := make(map[int]*pair)
	for param := range form {
		tail, present := strings.CutPrefix(param, prefix+".")
		if !present {
			continue
		}
		index, field, found := strings.Cut(tail, ".")
		n, err := strconv.Atoi(index)
		if !found || err != nil || n < 1 {
			return nil, invalid(prefix)
		}
		item := pairs[n]
		if item == nil {
			item = new(pair)
			pairs[n] = item
		}
		value := form.Get(param)
		switch field {
		case keyField:
			item.key = &value
		case valueField:
			item.value = &value
		default:
			return nil, invalid(prefix)
		}
	}
	values := make(map[string]string, len(pairs))
	for _, item := range pairs {
		if item.key == nil || item.value == nil {
			return nil, invalid(prefix)
		}
		if _, duplicate := values[*item.key]; duplicate {
			return nil, invalid(prefix)
		}
		values[*item.key] = *item.value
	}
	return values, nil
}
