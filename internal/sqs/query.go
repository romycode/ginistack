package sqs

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const queryNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"

func (r *router) serveQuery(w http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(w, request.Body, maxRequestBody)
	if request.ParseForm() != nil {
		writeQueryError(w, &apiError{"InvalidParameterValue", "Invalid AWS Query form.", 400})
		return
	}
	action := request.Form.Get("Action")
	if action == "GetCallerIdentity" && request.Form.Get("Version") == "2011-06-15" {
		writeCallerIdentity(w)
		return
	}
	operation := r.operations["AmazonSQS."+action]
	if operation == nil {
		writeQueryError(w, &apiError{"InvalidAction", "The action " + action + " is not valid for this endpoint.", 400})
		return
	}
	input, err := queryInput(request.Form, action)
	if err != nil {
		writeQueryError(w, &apiError{"InvalidParameterValue", err.Error(), 400})
		return
	}
	if _, present := input["QueueUrl"]; !present && (strings.HasPrefix(request.URL.Path, "/"+accountID+"/") || strings.HasPrefix(request.URL.Path, "/"+requestRegion(request)+"/"+accountID+"/")) {
		host := url.URL{Scheme: "http", Host: request.Host, Path: request.URL.Path}
		if request.TLS != nil {
			host.Scheme = "https"
		}
		input["QueueUrl"] = host.String()
	}
	payload, err := json.Marshal(input)
	if err != nil {
		writeQueryError(w, &apiError{"InvalidParameterValue", "Invalid AWS Query parameter.", 400})
		return
	}
	request.Body = io.NopCloser(bytes.NewReader(payload))
	request.Header.Set("X-Amz-Target", "AmazonSQS."+action)
	operation(w, request)
}

// This endpoint reports the local principal shared by SQS and STS.
func writeCallerIdentity(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/xml")
	type identity struct {
		XMLName   xml.Name `xml:"https://sts.amazonaws.com/doc/2011-06-15/ GetCallerIdentityResponse"`
		Account   string   `xml:"GetCallerIdentityResult>Account"`
		ARN       string   `xml:"GetCallerIdentityResult>Arn"`
		UserID    string   `xml:"GetCallerIdentityResult>UserId"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}
	_ = xml.NewEncoder(w).Encode(identity{
		Account: accountID, ARN: "arn:aws:iam::" + accountID + ":user/ginistack-local",
		UserID: localUserID, RequestID: w.Header().Get("x-amzn-RequestId"),
	})
}

func queryInput(values url.Values, action string) (map[string]any, error) {
	result := make(map[string]any)
	groups := make(map[string]url.Values)
	for key, values := range values {
		if prefix, tail, nested := strings.Cut(key, "."); nested {
			if groups[prefix] == nil {
				groups[prefix] = make(url.Values)
			}
			groups[prefix][tail] = values
			continue
		}
		switch key {
		case "Action", "Version":
		case "MaxResults", "MaxNumberOfMessages", "DelaySeconds", "VisibilityTimeout", "WaitTimeSeconds", "MaxNumberOfMessagesPerSecond":
			number, err := strconv.Atoi(values[0])
			if err != nil {
				return nil, fmt.Errorf("Value for parameter %s is invalid.", key)
			}
			result[key] = number
		default:
			result[key] = values[0]
		}
	}
	for prefix, members := range groups {
		rows := queryRows(members)
		switch prefix {
		case "Attribute", "Tag", "MessageAttribute", "MessageSystemAttribute":
			attr := make(map[string]any, len(rows))
			for _, row := range rows {
				switch prefix {
				case "Attribute":
					attr[row.Get("Name")] = row.Get("Value")
				case "Tag":
					attr[row.Get("Key")] = row.Get("Value")
				default:
					value := make(map[string]string)
					for key, values := range row {
						if field, ok := strings.CutPrefix(key, "Value."); ok {
							value[field] = values[0]
						}
					}
					attr[row.Get("Name")] = value
				}
			}
			field := prefix + "s"
			result[field] = attr
		case "AttributeName", "MessageAttributeName", "MessageSystemAttributeName", "TagKey":
			values := make([]string, 0, len(rows))
			for _, row := range rows {
				values = append(values, row.Get(""))
			}
			result[prefix+"s"] = values
		default:
			if prefix == action+"RequestEntry" {
				entries := make([]map[string]any, 0, len(rows))
				for _, row := range rows {
					entry, err := queryInput(row, "")
					if err != nil {
						return nil, err
					}
					entries = append(entries, entry)
				}
				result["Entries"] = entries
			}
		}
	}
	return result, nil
}

func queryRows(values url.Values) []url.Values {
	indexed := make(map[int]url.Values)
	for key, values := range values {
		index, field, _ := strings.Cut(key, ".")
		number, err := strconv.Atoi(index)
		if err != nil || number < 1 {
			continue
		}
		if indexed[number] == nil {
			indexed[number] = make(url.Values)
		}
		indexed[number][field] = values
	}
	rows := make([]url.Values, 0, len(indexed))
	for _, index := range slices.Sorted(maps.Keys(indexed)) {
		rows = append(rows, indexed[index])
	}
	return rows
}

type queryFault struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Namespace string   `xml:"xmlns,attr"`
	Type      string   `xml:"Error>Type"`
	Code      string   `xml:"Error>Code"`
	Message   string   `xml:"Error>Message"`
	RequestID string   `xml:"RequestId"`
}

func writeQueryError(w http.ResponseWriter, failure *apiError) {
	kind := "Sender"
	if failure.status >= 500 {
		kind = "Receiver"
	}
	message := failure.message
	if failure == missingQueue {
		message = "The specified queue does not exist for this wsdl version."
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(failure.status)
	_ = xml.NewEncoder(w).Encode(queryFault{
		Namespace: queryNamespace, Type: kind, Code: failure.code, Message: message,
		RequestID: w.Header().Get("x-amzn-RequestId"),
	})
}

func writeQueryResult(w http.ResponseWriter, action string, output any) {
	payload, err := json.Marshal(output)
	if err != nil {
		writeQueryError(w, &apiError{"InternalError", "Unable to encode the result.", 500})
		return
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		writeQueryError(w, &apiError{"InternalError", "Unable to encode the result.", 500})
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	encoder := xml.NewEncoder(w)
	root := xml.StartElement{Name: xml.Name{Local: action + "Response"}, Attr: []xml.Attr{{Name: xml.Name{Local: "xmlns"}, Value: queryNamespace}}}
	_ = encoder.EncodeToken(root)
	if action != "DeleteQueue" && action != "DeleteMessage" && action != "ChangeMessageVisibility" && action != "PurgeQueue" && action != "TagQueue" && action != "UntagQueue" && action != "SetQueueAttributes" {
		encodeQueryField(encoder, action, action+"Result", fields)
	}
	encodeQueryField(encoder, action, "ResponseMetadata", map[string]any{"RequestId": w.Header().Get("x-amzn-RequestId")})
	_ = encoder.EncodeToken(root.End())
	_ = encoder.Flush()
}

func encodeQueryField(e *xml.Encoder, action, field string, value any) {
	switch field {
	case "Messages", "QueueUrls", "queueUrls", "Successful", "Failed", "Results":
		name := map[string]string{"Messages": "Message", "QueueUrls": "QueueUrl", "queueUrls": "QueueUrl", "Results": "Result", "Successful": action + "ResultEntry", "Failed": "BatchResultErrorEntry"}[field]
		for _, item := range value.([]any) {
			encodeQueryField(e, action, name, item)
		}
		return
	case "Attributes", "MessageAttributes", "Tags":
		name := strings.TrimSuffix(field, "s")
		keyField := "Name"
		if field == "Tags" {
			keyField = "Key"
		}
		items := value.(map[string]any)
		for _, key := range slices.Sorted(maps.Keys(items)) {
			encodeQueryField(e, action, name, map[string]any{keyField: key, "Value": items[key]})
		}
		return
	}
	start := xml.StartElement{Name: xml.Name{Local: field}}
	if items, ok := value.(map[string]any); ok {
		_ = e.EncodeToken(start)
		for _, key := range slices.Sorted(maps.Keys(items)) {
			encodeQueryField(e, action, key, items[key])
		}
		_ = e.EncodeToken(start.End())
	} else {
		_ = e.EncodeElement(value, start)
	}
}
