package sqs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/romycode/ginistack/internal/policy"
)

const (
	maxPolicySize       = 8192
	maxPolicyStatements = 20
)

var permissionActions = []string{
	"AddPermission", "CancelMessageMoveTask", "ChangeMessageVisibility", "CreateQueue",
	"DeleteMessage", "DeleteQueue", "GetQueueAttributes", "GetQueueUrl",
	"ListDeadLetterSourceQueues", "ListMessageMoveTasks", "ListQueues", "ListQueueTags",
	"PurgeQueue", "ReceiveMessage", "RemovePermission", "SendMessage", "SetQueueAttributes",
	"StartMessageMoveTask", "TagQueue", "UntagQueue",
}

func parseQueuePolicy(raw string) (*policy.Document, *apiError) {
	if raw == "" {
		return nil, nil
	}
	if len(raw) > maxPolicySize {
		return nil, &apiError{"OverLimit", "Submitted policy is over max allowed size.", 400}
	}
	document, err := policy.Parse(raw)
	if err != nil {
		return nil, invalidQueueAttribute("Policy")
	}
	return document, validateQueuePolicy(document)
}

func validateQueuePolicy(document *policy.Document) *apiError {
	if n := len(document.Statement.Values); n > maxPolicyStatements {
		return &apiError{"OverLimit", fmt.Sprintf("%d Statements were found, maximum allowed is %d. ", n, maxPolicyStatements), 400}
	}
	for _, statement := range document.Statement.Values {
		if statement.Principal == nil && statement.NotPrincipal == nil {
			return invalidQueueAttribute("Policy")
		}
		actions := append(slices.Clone(statement.Action.Values), statement.NotAction.Values...)
		if n := len(actions); n > 7 {
			return &apiError{"OverLimit", fmt.Sprintf("%d actions were found in a statement from the submitted policy, max allowed is 7", n), 400}
		}
		for _, action := range actions {
			if !validPolicyAction(action) {
				return invalidSQSAction(action)
			}
		}
		conditions := 0
		for _, keys := range statement.Condition {
			conditions += len(keys)
		}
		if conditions > 10 {
			return &apiError{"OverLimit", fmt.Sprintf("%d conditions were found in a statement from the submitted policy, max allowed is 10", conditions), 400}
		}
		for _, principal := range []*policy.Principal{statement.Principal, statement.NotPrincipal} {
			if principal != nil {
				if n := len(principal.AWS.Values) + len(principal.Service.Values); n > 50 {
					return &apiError{"InvalidAttributeValue", fmt.Sprintf("Invalid value for the parameter %d principals were found in a statement from the submitted policy, max allowed is 50.", n), 400}
				}
			}
		}
	}
	return nil
}

func validPolicyAction(action string) bool {
	if action == "*" {
		return true
	}
	service, name, ok := strings.Cut(action, ":")
	return ok && strings.EqualFold(service, "sqs") && (name == "*" || validPermissionAction(name))
}

func validPermissionAction(name string) bool {
	return slices.ContainsFunc(permissionActions, func(candidate string) bool { return strings.EqualFold(name, candidate) })
}

func invalidSQSAction(action string) *apiError {
	return &apiError{"InvalidParameterValue", "Value " + action + " for parameter ActionName is invalid. Reason: Please refer to the appropriate WSDL for a list of valid actions.", 400}
}

type permissionLabel struct {
	queueInput
	Label string
}

type addPermissionInput struct {
	permissionLabel
	AWSAccountIDs []string `json:"AWSAccountIds"`
	Actions       []string
}

func (s *service) addPermission(request *http.Request, input addPermissionInput) (empty, *apiError) {
	key, failure := queueName(request, input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if input.Label == "" {
		return empty{}, missingParameter("Label")
	}
	if len(input.AWSAccountIDs) == 0 {
		return empty{}, missingParameter("AWSAccountId")
	}
	if len(input.Actions) == 0 {
		return empty{}, missingParameter("ActionName")
	}
	principals := make([]string, 0, len(input.AWSAccountIDs))
	for _, account := range input.AWSAccountIDs {
		if !policy.IsAccountID(account) {
			return empty{}, &apiError{"InvalidParameterValue", "Invalid AWS account ID.", 400}
		}
		principals = append(principals, "arn:aws:iam::"+account+":root")
	}
	actions := make([]string, 0, len(input.Actions))
	for _, action := range input.Actions {
		if action != "*" && !validPermissionAction(action) {
			return empty{}, invalidSQSAction("SQS:" + action)
		}
		actions = append(actions, "SQS:"+action)
	}

	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	q := s.store.queues[key]
	if q == nil {
		return empty{}, missingQueue
	}
	document := policy.Document{Version: "2012-10-17", Statement: policy.NewList[policy.Statement]()}
	if q.accessPolicy != nil {
		document = *q.accessPolicy
	}
	if slices.ContainsFunc(document.Statement.Values, func(statement policy.Statement) bool { return statement.SID == input.Label }) {
		return empty{}, &apiError{"InvalidParameterValue", "Value " + input.Label + " for parameter Label is invalid. Reason: label already exists on existing policy.", 400}
	}
	statement := policy.Statement{
		SID: input.Label, Effect: "Allow", Principal: &policy.Principal{AWS: policy.NewList(principals...)},
		Action: policy.NewList(actions...), Resource: policy.NewList(q.arn),
	}
	document.Statement.Values = append(slices.Clone(document.Statement.Values), statement)
	if failure := validateQueuePolicy(&document); failure != nil {
		return empty{}, failure
	}
	payload, _ := json.Marshal(document)
	if len(payload) > maxPolicySize {
		return empty{}, &apiError{"OverLimit", "Submitted policy is over max allowed size.", 400}
	}
	q.accessPolicy = &document
	return empty{}, nil
}

func (s *service) removePermission(request *http.Request, input permissionLabel) (empty, *apiError) {
	key, failure := queueName(request, input.queueInput)
	if failure != nil {
		return empty{}, failure
	}
	if input.Label == "" {
		return empty{}, missingParameter("Label")
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	q := s.store.queues[key]
	if q == nil {
		return empty{}, missingQueue
	}
	if q.accessPolicy != nil {
		document := *q.accessPolicy
		index := slices.IndexFunc(document.Statement.Values, func(statement policy.Statement) bool { return statement.SID == input.Label })
		if index != -1 {
			document.Statement.Values = slices.Delete(slices.Clone(document.Statement.Values), index, index+1)
			if len(document.Statement.Values) == 0 {
				q.accessPolicy = nil
			} else {
				q.accessPolicy = &document
			}
			return empty{}, nil
		}
	}
	return empty{}, &apiError{"InvalidParameterValue", "Value " + input.Label + " for parameter Label is invalid. Reason: can't find label on existing policy.", 400}
}

func (q *queue) checkSendPolicy(secureTransport bool) *apiError {
	if q.accessPolicy == nil {
		return nil
	}
	context := map[string]string{
		"aws:PrincipalArn": localUserARN, "aws:PrincipalAccount": accountID,
		"aws:SecureTransport": strconv.FormatBool(secureTransport),
	}
	for key, value := range q.tags {
		context["aws:ResourceTag/"+key] = value
	}
	request := policy.Request{
		PrincipalARN: localUserARN, PrincipalAccount: accountID, Action: "sqs:SendMessage", Resource: q.arn, Context: context,
	}
	if q.accessPolicy.Evaluate(request) == policy.Deny {
		return &apiError{"AccessDenied", "User: " + localUserARN + " is not authorized to perform: sqs:sendmessage on resource: " + q.arn + " with an explicit deny in a resource-based policy", http.StatusForbidden}
	}
	return nil
}
