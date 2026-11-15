package sns

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/romycode/ginistack/internal/policy"
)

func defaultTopicPolicy(arn string) policy.Document {
	return policy.Document{
		Version: "2008-10-17", ID: "__default_policy_ID",
		Statement: policy.NewList(policy.Statement{
			SID: "__default_statement_ID", Effect: "Allow",
			Principal: &policy.Principal{AWS: policy.NewList("*")},
			Action: policy.NewList("SNS:GetTopicAttributes", "SNS:SetTopicAttributes", "SNS:AddPermission",
				"SNS:RemovePermission", "SNS:DeleteTopic", "SNS:Subscribe", "SNS:ListSubscriptionsByTopic", "SNS:Publish"),
			Resource: policy.NewList(arn),
			Condition: map[string]map[string]policy.List[string]{
				"StringEquals": {"AWS:SourceOwner": policy.NewList(localAccountID)},
			},
		}),
	}
}

func encodeTopicPolicy(document policy.Document) string {
	data, _ := json.Marshal(struct {
		Version   string `json:",omitempty"`
		ID        string `json:"Id,omitempty"`
		Statement []policy.Statement
	}{document.Version, document.ID, document.Statement.Values})
	return string(data)
}

func (s *service) changePermission(region, action string, form url.Values) (any, *apiError) {
	arn := form.Get("TopicArn")
	if failure := checkTopicARN(region, arn); failure != nil {
		return nil, failure
	}
	t := s.topics[arn]
	if t == nil {
		return nil, topicNotFound
	}
	label := form.Get("Label")
	if label == "" {
		return nil, invalid("Label")
	}
	document := defaultTopicPolicy(arn)
	if t.accessPolicy != nil {
		document = *t.accessPolicy
	}
	statements := document.Statement.Values
	index := slices.IndexFunc(statements, func(statement policy.Statement) bool { return statement.SID == label })
	if action == "RemovePermission" {
		if index == -1 {
			return nil, nil
		}
		document.Statement.Values = slices.Delete(slices.Clone(statements), index, index+1)
	} else {
		if index != -1 {
			return nil, invalid("Statement already exists")
		}
		accounts := permissionValues(form, "AWSAccountId.member.")
		actions := permissionValues(form, "ActionName.member.")
		if len(accounts) == 0 || len(actions) == 0 {
			return nil, invalid("AWSAccountId or ActionName")
		}
		for i, account := range accounts {
			if !policy.IsAccountID(account) {
				return nil, invalid("A provided account ID is not valid")
			}
			accounts[i] = "arn:aws:iam::" + account + ":root"
		}
		for i, action := range actions {
			switch action {
			case "GetTopicAttributes", "SetTopicAttributes", "AddPermission", "RemovePermission",
				"DeleteTopic", "Subscribe", "ListSubscriptionsByTopic", "Publish", "Receive", "*":
			default:
				return nil, invalid("Policy statement action out of service scope!")
			}
			actions[i] = "SNS:" + action
		}
		document.Statement.Values = append(slices.Clone(statements), policy.Statement{
			SID: label, Effect: "Allow", Principal: &policy.Principal{AWS: policy.NewList(accounts...)},
			Action: policy.NewList(actions...), Resource: policy.NewList(arn),
		})
	}
	if document.Statement.Values == nil {
		document.Statement.Values = []policy.Statement{}
	}
	t.accessPolicy = &document
	t.attributes["Policy"] = encodeTopicPolicy(document)
	return nil, nil
}

func permissionValues(form url.Values, prefix string) []string {
	values := make([]string, 0)
	for i := 1; ; i++ {
		key := prefix + strconv.Itoa(i)
		if !form.Has(key) {
			return values
		}
		values = append(values, form.Get(key))
	}
}

func (t *topic) authorize(arn, action string, context map[string]string) *apiError {
	if t.accessPolicy.Evaluate(policy.Request{
		PrincipalARN: localSNSUserARN, PrincipalAccount: localAccountID,
		Action: action, Resource: arn, Context: context,
	}) != policy.Deny {
		return nil
	}
	return &apiError{"AuthorizationError", "User: " + localSNSUserARN + " is not authorized to perform: " +
		strings.Replace(action, "sns:", "SNS:", 1) + " on resource: " + arn + " with an explicit deny in a resource-based policy", http.StatusForbidden}
}
