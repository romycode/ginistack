package policy

import "testing"

func TestResourcePolicyForServicePrincipal(t *testing.T) {
	t.Parallel()
	doc, err := Parse(`{
		"Statement": [
			{"Effect":"Allow","Principal":{"Service":"sns.amazonaws.com"},
			 "Action":"sqs:SendMessage","Resource":"arn:aws:sqs:us-west-2:000000000000:orders",
			 "Condition":{"ArnEquals":{"aws:SourceArn":"arn:aws:sns:us-west-2:000000000000:orders"}}},
			{"Effect":"Deny","Principal":{"Service":"sns.amazonaws.com"},
			 "Action":"sqs:SendMessage","Resource":"arn:aws:sqs:us-west-2:000000000000:orders",
			 "Condition":{"StringEquals":{"aws:SourceAccount":"111111111111"}}}
		]
	}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		context map[string]string
		want    Decision
	}{
		{"matching topic", map[string]string{"aws:SourceArn": "arn:aws:sns:us-west-2:000000000000:orders"}, Allow},
		{"different topic", map[string]string{"aws:SourceArn": "arn:aws:sns:us-west-2:000000000000:other"}, NoMatch},
		{"explicit deny wins", map[string]string{"aws:SourceArn": "arn:aws:sns:us-west-2:000000000000:orders", "aws:SourceAccount": "111111111111"}, Deny},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := Request{
				PrincipalService: "sns.amazonaws.com",
				Action:           "SQS:SendMessage",
				Resource:         "arn:aws:sqs:us-west-2:000000000000:orders",
				Context:          tc.context,
			}
			if got := doc.Evaluate(request); got != tc.want {
				t.Errorf("Evaluate(%+v) = %v, want %v", request, got, tc.want)
			}
		})
	}
}

func TestResourcePolicyDistinguishesS3ObjectsFromBucket(t *testing.T) {
	t.Parallel()
	doc, err := Parse(`{"Statement":{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::orders/reports/*"}}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		resource string
		want     Decision
	}{
		{"arn:aws:s3:::orders/reports/2026/november.csv", Allow},
		{"arn:aws:s3:::orders/reports/november.csv", Allow},
		{"arn:aws:s3:::orders", NoMatch},
		{"arn:aws:s3:::orders/invoices/november.csv", NoMatch},
	} {
		if got := doc.Evaluate(Request{Action: "s3:GetObject", Resource: tc.resource}); got != tc.want {
			t.Errorf("Evaluate(s3:GetObject, %q) = %v, want %v", tc.resource, got, tc.want)
		}
	}
}
