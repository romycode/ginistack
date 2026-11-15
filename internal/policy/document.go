package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

// List accepts the single-element and array forms used in policy documents.
type List[T any] struct {
	Values []T
	array  bool
}

func NewList[T any](values ...T) List[T] {
	return List[T]{Values: values, array: len(values) != 1}
}

func (l List[T]) IsZero() bool { return len(l.Values) == 0 }

func (l *List[T]) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return errors.New("expected a policy value or array")
	}
	l.array = data[0] == '['
	if l.array {
		return decodeStrict(data, &l.Values)
	}
	var value T
	if err := decodeStrict(data, &value); err != nil {
		return err
	}
	l.Values = []T{value}
	return nil
}

func (l List[T]) MarshalJSON() ([]byte, error) {
	if !l.array && len(l.Values) == 1 {
		return json.Marshal(l.Values[0])
	}
	return json.Marshal(l.Values)
}

type Principal struct {
	Any     bool         `json:"-"`
	AWS     List[string] `json:",omitzero"`
	Service List[string] `json:",omitzero"`
}

func (p *Principal) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte(`"*"`)) {
		*p = Principal{Any: true}
		return nil
	}
	type object Principal
	var value object
	if err := decodeStrict(data, &value); err != nil {
		return err
	}
	if len(value.AWS.Values)+len(value.Service.Values) == 0 {
		return errors.New("principal is empty")
	}
	for i, arn := range value.AWS.Values {
		if IsAccountID(arn) {
			value.AWS.Values[i] = "arn:aws:iam::" + arn + ":root"
		}
	}
	*p = Principal(value)
	return nil
}

func (p Principal) MarshalJSON() ([]byte, error) {
	if p.Any {
		return json.Marshal("*")
	}
	type object Principal
	return json.Marshal(object(p))
}

func IsAccountID(value string) bool {
	if len(value) != 12 {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

type Statement struct {
	SID          string `json:"Sid,omitempty"`
	Effect       string
	Principal    *Principal                         `json:",omitempty"`
	NotPrincipal *Principal                         `json:",omitempty"`
	Action       List[string]                       `json:",omitzero"`
	NotAction    List[string]                       `json:",omitzero"`
	Resource     List[string]                       `json:",omitzero"`
	NotResource  List[string]                       `json:",omitzero"`
	Condition    map[string]map[string]List[string] `json:",omitempty"`
}

type Document struct {
	Version   string `json:",omitempty"`
	ID        string `json:"Id,omitempty"`
	Statement List[Statement]
}

func Parse(raw string) (*Document, error) {
	var doc Document
	if err := decodeStrict([]byte(raw), &doc); err != nil {
		return nil, err
	}
	if len(doc.Statement.Values) == 0 {
		return nil, errors.New("policy has no statements")
	}
	for _, s := range doc.Statement.Values {
		if s.Effect != "Allow" && s.Effect != "Deny" {
			return nil, errors.New("invalid effect")
		}
		if s.Action.IsZero() == s.NotAction.IsZero() || s.Resource.IsZero() == s.NotResource.IsZero() {
			return nil, errors.New("policy must specify one action selector and one resource selector")
		}
		if s.Principal != nil && s.NotPrincipal != nil {
			return nil, errors.New("principal selectors are mutually exclusive")
		}
		for operator, keys := range s.Condition {
			if !supportedCondition(operator) || len(keys) == 0 {
				return nil, fmt.Errorf("unsupported policy condition %q", operator)
			}
			for key, values := range keys {
				if key == "" || values.IsZero() {
					return nil, errors.New("policy condition is empty")
				}
			}
		}
	}
	return &doc, nil
}

func decodeStrict(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after policy value")
	}
	return nil
}

// EquivalentIgnoringSIDs compares the policies used to recreate an existing resource.
func EquivalentIgnoringSIDs(left, right *Document) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Version != right.Version || left.ID != right.ID || len(left.Statement.Values) != len(right.Statement.Values) {
		return false
	}
	for i, a := range left.Statement.Values {
		b := right.Statement.Values[i]
		a.SID, b.SID = "", ""
		if !reflect.DeepEqual(a, b) {
			return false
		}
	}
	return true
}

func supportedCondition(operator string) bool {
	operator, _ = strings.CutSuffix(operator, "IfExists")
	switch operator {
	case "StringEquals", "StringLike", "StringNotEquals", "StringNotLike",
		"ArnEquals", "ArnLike", "ArnNotEquals", "ArnNotLike", "Bool", "Null":
		return true
	}
	return false
}
