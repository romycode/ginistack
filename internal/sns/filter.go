package sns

import (
	"bytes"
	"encoding/json"
	"strings"
)

type filterPolicy map[string][]any

var filterTooComplex = invalid("FilterPolicy: Filter policy is too complex")

func parseFilterPolicy(raw string) (filterPolicy, string, *apiError) {
	if raw == "" {
		return nil, "", nil
	}
	var rules filterPolicy
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var normalized bytes.Buffer
	if json.Compact(&normalized, []byte(raw)) != nil || decoder.Decode(&rules) != nil || rules == nil {
		return nil, "", invalid("FilterPolicy")
	}
	combinations := 1
	for name, choices := range rules {
		if name == "" || len(choices) == 0 {
			return nil, "", invalid("FilterPolicy")
		}
		if len(choices) > 150/combinations {
			return nil, "", filterTooComplex
		}
		combinations *= len(choices)
		for _, choice := range choices {
			switch choice.(type) {
			case string, json.Number:
			default:
				return nil, "", invalid("FilterPolicy")
			}
		}
	}
	return rules, normalized.String(), nil
}

func (p filterPolicy) matches(attributes map[string]envelopeAttribute) bool {
	for name, choices := range p {
		attribute, present := attributes[name]
		if !present || !matchesAttribute(attribute, choices) {
			return false
		}
	}
	return true
}

func matchesAttribute(attribute envelopeAttribute, choices []any) bool {
	if attribute.Type == "String.Array" {
		if !json.Valid([]byte(attribute.Value)) {
			return false
		}
		var members []any
		decoder := json.NewDecoder(strings.NewReader(attribute.Value))
		decoder.UseNumber()
		if decoder.Decode(&members) != nil {
			return false
		}
		for _, member := range members {
			if matchesChoice(member, choices) {
				return true
			}
		}
		return false
	}
	baseType, _, _ := strings.Cut(attribute.Type, ".")
	switch baseType {
	case "String":
		return matchesChoice(attribute.Value, choices)
	case "Number":
		return matchesChoice(json.Number(attribute.Value), choices)
	}
	return false
}

func matchesChoice(member any, choices []any) bool {
	switch value := member.(type) {
	case string:
		for _, choice := range choices {
			if want, ok := choice.(string); ok && want == value {
				return true
			}
		}
	case json.Number:
		received, err := value.Float64()
		if err != nil {
			return false
		}
		for _, choice := range choices {
			if number, ok := choice.(json.Number); ok {
				want, err := number.Float64()
				if err == nil && want == received {
					return true
				}
			}
		}
	}
	return false
}
