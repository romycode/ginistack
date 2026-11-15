package sns

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

type filterPolicy map[string][]any

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
			return nil, "", invalid("FilterPolicy: Filter policy is too complex")
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
	baseType, _, _ := strings.Cut(attribute.Type, ".")
	for _, choice := range choices {
		switch value := choice.(type) {
		case string:
			if baseType == "String" && attribute.Value == value {
				return true
			}
		case json.Number:
			if baseType == "Number" {
				want, expected := value.Float64()
				received, valid := strconv.ParseFloat(attribute.Value, 64)
				if expected == nil && valid == nil && received == want {
					return true
				}
			}
		}
	}
	return false
}
