package sns

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	filterMessageAttributes = "MessageAttributes"
	filterMessageBody       = "MessageBody"
	maxFilterKeys           = 5
	maxFilterCombinations   = 150
)

type filterMatch func(value any, present bool) bool

type filterPolicy struct {
	match  filterMatch
	nested bool
}

type filterNode struct {
	value           any
	start, firstEnd int
	fields          []filterField
	items           []filterNode
}

type filterField struct {
	name string
	node filterNode
}

type filterCompiler struct {
	source string
	nested bool
}

var (
	filterTooComplex      = invalid("FilterPolicy: Filter policy is too complex")
	filterTooManyKeys     = invalid("FilterPolicy: Filter policy can not have more than 5 keys")
	attributeFilterNested = invalid("Filter policy scope MessageAttributes does not support nested filter policy")
)

func parseFilterPolicy(raw, scope string) (filterPolicy, string, *apiError) {
	if raw == "" {
		return filterPolicy{}, "", nil
	}
	var normalized bytes.Buffer
	if err := json.Compact(&normalized, []byte(raw)); err != nil {
		return filterPolicy{}, "", malformedFilterPolicy(raw, err)
	}
	compiler := filterCompiler{source: normalized.String()}
	decoder := json.NewDecoder(strings.NewReader(compiler.source))
	decoder.UseNumber()
	root := readFilterNode(decoder, compiler.source)
	if root.value != json.Delim('{') {
		return filterPolicy{}, "", invalid("FilterPolicy")
	}
	if len(root.fields) == 0 {
		return filterPolicy{}, "", nil
	}
	match, leaves, combinations, failure := compiler.object(root, 1)
	if failure != nil {
		return filterPolicy{}, "", failure
	}
	if compiler.nested && scope != filterMessageBody {
		return filterPolicy{}, "", attributeFilterNested
	}
	if len(leaves) > maxFilterKeys {
		return filterPolicy{}, "", filterTooManyKeys
	}
	if combinations > maxFilterCombinations {
		return filterPolicy{}, "", filterTooComplex
	}
	return filterPolicy{match, compiler.nested}, compiler.source, nil
}

func malformedFilterPolicy(raw string, err error) *apiError {
	syntax, ok := errors.AsType[*json.SyntaxError](err)
	if !ok || syntax.Offset < 1 || syntax.Offset > int64(len(raw)) {
		return invalid("FilterPolicy")
	}
	isLetter := func(ch byte) bool { return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' }
	start := int(syntax.Offset) - 1
	if !isLetter(raw[start]) {
		return invalid("FilterPolicy")
	}
	end := start + 1
	for start > 0 && isLetter(raw[start-1]) {
		start--
	}
	for end < len(raw) && isLetter(raw[end]) {
		end++
	}
	line := strings.Count(raw[:start], "\n") + 1
	column := start - strings.LastIndexByte(raw[:start], '\n')
	return invalid(fmt.Sprintf(
		"FilterPolicy: failed to parse JSON. Unrecognized token '%s': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')\n at [Source: REDACTED ("+
			"`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: %d, column: %d]",
		raw[start:end], line, column,
	))
}

// The compact document has already been checked by json.Compact. Keep token
// positions and field order so validation can point to the offending value.
func readFilterNode(decoder *json.Decoder, source string) filterNode {
	start := int(decoder.InputOffset())
	for start < len(source) && (source[start] == ':' || source[start] == ',') {
		start++
	}
	token, _ := decoder.Token()
	node := filterNode{value: token, start: start, firstEnd: int(decoder.InputOffset())}
	switch token {
	case json.Delim('{'):
		for decoder.More() {
			name, _ := decoder.Token()
			node.fields = append(node.fields, filterField{name.(string), readFilterNode(decoder, source)})
		}
		_, _ = decoder.Token()
	case json.Delim('['):
		for decoder.More() {
			node.items = append(node.items, readFilterNode(decoder, source))
		}
		_, _ = decoder.Token()
	}
	return node
}

type filterProperty struct {
	name  string
	match filterMatch
}

func (c *filterCompiler) object(node filterNode, depth int) (filterMatch, map[string]struct{}, int, *apiError) {
	var properties []filterProperty
	var alternatives []filterMatch
	leaves := make(map[string]struct{})
	keys := make(map[string]struct{}, len(node.fields))
	combinations := 1
	for _, field := range node.fields {
		if _, duplicate := keys[field.name]; duplicate || field.name == "" {
			return nil, nil, 0, invalid("FilterPolicy")
		}
		keys[field.name] = struct{}{}
		if field.name == "$or" && field.node.value == json.Delim('[') && len(field.node.items) >= 2 {
			allObjects := true
			for _, branch := range field.node.items {
				allObjects = allObjects && branch.value == json.Delim('{') && len(branch.fields) != 0
			}
			if allObjects {
				options := 0
				for _, branch := range field.node.items {
					match, branchLeaves, count, failure := c.object(branch, depth)
					if failure != nil {
						return nil, nil, 0, failure
					}
					for key := range branchLeaves {
						leaves[key] = struct{}{}
					}
					options = min(maxFilterCombinations+1, options+count)
					alternatives = append(alternatives, match)
				}
				combinations = min(maxFilterCombinations+1, combinations*options)
				continue
			}
		}
		switch field.node.value {
		case json.Delim('{'):
			if len(field.node.fields) == 0 {
				return nil, nil, 0, invalid("FilterPolicy")
			}
			c.nested = true
			match, nestedLeaves, count, failure := c.object(field.node, depth+1)
			if failure != nil {
				return nil, nil, 0, failure
			}
			for key := range nestedLeaves {
				leaves[field.name+"\x00"+key] = struct{}{}
			}
			combinations = min(maxFilterCombinations+1, combinations*count)
			properties = append(properties, filterProperty{field.name, match})
		case json.Delim('['):
			if len(field.node.items) == 0 {
				return nil, nil, 0, invalid("FilterPolicy")
			}
			var choices []filterMatch
			for _, item := range field.node.items {
				match, failure := c.choice(item)
				if failure != nil {
					return nil, nil, 0, failure
				}
				choices = append(choices, match)
			}
			leaves[field.name] = struct{}{}
			factor := min(maxFilterCombinations+1, len(choices)*depth)
			combinations = min(maxFilterCombinations+1, combinations*factor)
			properties = append(properties, filterProperty{field.name, func(value any, present bool) bool {
				for _, choice := range choices {
					if matchArrayMember(choice, value, present) {
						return true
					}
				}
				return false
			}})
		default:
			return nil, nil, 0, c.error(fmt.Sprintf("%q must be an object or an array", field.name), field.node.start+2)
		}
	}
	match := func(value any, _ bool) bool {
		object, ok := value.(map[string]any)
		if !ok {
			return false
		}
		for _, property := range properties {
			item, present := object[property.name]
			if !matchArrayMember(property.match, item, present) {
				return false
			}
		}
		if len(alternatives) == 0 {
			return true
		}
		for _, branch := range alternatives {
			if branch(value, true) {
				return true
			}
		}
		return false
	}
	return match, leaves, combinations, nil
}

func (c *filterCompiler) error(message string, column int) *apiError {
	return invalid(fmt.Sprintf("FilterPolicy: %s\n at [Source: (String)\"%s\"; line: 1, column: %d]", message, c.source, column))
}

func matchArrayMember(match filterMatch, value any, present bool) bool {
	if members, ok := value.([]any); ok {
		if match(value, present) {
			return true
		}
		for _, member := range members {
			if match(member, present) {
				return true
			}
		}
		return false
	}
	return match(value, present)
}

func parseFilterValue(raw string) (any, bool) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var value any
	err := decoder.Decode(&value)
	return value, err == nil && json.Valid([]byte(raw))
}

func (p filterPolicy) matches(scope, body string, attributes map[string]envelopeAttribute) bool {
	if p.match == nil {
		return true
	}
	if scope == filterMessageBody {
		value, valid := parseFilterValue(body)
		return valid && p.match(value, true)
	}
	values := make(map[string]any, len(attributes))
	for name, attr := range attributes {
		if attr.Type == "String.Array" {
			if value, valid := parseFilterValue(attr.Value); valid {
				if members, ok := value.([]any); ok {
					values[name] = members
				}
			}
			continue
		}
		base, _, _ := strings.Cut(attr.Type, ".")
		switch base {
		case "String":
			values[name] = attr.Value
		case "Number":
			values[name] = json.Number(attr.Value)
		}
	}
	return p.match(values, true)
}
