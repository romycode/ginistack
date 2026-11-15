package sns

import (
	"encoding/json"
	"net/netip"
	"regexp"
	"strings"
)

func (c *filterCompiler) choice(node filterNode) (filterMatch, *apiError) {
	switch value := node.value.(type) {
	case json.Delim:
		if value == '{' && len(node.fields) == 1 {
			return c.operator(node.fields[0])
		}
	case string, json.Number, bool, nil:
		return equalFilterValue(value), nil
	}
	return nil, invalid("FilterPolicy")
}

func equalFilterValue(want any) filterMatch {
	if number, ok := want.(json.Number); ok {
		expected, err := number.Float64()
		return func(value any, present bool) bool {
			number, ok := filterNumber(value)
			return present && err == nil && ok && number == expected
		}
	}
	return func(value any, present bool) bool { return present && value == want }
}

func filterNumber(value any) (float64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := number.Float64()
	return parsed, err == nil
}

func (c *filterCompiler) operator(field filterField) (filterMatch, *apiError) {
	value := field.node
	switch field.name {
	case "exists":
		present, ok := value.value.(bool)
		if !ok {
			return nil, c.error("exists match pattern must be either true or false.", value.start+2)
		}
		return func(_ any, exists bool) bool { return exists == present }, nil
	case "numeric":
		return c.numeric(value)
	case "anything-but":
		return c.anythingBut(value)
	case "prefix", "suffix", "equals-ignore-case", "wildcard", "cidr":
		return c.text(field)
	}
	return nil, c.error("Unrecognized match type "+field.name, value.firstEnd+1)
}

func (c *filterCompiler) text(field filterField) (filterMatch, *apiError) {
	pattern, ok := field.node.value.(string)
	ignoreCase := field.name == "equals-ignore-case"
	if !ok && field.name == "prefix" && len(field.node.fields) == 1 && field.node.fields[0].name == "equals-ignore-case" {
		pattern, ok = field.node.fields[0].node.value.(string)
		ignoreCase = true
	}
	if !ok {
		return nil, invalid("FilterPolicy")
	}
	if ignoreCase {
		pattern = strings.ToLower(pattern)
	}
	var test func(string) bool
	switch field.name {
	case "prefix":
		test = func(text string) bool { return strings.HasPrefix(text, pattern) }
	case "suffix":
		test = func(text string) bool { return strings.HasSuffix(text, pattern) }
	case "equals-ignore-case":
		test = func(text string) bool { return text == pattern }
	case "wildcard":
		expression, err := regexp.Compile("(?s)^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), "\\*", ".*") + "$")
		if err != nil {
			return nil, invalid("FilterPolicy")
		}
		test = expression.MatchString
	case "cidr":
		prefix, err := netip.ParsePrefix(pattern)
		if err != nil {
			return nil, invalid("FilterPolicy")
		}
		test = func(text string) bool {
			address, err := netip.ParseAddr(text)
			return err == nil && prefix.Contains(address)
		}
	}
	return func(value any, present bool) bool {
		text, ok := value.(string)
		if !present || !ok {
			return false
		}
		if ignoreCase {
			text = strings.ToLower(text)
		}
		return test(text)
	}, nil
}

func (c *filterCompiler) anythingBut(node filterNode) (filterMatch, *apiError) {
	var denied []filterMatch
	var typ any
	if node.value == json.Delim('{') && len(node.fields) == 1 {
		field := node.fields[0]
		switch field.name {
		case "prefix", "suffix", "equals-ignore-case", "wildcard":
		default:
			return nil, c.error("Unsupported anything-but pattern: "+field.name, field.node.start+2)
		}
		match, failure := c.text(field)
		if failure != nil {
			return nil, failure
		}
		denied, typ = []filterMatch{match}, ""
	} else {
		items := []filterNode{node}
		if node.value == json.Delim('[') {
			items = node.items
		}
		for _, item := range items {
			switch item.value.(type) {
			case string, json.Number:
				denied = append(denied, equalFilterValue(item.value))
				typ = item.value
			default:
				return nil, invalid("FilterPolicy")
			}
		}
		if len(denied) == 0 {
			return nil, invalid("FilterPolicy")
		}
	}
	return func(value any, present bool) bool {
		if !present {
			return false
		}
		_, stringPattern := typ.(string)
		_, stringValue := value.(string)
		_, numberValue := value.(json.Number)
		if stringPattern && !stringValue || !stringPattern && !numberValue {
			return false
		}
		for _, match := range denied {
			if match(value, true) {
				return false
			}
		}
		return true
	}, nil
}

func (c *filterCompiler) numeric(node filterNode) (filterMatch, *apiError) {
	if count := len(node.items); count == 0 || count%2 != 0 {
		return nil, invalid("FilterPolicy")
	}
	var comparisons []func(float64) bool
	for i := 0; i < len(node.items); i += 2 {
		operator, ok := node.items[i].value.(string)
		want, number := filterNumber(node.items[i+1].value)
		if !ok || !number {
			return nil, invalid("FilterPolicy")
		}
		switch operator {
		case "=":
			comparisons = append(comparisons, func(value float64) bool { return value == want })
		case ">":
			comparisons = append(comparisons, func(value float64) bool { return value > want })
		case ">=":
			comparisons = append(comparisons, func(value float64) bool { return value >= want })
		case "<":
			comparisons = append(comparisons, func(value float64) bool { return value < want })
		case "<=":
			comparisons = append(comparisons, func(value float64) bool { return value <= want })
		default:
			return nil, c.error("Unrecognized numeric range operator: "+operator, node.items[i+1].firstEnd+1)
		}
	}
	return func(value any, present bool) bool {
		number, ok := filterNumber(value)
		if !present || !ok {
			return false
		}
		for _, compare := range comparisons {
			if !compare(number) {
				return false
			}
		}
		return true
	}, nil
}
