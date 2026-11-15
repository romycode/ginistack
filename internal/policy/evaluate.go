package policy

import (
	"strconv"
	"strings"
)

type Decision uint8

const (
	NoMatch Decision = iota
	Allow
	Deny
)

type Request struct {
	PrincipalARN     string
	PrincipalAccount string
	PrincipalService string
	Action           string
	Resource         string
	Context          map[string]string
}

// Evaluate reports a policy decision. The caller supplies the default decision
// and combines the results from resource and identity policies.
func (d *Document) Evaluate(request Request) Decision {
	if d == nil {
		return NoMatch
	}
	decision := NoMatch
	for _, statement := range d.Statement.Values {
		if !statement.matches(request) {
			continue
		}
		if statement.Effect == "Deny" {
			return Deny
		}
		decision = Allow
	}
	return decision
}

func (s Statement) matches(request Request) bool {
	if s.Principal != nil && !s.Principal.matches(request) ||
		s.NotPrincipal != nil && s.NotPrincipal.matches(request) {
		return false
	}
	if !matchesSelector(request.Action, s.Action, s.NotAction, true) ||
		!matchesSelector(request.Resource, s.Resource, s.NotResource, false) {
		return false
	}
	for operator, conditions := range s.Condition {
		for key, options := range conditions {
			if !matchCondition(operator, key, options.Values, request.Context) {
				return false
			}
		}
	}
	return true
}

func (p Principal) matches(request Request) bool {
	if p.Any {
		return true
	}
	for _, item := range p.AWS.Values {
		if item == "*" || (request.PrincipalARN != "" && item == request.PrincipalARN) ||
			(request.PrincipalAccount != "" && item == "arn:aws:iam::"+request.PrincipalAccount+":root") {
			return true
		}
	}
	for _, item := range p.Service.Values {
		if request.PrincipalService != "" && strings.EqualFold(item, request.PrincipalService) {
			return true
		}
	}
	return false
}

func matchesSelector(value string, include, exclude List[string], ignoreCase bool) bool {
	patterns := include.Values
	inverted := len(exclude.Values) != 0
	if inverted {
		patterns = exclude.Values
	}
	if ignoreCase {
		value = strings.ToLower(value)
	}
	for _, pattern := range patterns {
		if ignoreCase {
			pattern = strings.ToLower(pattern)
		}
		if wildcardMatch(pattern, value) {
			return !inverted
		}
	}
	return inverted
}

func matchCondition(operator, key string, options []string, context map[string]string) bool {
	operator, ifExists := strings.CutSuffix(operator, "IfExists")
	var value string
	var exists bool
	for name, v := range context {
		if strings.EqualFold(name, key) {
			value, exists = v, true
			break
		}
	}
	negative := strings.HasPrefix(operator, "StringNot") || strings.HasPrefix(operator, "ArnNot")
	if operator == "Null" {
		for _, option := range options {
			if option == strconv.FormatBool(!exists) {
				return true
			}
		}
		return false
	}
	if !exists {
		return ifExists || negative
	}
	for _, option := range options {
		matched := option == value
		switch operator {
		case "StringLike", "StringNotLike", "ArnEquals", "ArnLike", "ArnNotEquals", "ArnNotLike":
			matched = wildcardMatch(option, value)
		case "Bool":
			matched = (option == "true" || option == "false") && option == value
		}
		if matched {
			return !negative
		}
	}
	return negative
}

// wildcardMatch recognizes AWS-style * and ? across an entire value, including slashes in ARNs.
func wildcardMatch(pattern, value string) bool {
	p, v := []rune(pattern), []rune(value)
	i, j, star, saved := 0, 0, -1, 0
	for j < len(v) {
		switch {
		case i < len(p) && (p[i] == '?' || p[i] == v[j] && p[i] != '*'):
			i++
			j++
		case i < len(p) && p[i] == '*':
			star, saved = i, j
			i++
		case star >= 0:
			saved++
			i, j = star+1, saved
		default:
			return false
		}
	}
	for i < len(p) && p[i] == '*' {
		i++
	}
	return i == len(p)
}
