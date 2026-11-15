package sqs

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/romycode/ginistack/internal/awshttp"
)

type messageAttribute struct {
	DataType    string
	StringValue *string `json:",omitempty"`
	BinaryValue []byte  `json:",omitempty"`
}

type messageInput struct {
	MessageBody             string
	DelaySeconds            *int
	MessageGroupID          *string `json:"MessageGroupId"`
	MessageDeduplicationID  *string `json:"MessageDeduplicationId"`
	MessageAttributes       map[string]messageAttribute
	MessageSystemAttributes map[string]messageAttribute
}

func attributeDigest(attributes map[string]messageAttribute) string {
	if len(attributes) == 0 {
		return ""
	}
	digest := md5.New()
	for _, name := range slices.Sorted(maps.Keys(attributes)) {
		attribute := attributes[name]
		writeAttributeField(digest, []byte(name))
		writeAttributeField(digest, []byte(attribute.DataType))
		if attribute.StringValue != nil {
			_, _ = digest.Write([]byte{1})
			writeAttributeField(digest, []byte(*attribute.StringValue))
		} else {
			_, _ = digest.Write([]byte{2})
			writeAttributeField(digest, attribute.BinaryValue)
		}
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func writeAttributeField(digest hash.Hash, value []byte) {
	_, _ = digest.Write(binary.BigEndian.AppendUint32(nil, uint32(len(value))))
	_, _ = digest.Write(value)
}

func attributeSize(attributes map[string]messageAttribute) int {
	size := 0
	for name, attribute := range attributes {
		size += len(name) + len(attribute.DataType) + len(attribute.BinaryValue)
		if attribute.StringValue != nil {
			size += len(*attribute.StringValue)
		}
	}
	return size
}

var decimalPattern = regexp.MustCompile(`^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$`)

func validateMessageAttributes(attributes map[string]messageAttribute) *apiError {
	if len(attributes) > 10 {
		return &apiError{"InvalidParameterValue", fmt.Sprintf("Number of message attributes [%d] exceeds the allowed maximum [10].", len(attributes)), 400}
	}
	for _, name := range slices.Sorted(maps.Keys(attributes)) {
		attribute := attributes[name]
		if issue := messageAttributeNameIssue(name); issue != "" {
			return &apiError{"InvalidParameterValue", issue, 400}
		}
		if len(attribute.DataType) > 256 {
			return &apiError{"InvalidParameterValue", "Message (user) attribute type must be shorter than 256 bytes.", 400}
		}
		typeName, label, custom := strings.Cut(attribute.DataType, ".")
		if (typeName != "String" && typeName != "Number" && typeName != "Binary") || (custom && label == "") {
			return &apiError{"InvalidParameterValue", fmt.Sprintf("The type of message (user) attribute '%s' is invalid. You must use only the following supported type prefixes: Binary, Number, String.", name), 400}
		}
		field := "String"
		wrongField := attribute.BinaryValue != nil
		empty := attribute.StringValue == nil || *attribute.StringValue == ""
		if typeName == "Binary" {
			field = "Binary"
			wrongField = attribute.StringValue != nil
			empty = len(attribute.BinaryValue) == 0
		}
		if wrongField {
			return &apiError{"InvalidParameterValue", fmt.Sprintf("Message (user) attribute '%s' with type '%s' must use field '%s'.", name, attribute.DataType, field), 400}
		}
		if empty {
			return &apiError{"InvalidParameterValue", fmt.Sprintf("Message (user) attribute '%s' must contain a non-empty value of type '%s'.", name, attribute.DataType), 400}
		}
		if typeName == "Number" {
			value, issue := normalizeNumber(*attribute.StringValue, name)
			if issue != "" {
				return &apiError{"InvalidParameterValue", issue, 400}
			}
			attribute.StringValue = &value
			attributes[name] = attribute
		}
	}
	return nil
}

func messageAttributeNameIssue(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon."):
		return "You can't use message attribute names beginning with 'AWS.' or 'Amazon'. These strings are reserved for internal use."
	case strings.HasPrefix(name, "."):
		return "Message (user) attribute name must not begin with character '.'."
	case strings.HasSuffix(name, "."):
		return "Message (user) attribute name must not end with character '.'."
	case strings.Contains(name, ".."):
		return "Message (user) attribute name must not have successive '.' characters."
	case len(name) > 256:
		return "Message (user) attribute name must be shorter than 256 bytes."
	case name == "":
		return "Message (user) attribute name must not be empty."
	}
	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || strings.ContainsRune("-_.", ch) {
			continue
		}
		return fmt.Sprintf("Message (user) attribute name contains invalid non-alphanumeric character '#x%X'. You can use only alphanumeric characters, hyphens (-), underscores (_), or periods (.).", ch)
	}
	return ""
}

func normalizeNumber(raw, name string) (string, string) {
	parts := decimalPattern.FindStringSubmatch(raw)
	invalid := fmt.Sprintf("Can't cast the value of message (user) attribute '%s' to a number.", name)
	if parts == nil || parts[2]+parts[3] == "" {
		return "", invalid
	}
	digits := parts[2] + parts[3]
	withoutLeading := strings.TrimLeft(digits, "0")
	if withoutLeading == "" {
		return "0", ""
	}
	significant := strings.TrimRight(withoutLeading, "0")
	if len(significant) > 38 {
		return "", fmt.Sprintf("Message (user) attribute '%s' number value [%s] must contain fewer than 38 precision digits.", name, raw)
	}
	exponent := 0
	if parts[4] != "" {
		var err error
		exponent, err = strconv.Atoi(parts[4])
		if err != nil || exponent < -awshttp.MaxRequestBody || exponent > awshttp.MaxRequestBody {
			return "", invalid
		}
	}
	point := len(parts[2]) - len(digits) + len(withoutLeading) + exponent
	if point < -127 || point > 127 {
		return "", invalid
	}
	switch {
	case point <= 0:
		significant = "0." + strings.Repeat("0", -point) + significant
	case point >= len(significant):
		significant += strings.Repeat("0", point-len(significant))
	default:
		significant = significant[:point] + "." + significant[point:]
	}
	return strings.TrimPrefix(parts[1], "+") + significant, ""
}

var traceRootPattern = regexp.MustCompile(`^(?:[^;]+;)*Root=1-[0-9a-fA-F]{8}-[0-9a-fA-F]{24}(?:;|$)`)

func messageTraceHeader(attributes map[string]messageAttribute) (string, *apiError) {
	for _, name := range slices.Sorted(maps.Keys(attributes)) {
		if name != "AWSTraceHeader" {
			return "", &apiError{"InvalidParameterValue", fmt.Sprintf("Message system attribute name '%s' is invalid.", name), 400}
		}
	}
	trace, exists := attributes["AWSTraceHeader"]
	if !exists || trace.DataType != "String" {
		return "", nil
	}
	if trace.StringValue == nil || *trace.StringValue == "" {
		return "", &apiError{"InvalidParameterValue", "Message system attribute 'AWSTraceHeader' must contain a non-empty value of type 'String'.", 400}
	}
	if !traceRootPattern.MatchString(*trace.StringValue) {
		return "", nil
	}
	return *trace.StringValue, nil
}

func selectMessageAttributes(attributes map[string]messageAttribute, names []string) map[string]messageAttribute {
	if len(attributes) == 0 || len(names) == 0 {
		return nil
	}
	selected := make(map[string]messageAttribute)
	for _, pattern := range names {
		if pattern == "All" || pattern == ".*" {
			return attributes
		}
		if prefix, wildcard := strings.CutSuffix(pattern, ".*"); wildcard {
			for name, attribute := range attributes {
				if strings.HasPrefix(name, prefix) {
					selected[name] = attribute
				}
			}
		} else if attribute, exists := attributes[pattern]; exists {
			selected[pattern] = attribute
		}
	}
	return selected
}
