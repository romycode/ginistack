package sqs

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"maps"
	"slices"
	"strings"
)

type messageAttribute struct {
	DataType    string
	StringValue *string `json:",omitempty"`
	BinaryValue []byte  `json:",omitempty"`
}

type messageInput struct {
	MessageBody             string
	DelaySeconds            *int
	MessageGroupID          string  `json:"MessageGroupId"`
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

func validateMessageAttributes(attributes map[string]messageAttribute) *apiError {
	if len(attributes) > 10 {
		return &apiError{"InvalidParameterValue", "A message can have at most 10 message attributes.", 400}
	}
	for name, attribute := range attributes {
		typeName, _, _ := strings.Cut(attribute.DataType, ".")
		invalid := name == "" || strings.Contains(name, "..") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".")
		switch typeName {
		case "String", "Number":
			invalid = invalid || attribute.StringValue == nil || *attribute.StringValue == "" || attribute.BinaryValue != nil
		case "Binary":
			invalid = invalid || len(attribute.BinaryValue) == 0 || attribute.StringValue != nil
		default:
			invalid = true
		}
		if invalid {
			return &apiError{"InvalidParameterValue", "Invalid message attribute " + name + ".", 400}
		}
	}
	return nil
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
