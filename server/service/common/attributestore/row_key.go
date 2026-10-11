// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

package attributestore

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/superdurable/dex/config"
)

const rowKeyWildcard = "*"

// rowKeyTemplate splits a Flow ID into primary-key column values, such as release-{project_id}-{release_id}.
type rowKeyTemplate struct {
	segments []rowKeySegment
}

// rowKeySegment is literal text or a placeholder; a wildcard placeholder has no column.
type rowKeySegment struct {
	literal       string
	column        string
	isPlaceholder bool
}

func parseRowKeyTemplate(template string) (*rowKeyTemplate, error) {
	parsed := &rowKeyTemplate{}
	seenColumns := map[string]bool{}
	remaining := template
	for remaining != "" {
		opening := strings.IndexAny(remaining, "{}")
		if opening < 0 {
			parsed.segments = append(parsed.segments, rowKeySegment{literal: remaining})
			break
		}
		if remaining[opening] == '}' {
			return nil, fmt.Errorf("rowKey has an unmatched '}'")
		}
		if opening > 0 {
			parsed.segments = append(parsed.segments, rowKeySegment{literal: remaining[:opening]})
		}
		closing := strings.IndexAny(remaining[opening+1:], "{}")
		if closing < 0 || remaining[opening+1+closing] != '}' {
			return nil, fmt.Errorf("rowKey has an unterminated placeholder")
		}
		name := remaining[opening+1 : opening+1+closing]
		if name == "" {
			return nil, fmt.Errorf("rowKey has an empty placeholder")
		}
		if len(parsed.segments) > 0 && parsed.segments[len(parsed.segments)-1].isPlaceholder {
			return nil, fmt.Errorf("rowKey placeholders must be separated by literal text")
		}
		segment := rowKeySegment{isPlaceholder: true}
		if name != rowKeyWildcard {
			if seenColumns[name] {
				return nil, fmt.Errorf("rowKey names column %q more than once", name)
			}
			seenColumns[name] = true
			segment.column = name
		}
		parsed.segments = append(parsed.segments, segment)
		remaining = remaining[opening+2+closing:]
	}
	if len(seenColumns) == 0 {
		return nil, fmt.Errorf("rowKey must name at least one column")
	}
	return parsed, nil
}

// columns returns the named placeholders in template order.
func (t *rowKeyTemplate) columns() []string {
	columns := make([]string, 0, len(t.segments))
	for _, segment := range t.segments {
		if segment.column != "" {
			columns = append(columns, segment.column)
		}
	}
	return columns
}

// match returns the value of each named placeholder in template order, and false when flowID does not match.
// A placeholder captures up to the next literal text, the last one captures the rest, and no capture is empty.
func (t *rowKeyTemplate) match(flowID string) ([]string, bool) {
	values := make([]string, 0, len(t.segments))
	position := 0
	for index, segment := range t.segments {
		if !segment.isPlaceholder {
			if !strings.HasPrefix(flowID[position:], segment.literal) {
				return nil, false
			}
			position += len(segment.literal)
			continue
		}
		end := len(flowID)
		if index+1 < len(t.segments) {
			next := strings.Index(flowID[position:], t.segments[index+1].literal)
			if next < 0 {
				return nil, false
			}
			end = position + next
		}
		if end == position {
			return nil, false
		}
		if segment.column != "" {
			values = append(values, flowID[position:end])
		}
		position = end
	}
	if position != len(flowID) {
		return nil, false
	}
	return values, true
}

// columnName returns the column that stores an Attribute or map value field under the store's naming rule.
func columnName(naming config.AttributeStoreColumnNaming, name string) string {
	if naming != config.AttributeStoreColumnNamingSnakeCase {
		return name
	}
	return snakeCase(name)
}

// snakeCase returns name with an underscore before each upper-case letter that follows a lower-case letter or
// digit, or that ends an acronym before a lower-case letter, and every letter lower-cased: DeploymentURL returns
// deployment_url, URLPath returns url_path, and S3Bucket returns s3_bucket.
func snakeCase(name string) string {
	runes := []rune(name)
	var result strings.Builder
	for index, character := range runes {
		if index > 0 && unicode.IsUpper(character) {
			previous := runes[index-1]
			endsAcronym := unicode.IsUpper(previous) && index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || endsAcronym {
				result.WriteByte('_')
			}
		}
		result.WriteRune(unicode.ToLower(character))
	}
	return result.String()
}
