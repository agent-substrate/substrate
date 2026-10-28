// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/gengo/v2/codetags"
)

const dvTagPrefix = "+k8s:"

// DVTag is one declarative validation tag on a comment line of its own. For
// example, `+k8s:maxItems=32 # matches substrate.ateapi.v1.ActorTemplate.volumes`.
type DVTag struct {
	// Name is the tag in canonical form up to its final value: args are
	// spaced and their strings double-quoted. For example, "maxItems", or
	// "eachVal=+k8s:maxLength" for a tag applied to each list item.
	Name string
	// Value is the final value, or "" for a tag that takes none. An int is
	// in decimal. For example, "32" for `+k8s:maxItems=32` or
	// `+k8s:maxItems=0x20`, "" for `+k8s:required`.
	Value string
	// Note is the trailing comment after the tag's "#", or "". For example,
	// "matches substrate.ateapi.v1.ActorTemplate.volumes".
	Note string
}

// DVTags returns the declarative validation tags in comment, in order. A
// line is a tag when it starts with "+k8s:". Tags are parsed with
// validation-gen's own parser, so a tag it rejects is an error here too.
func DVTags(comment string) ([]DVTag, error) {
	var tags []DVTag
	for line := range strings.Lines(comment) {
		line, ok := strings.CutPrefix(strings.TrimSpace(line), dvTagPrefix)
		if !ok {
			continue
		}
		tag, err := codetags.Parse(line)
		if err != nil {
			return nil, fmt.Errorf("while parsing %s%s: %w", dvTagPrefix, line, err)
		}
		name, value, err := splitFinalValue(tag)
		if err != nil {
			return nil, fmt.Errorf("while parsing %s%s: %w", dvTagPrefix, line, err)
		}
		tags = append(tags, DVTag{Name: name, Value: value, Note: note(line, tag)})
	}
	return tags, nil
}

// splitFinalValue returns tag's canonical form without the value at the end
// of its chain, and that value.
func splitFinalValue(tag codetags.Tag) (string, string, error) {
	if tag.ValueType == codetags.ValueTypeTag {
		name, value, err := splitFinalValue(*tag.ValueTag)
		tag.ValueTag = &codetags.Tag{Name: name}
		return tag.String(), value, err
	}
	value := tag.Value
	if tag.ValueType == codetags.ValueTypeInt {
		n, err := strconv.ParseInt(value, 0, 64)
		if err != nil {
			return "", "", err
		}
		value = strconv.FormatInt(n, 10)
	}
	tag.Value, tag.ValueType = "", codetags.ValueTypeNone
	return tag.String(), value, nil
}

// note returns line's trailing comment. codetags.Parse drops it, so it starts
// at the first '#' before which line parses to the same tag on its own. A '#'
// inside a quoted string leaves the string unterminated there.
func note(line string, tag codetags.Tag) string {
	for i, r := range line {
		if r != '#' {
			continue
		}
		if t, err := codetags.Parse(line[:i]); err == nil && t.String() == tag.String() {
			return strings.TrimSpace(line[i+1:])
		}
	}
	return ""
}
