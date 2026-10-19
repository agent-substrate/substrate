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

package lint

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// matchesDVPrefix starts the note of a tag that copies another field's tag.
// The full name of that field follows it, then optionally a comma or space
// and free text. For example,
// `+k8s:maxItems=32 # matches substrate.ateapi.v1.ActorTemplate.volumes`.
const matchesDVPrefix = "matches"

// MatchingDVTags checks the tags that copy a bound from another proto. Unlike
// the rules in All, it runs over every proto apitool loads, unscoped, since
// the field a tag matches is often in another file.
var MatchingDVTags = Rule{
	Name:        "matching-dv-tags",
	Description: `A declarative validation tag noted "# matches <field>" must match an existing field with the same DV tag and value.`,
	Check:       checkMatchingDVTags,
}

func checkMatchingDVTags(api *model.API) ([]Finding, error) {
	messages := api.MessagesByFullName()
	var findings []Finding
	for _, m := range api.Messages {
		for _, f := range m.Fields {
			subject := m.FullName.Append(f.Name)
			// A tag validation-gen would reject fails the run, not a finding.
			tags, err := model.DVTags(f.Comment)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", subject, err)
			}
			for _, tag := range tags {
				// Only process a note that is "matches" or starts with "matches ".
				rest, ok := strings.CutPrefix(tag.Note, matchesDVPrefix)
				if !ok || rest != "" && rest[0] != ' ' {
					continue
				}
				// The reference is the next word, minus a trailing comma.
				// A bare "matches" leaves it empty, which lookupField rejects.
				ref, _, _ := strings.Cut(strings.TrimSpace(rest), " ")
				ref = strings.TrimSuffix(ref, ",")
				if protoreflect.FullName(ref) == subject {
					findings = append(findings, findingf(subject, "+k8s:%s matches its own field", tag.Name))
					continue
				}
				target, ok := lookupField(messages, protoreflect.FullName(ref))
				if !ok {
					findings = append(findings, findingf(subject, "+k8s:%s matches %q, which is not a field", tag.Name, ref))
					continue
				}
				// Compare against the target's tags only, never its own
				// matches note, so each reference is checked one hop.
				targetDVTags, err := model.DVTags(target.Comment)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", ref, err)
				}
				findings = append(findings, compareDVTag(subject, tag, ref, targetDVTags)...)
			}
		}
	}
	return findings, nil
}

// lookupField resolves a field's full name, such as
// "substrate.ateapi.v1.ActorTemplate.volumes".
func lookupField(messages map[protoreflect.FullName]model.Message, name protoreflect.FullName) (model.Field, bool) {
	m, ok := messages[name.Parent()]
	if !ok {
		return model.Field{}, false
	}
	for _, f := range m.Fields {
		if f.Name == name.Name() {
			return f, true
		}
	}
	return model.Field{}, false
}

// compareDVTag checks that targetDVTags hold tag's name with tag's value. A
// field can repeat a tag name, as with +k8s:update, so any one match is
// enough.
func compareDVTag(subject protoreflect.FullName, tag model.DVTag, ref string, targetDVTags []model.DVTag) []Finding {
	var values []string
	for _, t := range targetDVTags {
		if t.Name != tag.Name {
			continue
		}
		if t.Value == tag.Value {
			return nil
		}
		values = append(values, t.Value)
	}
	if len(values) == 0 {
		return []Finding{findingf(subject, "+k8s:%s matches %s, which has no +k8s:%s tag", tag.Name, ref, tag.Name)}
	}
	return []Finding{findingf(subject, "+k8s:%s=%s matches %s, which has +k8s:%s=%s", tag.Name, tag.Value, ref, tag.Name, strings.Join(values, ","))}
}
