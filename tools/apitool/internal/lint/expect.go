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

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// fieldType is the type a field is expected to have.
type fieldType struct {
	// desc names the type in findings. For example, "string" or
	// "ateapi.ObjectRef".
	desc    string
	matches func(model.Field) bool
}

func scalar(kind string) fieldType {
	return fieldType{kind, func(f model.Field) bool { return f.TypeDisplay == kind }}
}

func message(typeFullName string) fieldType {
	return fieldType{typeFullName, func(f model.Field) bool {
		return !f.Repeated && isMessageOf(f, typeFullName)
	}}
}

var repeatedMessage = fieldType{"repeated message", func(f model.Field) bool {
	return f.Repeated && f.TypeKind == "message"
}}

// fieldSpec is one field a message is expected to have.
type fieldSpec struct {
	// name is the field's expected name, or "" to match a field by its
	// type alone, whatever its name.
	name string
	typ  fieldType
	// optional lets the field be absent.
	optional bool
}

// required expects exactly one field named name, of type t.
func required(name string, t fieldType) []fieldSpec {
	return []fieldSpec{{name: name, typ: t}}
}

// optional expects at most one field named name, of type t.
func optional(name string, t fieldType) []fieldSpec {
	return []fieldSpec{{name: name, typ: t, optional: true}}
}

// optionalOfType expects at most one field of type t, whatever its name.
func optionalOfType(t fieldType) []fieldSpec {
	return []fieldSpec{{typ: t, optional: true}}
}

func (s fieldSpec) String() string {
	var b strings.Builder
	if s.optional {
		b.WriteString("optional ")
	}
	if s.name != "" {
		fmt.Fprintf(&b, "%q ", s.name)
	}
	b.WriteString(s.typ.desc)
	return b.String()
}

// expectFields checks that msg - named what in findings, such as
// "request" - has exactly the fields groups describe, nothing else.
func expectFields(subject, what string, msg model.Message, groups ...[]fieldSpec) []Finding {
	var specs []fieldSpec
	for _, g := range groups {
		specs = append(specs, g...)
	}

	var findings []Finding
	counts := make([]int, len(specs))
	assigned := make([]bool, len(msg.Fields))

	// A field with a spec's name is that spec's, whatever its type.
	for fi, f := range msg.Fields {
		for si, s := range specs {
			if s.name != "" && s.name == f.Name {
				assigned[fi] = true
				counts[si]++
				if !s.typ.matches(f) {
					findings = append(findings, findingf(subject, "%s field %q is %s, want %s", what, f.Name, typeOf(f), s.typ.desc))
				}
				break
			}
		}
	}
	for fi, f := range msg.Fields {
		if assigned[fi] {
			continue
		}
		si := matchByType(specs, counts, f)
		switch {
		case si == -1:
			findings = append(findings, findingf(subject, "%s field %q is not allowed - want only %s", what, f.Name, joinAnd(specStrings(specs))))
		case specs[si].name != "":
			counts[si]++
			findings = append(findings, findingf(subject, "%s field %q is %s, want it named %q", what, f.Name, typeOf(f), specs[si].name))
		default:
			counts[si]++
		}
	}

	for si, s := range specs {
		switch {
		case counts[si] == 0 && !s.optional:
			findings = append(findings, findingf(subject, "%s has no %s field", what, s))
		case counts[si] > 1:
			findings = append(findings, findingf(subject, "%s has %d %s fields, want at most 1", what, counts[si], strings.TrimPrefix(s.String(), "optional ")))
		}
	}
	return findings
}

// matchByType returns the index of the spec f belongs to by its type
// alone: a named spec no field has filled yet, so f is just misnamed, or
// else a spec of a type alone. -1 if there's none.
func matchByType(specs []fieldSpec, counts []int, f model.Field) int {
	for si, s := range specs {
		if s.name != "" && counts[si] == 0 && s.typ.matches(f) {
			return si
		}
	}
	for si, s := range specs {
		if s.name == "" && s.typ.matches(f) {
			return si
		}
	}
	return -1
}

func specStrings(specs []fieldSpec) []string {
	out := make([]string, len(specs))
	for i, s := range specs {
		out[i] = s.String()
	}
	return out
}

// typeOf describes f's type in findings. For example, "string",
// "ateapi.ObjectRef", or "repeated ateapi.Actor".
func typeOf(f model.Field) string {
	if f.TypeKind == "" {
		if f.TypeDisplay == "" {
			return "untyped"
		}
		return f.TypeDisplay
	}
	if f.Repeated {
		return "repeated " + f.TypeFullName
	}
	return f.TypeFullName
}

// joinAnd joins names as an English list. For example, "a and b", or
// "a, b, and c".
func joinAnd(names []string) string {
	if len(names) <= 2 {
		return strings.Join(names, " and ")
	}
	return strings.Join(names[:len(names)-1], ", ") + ", and " + names[len(names)-1]
}

// Fields that every request or response of their kind has the same way.
var (
	pageSize              = required("page_size", scalar("int32"))
	pageToken             = required("page_token", scalar("string"))
	nextPageToken         = required("next_page_token", scalar("string"))
	optionalCreateOptions = optionalOfType(message(createOptionsTypeFullName))
	optionalDeleteOptions = optionalOfType(message(deleteOptionsTypeFullName))
)

// requestHas checks that m's request has exactly the fields groups
// describe.
func (m method) requestHas(groups ...[]fieldSpec) []Finding {
	return expectFields(subjectOf(m.rpc), "request", m.request, groups...)
}

// responseHas checks that m's response has exactly the fields groups
// describe.
func (m method) responseHas(groups ...[]fieldSpec) []Finding {
	return expectFields(subjectOf(m.rpc), "response", m.response, groups...)
}

// resourceRefFields are how m's request references m's resource: the
// resource's own ObjectRef, or for a subresource, the parent's plus a name
// (style guide section #2.4).
func (m method) resourceRefFields() []fieldSpec {
	if !m.isSubresource() {
		return required(fieldNameForResource(m.resource.Name), message(objectRefTypeFullName))
	}
	return append(m.parentRefField(), required("name", scalar("string"))...)
}

// parentRefField is how m's request identifies m's parent: the parent's
// ObjectRef, named after it. None for a top-level resource, which has no
// parent, or for a subresource of Global, which has no identity.
func (m method) parentRefField() []fieldSpec {
	if !m.isSubresource() || m.parent == model.GlobalParent {
		return nil
	}
	return required(fieldNameForResource(m.parent), message(objectRefTypeFullName))
}

// embeddedResourceField is m's resource, embedded whole in a Create or
// Update request.
func (m method) embeddedResourceField() []fieldSpec {
	return required(fieldNameForResource(m.resource.Name), message(m.resource.FullName))
}

const (
	objectRefTypeFullName        = "ateapi.ObjectRef"
	resourceMetadataTypeFullName = "ateapi.ResourceMetadata"
	createOptionsTypeFullName    = "ateapi.CreateOptions"
	deleteOptionsTypeFullName    = "ateapi.DeleteOptions"
)

// isMessageOf reports whether f is a singular or repeated field of the
// message type typeFullName.
func isMessageOf(f model.Field, typeFullName string) bool {
	return f.TypeKind == "message" && f.TypeFullName == typeFullName
}

func findFieldByName(m model.Message, name string) *model.Field {
	for i := range m.Fields {
		if m.Fields[i].Name == name {
			return &m.Fields[i]
		}
	}
	return nil
}

func fieldTypeDescription(f model.Field) string {
	if f.TypeKind == "" {
		return "not a message"
	}
	return f.TypeFullName
}
