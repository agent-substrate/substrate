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
	"slices"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// typeMatcher reports whether a field has the wanted type, including
// whether it's repeated, and names that type in findings.
type typeMatcher struct {
	// label names the type in findings. For example, "string" or
	// "ateapi.ObjectRef".
	label   string
	matches func(model.Field) bool
}

func scalar(kind protoreflect.Kind) typeMatcher {
	return typeMatcher{kind.String(), func(f model.Field) bool {
		return !f.Repeated && f.Kind == kind
	}}
}

func message(typeFullName protoreflect.FullName) typeMatcher {
	return typeMatcher{string(typeFullName), func(f model.Field) bool {
		return isMessageOf(f, typeFullName)
	}}
}

func repeatedMessage(typeFullName protoreflect.FullName) typeMatcher {
	return typeMatcher{"repeated " + string(typeFullName), func(f model.Field) bool {
		return f.Repeated && f.Kind == protoreflect.MessageKind && f.TypeFullName == typeFullName
	}}
}

// wantField is one field a message should have.
type wantField struct {
	// name is the field's wanted name.
	name string
	typ  typeMatcher
	// optional lets the field be absent.
	optional bool
}

// wantFields is some of the fields a message should have.
type wantFields []wantField

// required wants exactly one field named name, of type t.
func required(name string, t typeMatcher) wantFields {
	return wantFields{{name: name, typ: t}}
}

// optional wants at most one field named name, of type t.
func optional(name string, t typeMatcher) wantFields {
	return wantFields{{name: name, typ: t, optional: true}}
}

func (w wantField) String() string {
	var b strings.Builder
	if w.optional {
		b.WriteString("optional ")
	}
	fmt.Fprintf(&b, "%q %s", w.name, w.typ.label)
	return b.String()
}

// checkFields checks that msg has exactly the fields want lists, and
// nothing else. role names msg in findings, such as "request".
func checkFields(subject protoreflect.FullName, role string, msg model.Message, want ...wantFields) []Finding {
	all := slices.Concat(want...)

	var findings []Finding
	counts := make([]int, len(all))
	assigned := make([]bool, len(msg.Fields))

	// A field with a wanted name is that wantField's, whatever its type.
	for fi, f := range msg.Fields {
		for wi, w := range all {
			if w.name == string(f.Name) {
				assigned[fi] = true
				counts[wi]++
				if !w.typ.matches(f) {
					findings = append(findings, findingf(subject, "%s field %q is %s, want %s", role, f.Name, typeOf(f), w.typ.label))
				}
				break
			}
		}
	}
	for fi, f := range msg.Fields {
		if assigned[fi] {
			continue
		}
		if wi := matchByType(all, counts, f); wi == -1 {
			findings = append(findings, findingf(subject, "%s field %q is not allowed - want only %s", role, f.Name, joinAnd(wantStrings(all))))
		} else {
			counts[wi]++
			findings = append(findings, findingf(subject, "%s field %q is %s, want it named %q", role, f.Name, typeOf(f), all[wi].name))
		}
	}

	for wi, w := range all {
		switch {
		case counts[wi] == 0 && !w.optional:
			findings = append(findings, findingf(subject, "%s has no %s field", role, w))
		case counts[wi] > 1:
			findings = append(findings, findingf(subject, "%s has %d %s fields, want at most 1", role, counts[wi], strings.TrimPrefix(w.String(), "optional ")))
		}
	}
	return findings
}

// matchByType returns the index of the wantField f is misnamed for: one of
// f's type that no field has filled yet. -1 if there's none.
func matchByType(want wantFields, counts []int, f model.Field) int {
	for wi, w := range want {
		if counts[wi] == 0 && w.typ.matches(f) {
			return wi
		}
	}
	return -1
}

func wantStrings(want wantFields) []string {
	out := make([]string, len(want))
	for i, w := range want {
		out[i] = w.String()
	}
	return out
}

// typeOf describes f's type in findings. For example, "string",
// "ateapi.ObjectRef", or "repeated ateapi.Actor".
func typeOf(f model.Field) string {
	if f.TypeFullName == "" {
		return f.TypeDisplay
	}
	if f.Repeated {
		return "repeated " + string(f.TypeFullName)
	}
	return string(f.TypeFullName)
}

// joinAnd joins names as an English list. For example, "a and b", or
// "a, b, and c".
func joinAnd(names []string) string {
	if len(names) <= 2 {
		return strings.Join(names, " and ")
	}
	return strings.Join(names[:len(names)-1], ", ") + ", and " + names[len(names)-1]
}

// isMessageOf reports whether f is a singular field of the message type
// typeFullName.
func isMessageOf(f model.Field, typeFullName protoreflect.FullName) bool {
	return !f.Repeated && f.Kind == protoreflect.MessageKind && f.TypeFullName == typeFullName
}
