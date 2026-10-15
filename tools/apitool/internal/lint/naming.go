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
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

var RequestNameMatchesMethod = Rule{
	Name:        "request-name-matches-method",
	Description: `Every method's request message is named "{MethodName}Request".`,
	Check:       forEachMethod(checkRequestNameMatchesMethod),
}

func checkRequestNameMatchesMethod(rpc model.Method) []Finding {
	if got, want := rpc.InputName.Name(), rpc.Name+"Request"; got != want {
		return []Finding{findingf(subjectOf(rpc), "request message is named %q, want %q", got, want)}
	}
	return nil
}

var CustomResponseNameMatchesMethod = Rule{
	Name:        "custom-response-name-matches-method",
	Description: `Every custom method's response message is named "{MethodName}Response", not the resource itself.`,
	Check:       forEachResource(checkCustomResponseName),
}

func checkCustomResponseName(r model.Resource) []Finding {
	var findings []Finding
	for _, rpc := range r.Methods {
		if _, ok := standardVerbPrefix(rpc.Name); ok {
			continue
		}
		if got, want := rpc.OutputName.Name(), rpc.Name+"Response"; got != want {
			findings = append(findings, findingf(subjectOf(rpc), "response message is named %q, want %q", got, want))
		}
	}
	return findings
}

var SubresourceNaming = Rule{
	Name:        "subresource-naming",
	Description: "A subresource's message is named without its parent type.",
	Check:       forEachResource(checkSubresourceNaming),
}

func checkSubresourceNaming(r model.Resource) []Finding {
	var findings []Finding
	for _, parent := range r.Message.ResourceAnnotation.Parents {
		if strings.HasPrefix(r.Message.Name, parent) {
			findings = append(findings, findingf(r.Message.FullName, "message is named with its parent type %s - name it for the subresource alone", parent))
		}
	}
	return findings
}

// standardVerbPrefix returns the standard verb methodName starts with, if
// it's followed by another word. For example, "Get" for
// "GetActorEgressPolicy", but nothing for "Getaway".
func standardVerbPrefix(methodName protoreflect.Name) (verb, bool) {
	for _, v := range standardVerbs {
		rest, ok := strings.CutPrefix(string(methodName), string(v))
		if r, _ := utf8.DecodeRuneInString(rest); ok && unicode.IsUpper(r) {
			return v, true
		}
	}
	return "", false
}

var EnumZeroValueUnspecified = Rule{
	Name:        "enum-zero-value-unspecified",
	Description: `Every enum's zero value is named "{EnumName}_UNSPECIFIED".`,
	Check:       forEachEnum(checkEnumZeroValueUnspecified),
}

var EnumValuesPrefixed = Rule{
	Name:        "enum-values-prefixed",
	Description: `Every enum's values are prefixed with "{ENUM_NAME}_".`,
	Check:       forEachEnum(checkEnumValuesPrefixed),
}

func checkEnumZeroValueUnspecified(e model.Enum) []Finding {
	want := strings.ToUpper(snakeCase(string(e.FullName.Name()))) + "_UNSPECIFIED"
	zero := e.ValueByNumber(0)
	switch {
	case zero == nil:
		return []Finding{findingf(e.FullName, "has no value numbered 0")}
	case string(zero.Name) != want:
		return []Finding{findingf(e.FullName, "zero value is named %q, want %q", zero.Name, want)}
	}
	return nil
}

func checkEnumValuesPrefixed(e model.Enum) []Finding {
	prefix := strings.ToUpper(snakeCase(string(e.FullName.Name()))) + "_"
	var findings []Finding
	for _, v := range e.Values {
		if !strings.HasPrefix(string(v.Name), prefix) {
			findings = append(findings, findingf(e.FullName.Append(v.Name), "not prefixed with %q", prefix))
		}
	}
	return findings
}

var pascalBoundary = regexp.MustCompile(`([a-z0-9])([A-Z])`)

// snakeCase converts a PascalCase name to snake_case. For example,
// "actor_assignment" for "ActorAssignment".
func snakeCase(s string) string {
	return strings.ToLower(pascalBoundary.ReplaceAllString(s, "${1}_${2}"))
}
