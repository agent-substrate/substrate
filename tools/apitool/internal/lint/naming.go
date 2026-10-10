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
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

var pascalBoundary = regexp.MustCompile(`([a-z0-9])([A-Z])`)

func snakeCase(s string) string {
	return strings.ToLower(pascalBoundary.ReplaceAllString(s, "${1}_${2}"))
}

func fieldNameForResource(resourceName string) string {
	return snakeCase(resourceName)
}

// pluralName pluralizes a resource name the regular English way. For
// example, "ActorAssignments" for "ActorAssignment", or "AccessPolicies"
// for "AccessPolicy".
func pluralName(name string) string {
	if base, ok := strings.CutSuffix(name, "y"); ok && base != "" && !strings.ContainsRune("aeiou", rune(base[len(base)-1])) {
		return base + "ies"
	}
	if strings.HasSuffix(name, "s") {
		return name + "es"
	}
	return name + "s"
}

// RequestNameMatchesMethod requires every method's request to be named
// "{MethodName}Request".
var RequestNameMatchesMethod = Rule{
	Name:        "request-name-matches-method",
	Description: `Every method's request message is named "{MethodName}Request".`,
	Check:       checkRequestNameMatchesMethod,
}

func checkRequestNameMatchesMethod(api *model.API) ([]Finding, error) {
	messagesByName := api.MessagesByFullName()

	var findings []Finding
	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			subject := method.ServiceFullName + "." + method.Name
			want := method.Name + "Request"

			req, ok := messagesByName[method.InputName]
			if !ok {
				findings = append(findings, Finding{Subject: subject, Message: "request type " + method.InputName + " not found"})
				continue
			}
			if req.Name != want {
				findings = append(findings, Finding{
					Subject: subject,
					Message: fmt.Sprintf("request message is named %q, want %q", req.Name, want),
				})
			}
		}
	}
	return findings, nil
}

// SubresourceNaming requires a subresource's message to be named for the
// subresource alone, and each of its standard methods to be named the verb,
// then one of its parents, then the subresource - or, for List, its plural.
var SubresourceNaming = Rule{
	Name: "subresource-naming",
	Description: "A subresource's message is named without its parent type, and its standard methods are named the verb, " +
		"then one of its parents (Global for the whole installation), then the subresource's name - or, for List, its plural.",
	Check: forEachResource(checkSubresourceNaming),
}

func checkSubresourceNaming(r model.Resource) []Finding {
	parents := r.Message.Resource.Parents
	if len(parents) == 0 {
		return nil
	}
	var findings []Finding
	for _, parent := range parents {
		if parent != model.GlobalParent && strings.HasPrefix(r.Message.Name, parent) {
			findings = append(findings, findingf(r.Message.FullName, "message is named with its parent type %s - name it for the subresource alone", parent))
		}
	}
	for _, rpc := range r.Methods {
		v, ok := standardVerbPrefix(rpc.Name)
		if !ok {
			continue // a custom method - not this rule's concern.
		}
		if _, _, ok := parseStandardMethod(rpc.Name, r.Message); ok {
			continue
		}
		findings = append(findings, findingf(subjectOf(rpc), "want %s, then one of the parents %s, then %s", v, strings.Join(parents, "/"), resourceNameFor(v, r.Message)))
	}
	return findings
}

// standardVerbPrefix returns the standard verb methodName starts with, if
// it's followed by another word. For example, "Get" for
// "GetActorEgressPolicy", but nothing for "Getaway".
func standardVerbPrefix(methodName string) (verb, bool) {
	for _, v := range standardVerbs {
		rest, ok := strings.CutPrefix(methodName, string(v))
		if r, _ := utf8.DecodeRuneInString(rest); ok && unicode.IsUpper(r) {
			return v, true
		}
	}
	return "", false
}

// EnumZeroValueUnspecified requires every enum's zero value to be named
// "{EnumName}_UNSPECIFIED".
var EnumZeroValueUnspecified = Rule{
	Name:        "enum-zero-value-unspecified",
	Description: `Every enum's zero value is named "{EnumName}_UNSPECIFIED".`,
	Check:       forEachEnum(checkEnumZeroValueUnspecified),
}

// EnumValuesPrefixed requires every enum's values - top-level or nested -
// to be prefixed with the enum's own name.
var EnumValuesPrefixed = Rule{
	Name:        "enum-values-prefixed",
	Description: `Every enum's values are prefixed with "{ENUM_NAME}_".`,
	Check:       forEachEnum(checkEnumValuesPrefixed),
}

func checkEnumZeroValueUnspecified(e model.Enum) []Finding {
	want := strings.ToUpper(snakeCase(lastNameSegment(e.Name))) + "_UNSPECIFIED"
	zero := e.ValueByNumber(0)
	switch {
	case zero == nil:
		return []Finding{findingf(e.FullName, "has no value numbered 0")}
	case zero.Name != want:
		return []Finding{findingf(e.FullName, "zero value is named %q, want %q", zero.Name, want)}
	}
	return nil
}

func checkEnumValuesPrefixed(e model.Enum) []Finding {
	prefix := strings.ToUpper(snakeCase(lastNameSegment(e.Name))) + "_"
	var findings []Finding
	for _, v := range e.Values {
		if !strings.HasPrefix(v.Name, prefix) {
			findings = append(findings, findingf(e.FullName+"."+v.Name, "not prefixed with %q", prefix))
		}
	}
	return findings
}

func lastNameSegment(name string) string {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[i+1:]
	}
	return name
}
