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

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// method is a standard method with everything a rule needs resolved: its
// resource, how its name reads against that resource, and its messages.
type method struct {
	// verb is the standard verb the name starts with. For example, model.VerbGet
	// for GetActorEgressPolicy.
	verb model.Verb
	// parent is the parent the name names after the verb, for a
	// subresource method, or "" for a top-level resource's. For example,
	// "Actor" for GetActorEgressPolicy, or "Global" for
	// GetGlobalAccessPolicy.
	parent   string
	rpc      model.Method
	resource model.Message
	request  model.Message
	response model.Message
}

func (m method) isSubresource() bool {
	return m.parent != ""
}

// findingf reports a finding about m.
func (m method) findingf(format string, args ...any) Finding {
	return findingf(subjectOf(m.rpc), format, args...)
}

func findingf(subject protoreflect.FullName, format string, args ...any) Finding {
	return Finding{Subject: string(subject), Message: fmt.Sprintf(format, args...)}
}

// subjectOf names m in findings. For example, "ateapi.Control.GetActor".
func subjectOf(m model.Method) protoreflect.FullName {
	return m.ServiceFullName.Append(m.Name)
}

// forEachStandardMethod returns a Rule.Check running check on every
// standard method whose verb is one of verbs.
func forEachStandardMethod(check func(method) []Finding, verbs ...model.Verb) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		resources, err := model.Resources(api)
		if err != nil {
			return nil, err
		}
		messagesByName := api.MessagesByFullName()

		var findings []Finding
		for _, r := range resources {
			for _, rpc := range r.Methods {
				prefix, ok := standardVerbPrefix(rpc.Name)
				if !ok {
					continue
				}
				v, parent, ok := model.ParseStandardMethod(rpc.Name, r.Message)
				if !ok {
					return nil, fmt.Errorf("%s: a method named with %s is a standard method - %s, or name it with a custom verb", subjectOf(rpc), prefix, wantStandardName(prefix, r.Message))
				}
				if !slices.Contains(verbs, v) {
					continue
				}
				m := method{verb: v, parent: parent, rpc: rpc, resource: r.Message}
				if m.request, ok = messagesByName[rpc.InputName]; !ok {
					m.request = model.Message{FullName: rpc.InputName, Name: string(rpc.InputName.Name())}
				}
				if m.response, ok = messagesByName[rpc.OutputName]; !ok {
					m.response = model.Message{FullName: rpc.OutputName, Name: string(rpc.OutputName.Name())}
				}
				findings = append(findings, check(m)...)
			}
		}
		return findings, nil
	}
}

// forEachResource returns a Rule.Check running check on every resource.
func forEachResource(check func(model.Resource) []Finding) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		resources, err := model.Resources(api)
		if err != nil {
			return nil, err
		}
		var findings []Finding
		for _, r := range resources {
			findings = append(findings, check(r)...)
		}
		return findings, nil
	}
}

// forEachMessage returns a Rule.Check running check on every message.
func forEachMessage(check func(model.Message) []Finding) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		var findings []Finding
		for _, m := range api.Messages {
			findings = append(findings, check(m)...)
		}
		return findings, nil
	}
}

// forEachField returns a Rule.Check running check on every field of every
// message.
func forEachField(check func(model.Message, model.Field) []Finding) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		var findings []Finding
		for _, m := range api.Messages {
			for _, f := range m.Fields {
				findings = append(findings, check(m, f)...)
			}
		}
		return findings, nil
	}
}

// forEachMethod returns a Rule.Check running check on every method of
// every service.
func forEachMethod(check func(model.Method) []Finding) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		var findings []Finding
		for _, svc := range api.Services {
			for _, rpc := range svc.Methods {
				findings = append(findings, check(rpc)...)
			}
		}
		return findings, nil
	}
}

// allOf returns a Rule.Check running every one of checks, in order, and
// joining their findings.
func allOf(checks ...func(*model.API) ([]Finding, error)) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		var findings []Finding
		for _, check := range checks {
			f, err := check(api)
			if err != nil {
				return nil, err
			}
			findings = append(findings, f...)
		}
		return findings, nil
	}
}

// forEachEnum returns a Rule.Check running check on every enum.
func forEachEnum(check func(model.Enum) []Finding) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		var findings []Finding
		for _, e := range api.Enums {
			findings = append(findings, check(e)...)
		}
		return findings, nil
	}
}

// wantStandardName describes how a standard method with verb v on resource
// is named. For example, "want Get, then one of the parents Global/Atespace,
// then AccessPolicy".
func wantStandardName(v model.Verb, resource model.Message) string {
	parents := resource.ResourceAnnotation.Parents
	if len(parents) == 0 {
		return fmt.Sprintf("want %s, then %s", v, model.ResourceNameFor(v, resource))
	}
	return fmt.Sprintf("want %s, then one of the parents %s, then %s", v, strings.Join(parents, "/"), model.ResourceNameFor(v, resource))
}
