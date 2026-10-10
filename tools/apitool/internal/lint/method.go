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

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// method is a standard method with everything a rule needs resolved: its
// resource, how its name reads against that resource, and its messages.
type method struct {
	// verb is the standard verb the name starts with. For example, verbGet
	// for GetActorEgressPolicy.
	verb verb
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

func findingf(subject, format string, args ...any) Finding {
	return Finding{Subject: subject, Message: fmt.Sprintf(format, args...)}
}

// subjectOf names m in findings. For example, "ateapi.Control.GetActor".
func subjectOf(m model.Method) string {
	return m.ServiceFullName + "." + m.Name
}

// forEachStandardMethod returns a Rule.Check running check on every
// standard method whose verb is one of verbs.
func forEachStandardMethod(check func(method) []Finding, verbs ...verb) func(*model.API) ([]Finding, error) {
	return func(api *model.API) ([]Finding, error) {
		resources, err := model.Resources(api)
		if err != nil {
			return nil, err
		}
		messagesByName := api.MessagesByFullName()

		var findings []Finding
		for _, r := range resources {
			for _, rpc := range r.Methods {
				v, parent, ok := parseStandardMethod(rpc.Name, r.Message)
				if !ok || !slices.Contains(verbs, v) {
					continue
				}
				m := method{verb: v, parent: parent, rpc: rpc, resource: r.Message}
				if m.request, ok = messagesByName[rpc.InputName]; !ok {
					return nil, fmt.Errorf("%s: request type %s is not declared in this API", subjectOf(rpc), rpc.InputName)
				}
				if m.response, ok = messagesByName[rpc.OutputName]; !ok {
					return nil, fmt.Errorf("%s: response type %s is not declared in this API", subjectOf(rpc), rpc.OutputName)
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

// verb is a standard method's verb, which its name starts with (style
// guide section #3).
type verb string

const (
	verbGet    verb = "Get"
	verbList   verb = "List"
	verbCreate verb = "Create"
	verbUpdate verb = "Update"
	verbDelete verb = "Delete"
)

// standardVerbs lists every standard verb.
var standardVerbs = []verb{verbGet, verbList, verbCreate, verbUpdate, verbDelete}

// parseStandardMethod parses methodName as a standard method on resource:
// the verb, then one of resource's parents (none for a top-level
// resource), then resource as resourceNameFor names it. It returns the
// verb and the parent ("" for a top-level resource). ok is false for a
// custom method, and for a standard method named any other way.
func parseStandardMethod(methodName string, resource model.Message) (v verb, parent string, ok bool) {
	parents := resource.Resource.Parents
	if len(parents) == 0 {
		parents = []string{""}
	}
	for _, p := range parents {
		for _, sv := range standardVerbs {
			if methodName == string(sv)+p+resourceNameFor(sv, resource) {
				return sv, p, true
			}
		}
	}
	return "", "", false
}

// resourceNameFor is how a method with verb v names resource: by its
// plural for List, and by its name otherwise. For example, "Actors" in
// ListActors, but "Actor" in GetActor.
func resourceNameFor(v verb, resource model.Message) string {
	if v == verbList {
		return pluralName(resource.Name)
	}
	return resource.Name
}
