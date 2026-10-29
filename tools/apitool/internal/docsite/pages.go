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

package docsite

import (
	"fmt"
	"slices"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// resourcePage is a resource as reached through one of its parents, with
// the methods that operate on it there. For example, the AccessPolicy page
// under Global holds GetGlobalAccessPolicy, and the one under Atespace
// holds GetAtespaceAccessPolicy.
type resourcePage struct {
	Message model.Message
	// Parent is the parent this page documents the resource under: another
	// resource's message name, model.GlobalParent, or "" for a top-level
	// resource. For example, "Actor" for EgressPolicy.
	Parent string
	// Singleton is true if Parent has at most one instance of the resource.
	Singleton bool
	// Scope is "atespace" or "global" for a top-level resource, and "" for
	// a subresource, which has its parent's scope.
	Scope string
	// Methods lists the methods operating on the resource under Parent, in
	// declaration order.
	Methods []model.Method
}

// resourcePages lists one page per resource and parent, in api's message
// order: a single page for a top-level resource, and one per parent for a
// subresource. For example, AccessPolicy has a page under Global and one
// under Atespace. It fails wherever model.Resources does, and on a custom
// method of a resource with several parents, since its parent can't be read
// from its name.
func resourcePages(api *model.API) ([]resourcePage, error) {
	resources, err := model.Resources(api)
	if err != nil {
		return nil, err
	}
	var pages []resourcePage
	for _, r := range resources {
		p, err := pagesOf(r)
		if err != nil {
			return nil, err
		}
		pages = append(pages, p...)
	}
	return pages, nil
}

// pagesOf splits r into one page per parent, assigning each method to the
// parent its standard-method name names.
func pagesOf(r model.Resource) ([]resourcePage, error) {
	a := r.Message.ResourceAnnotation
	parents := a.Parents
	if len(parents) == 0 {
		parents = []string{""}
	}
	pages := make([]resourcePage, len(parents))
	for i, parent := range parents {
		pages[i] = resourcePage{
			Message:   r.Message,
			Parent:    parent,
			Singleton: a.Singleton,
			Scope:     scopeName(a.Scope),
		}
	}
	for _, m := range r.Methods {
		i := 0
		if len(parents) > 1 {
			_, parent, ok := model.ParseStandardMethod(m.Name, r.Message)
			if !ok {
				return nil, fmt.Errorf("%s.%s: %s has several parents, but the method isn't a standard method naming one of them", m.ServiceName, m.Name, r.Message.Name)
			}
			i = slices.Index(parents, parent)
		}
		pages[i].Methods = append(pages[i].Methods, m)
	}
	return pages, nil
}

func scopeName(s ateapi.Scope) string {
	switch s {
	case ateapi.Scope_SCOPE_ATESPACE:
		return "atespace"
	case ateapi.Scope_SCOPE_GLOBAL:
		return "global"
	default:
		return ""
	}
}
