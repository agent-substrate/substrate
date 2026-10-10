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
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// StandardMethodReturnsResource requires a Get/Create/Update/Delete
// method's response to be the resource itself, never a wrapper message.
var StandardMethodReturnsResource = Rule{
	Name:        "standard-method-returns-resource",
	Description: "A Get/Create/Update/Delete method's response is the resource itself, not a wrapper message.",
	Check:       forEachStandardMethod(checkReturnsResource, verbGet, verbCreate, verbUpdate, verbDelete),
}

func checkReturnsResource(m method) []Finding {
	if m.response.FullName == m.resource.FullName {
		return nil
	}
	return []Finding{m.findingf("%s method returns %s, want the resource itself (%s)", m.verb, m.response.FullName, m.resource.FullName)}
}

// GetRequestShape requires a Get method's request to identify the
// resource with exactly one field, of type ObjectRef and named after the
// resource's snake_case type name. A subresource's Get request instead
// identifies it by its parent's ObjectRef, named after the parent (none for
// Global), plus a string "name" field.
var GetRequestShape = Rule{
	Name: "get-request-shape",
	Description: "A Get method's request has exactly one field, of type ObjectRef and named after the resource's snake_case type name, identifying the resource. " +
		`For a subresource, it has the parent's ObjectRef, named after the parent (none for Global), plus a string "name" field - nothing else.`,
	Check: forEachStandardMethod(checkGetRequest, verbGet),
}

func checkGetRequest(m method) []Finding {
	return m.requestHas(m.resourceRefFields())
}

// ListMethodShape requires a List method's request to have exactly
// page_size/page_token fields plus, for a top-level resource, an optional
// atespace field, or for a subresource, an ObjectRef field named after the
// parent (none for Global). Its response must have exactly a repeated
// message field plus a next_page_token field - nothing else.
var ListMethodShape = Rule{
	Name: "list-method-shape",
	Description: "A List method's request has page_size/page_token fields, plus an optional atespace field for a top-level resource, " +
		"or for a subresource an ObjectRef field named after the parent (none for Global). Its response has a repeated message field, " +
		"named after the method's own plural, plus a next_page_token field - nothing else.",
	Check: forEachStandardMethod(checkListMethod, verbList),
}

// ListResponseNameMatchesMethod requires a "List*" method's response to be
// named "{MethodName}Response".
var ListResponseNameMatchesMethod = Rule{
	Name:        "list-response-name-matches-method",
	Description: `Every "List*" method's response message is named "{MethodName}Response".`,
	Check:       forEachStandardMethod(checkListResponseName, verbList),
}

func checkListMethod(m method) []Finding {
	return append(
		m.requestHas(m.listScopeField(), pageSize, pageToken),
		m.responseHas(m.resourcePageField(), nextPageToken)...,
	)
}

// listScopeField is how m's List request scopes the listing: an optional
// atespace for a top-level resource, or the parent's ObjectRef for a
// subresource. Sorting and filtering per AIP-132 are not supported.
func (m method) listScopeField() []fieldSpec {
	if !m.isSubresource() {
		return optional("atespace", scalar("string"))
	}
	return m.parentRefField()
}

// resourcePageField is the repeated field of m's List response holding a
// page of resources, named after their plural.
func (m method) resourcePageField() []fieldSpec {
	return required(snakeCase(pluralName(m.resource.Name)), repeatedMessage)
}

func checkListResponseName(m method) []Finding {
	if want := m.rpc.Name + "Response"; m.response.Name != want {
		return []Finding{m.findingf("response message is named %q, want %q", m.response.Name, want)}
	}
	return nil
}

// CreateRequestShape requires a Create method's request to embed the
// resource with exactly one field, named after the resource's snake_case
// type name, plus an optional CreateOptions field for non-resource controls.
// A subresource's Create request also has its parent's ObjectRef, named
// after the parent (none for Global).
var CreateRequestShape = Rule{
	Name: "create-request-shape",
	Description: "A Create method's request has exactly one field, of the resource's own type and named after its snake_case type name, plus an optional CreateOptions field - nothing else. " +
		"For a subresource, it also has the parent's ObjectRef, named after the parent (none for Global).",
	Check: forEachStandardMethod(checkCreateRequest, verbCreate),
}

func checkCreateRequest(m method) []Finding {
	return m.requestHas(m.parentRefField(), m.embeddedResourceField(), optionalCreateOptions)
}

// UpdateRequestShape requires an Update method's request to embed the
// resource with exactly one field, named after the resource's snake_case
// type name - nothing else.
// A subresource's Update request also has its parent's ObjectRef, named
// after the parent (none for Global).
var UpdateRequestShape = Rule{
	Name: "update-request-shape",
	Description: "An Update method's request has exactly one field, of the resource's own type and named after its snake_case type name - nothing else. " +
		"For a subresource, it also has the parent's ObjectRef, named after the parent (none for Global).",
	Check: forEachStandardMethod(checkUpdateRequest, verbUpdate),
}

func checkUpdateRequest(m method) []Finding {
	return m.requestHas(m.parentRefField(), m.embeddedResourceField())
}

// DeleteRequestShape requires a Delete method's request to identify the
// resource with exactly one ObjectRef field, named after the resource's
// snake_case type name, plus an optional DeleteOptions field for
// preconditions. A subresource's Delete request instead identifies it by its
// parent's ObjectRef, named after the parent (none for Global), plus a
// string "name" field.
var DeleteRequestShape = Rule{
	Name: "delete-request-shape",
	Description: "A Delete method's request has exactly one ObjectRef field, named after the resource's snake_case type name, plus an optional DeleteOptions field - nothing else. " +
		`For a subresource, the parent's ObjectRef, named after the parent (none for Global), plus a string "name" field replace the resource's ObjectRef.`,
	Check: forEachStandardMethod(checkDeleteRequest, verbDelete),
}

func checkDeleteRequest(m method) []Finding {
	return m.requestHas(m.resourceRefFields(), optionalDeleteOptions)
}

// DeleteOptionsShape requires the shared ateapi.DeleteOptions message to
// have only its two documented precondition fields: version (int64) and
// uid (string) - nothing else. Presence of either field is not required;
// this rule only bounds what's allowed.
var DeleteOptionsShape = Rule{
	Name:        "delete-options-shape",
	Description: "The shared ateapi.DeleteOptions message has only version (int64) and uid (string) fields - nothing else.",
	Check:       checkDeleteOptionsShape,
}

func checkDeleteOptionsShape(api *model.API) ([]Finding, error) {
	opts, ok := api.MessagesByFullName()[deleteOptionsTypeFullName]
	if !ok {
		return nil, nil // DeleteOptions isn't declared in this API - nothing to check.
	}
	return expectFields(opts.FullName, "DeleteOptions", opts,
		optional("version", scalar("int64")),
		optional("uid", scalar("string")),
	), nil
}

// SingletonNoList forbids a List method on a singleton resource, which has
// at most one instance per parent.
var SingletonNoList = Rule{
	Name:        "singleton-no-list",
	Description: "A singleton resource has no List method.",
	Check:       forEachStandardMethod(checkSingletonList, verbList),
}

func checkSingletonList(m method) []Finding {
	if !m.resource.Resource.Singleton {
		return nil
	}
	return []Finding{m.findingf("lists %s, which is a singleton - a singleton has no List method", m.resource.FullName)}
}
