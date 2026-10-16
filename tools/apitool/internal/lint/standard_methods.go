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
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

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

var GetRequestShape = Rule{
	Name: "get-request-shape",
	Description: "A Get method's request has exactly one field, of type ObjectRef and named after the resource's snake_case type name, identifying the resource. " +
		`For a subresource, it has the parent's ObjectRef, named after the parent (none for Global), plus a string "name" field - nothing else.`,
	Check: forEachStandardMethod(checkGetRequest, verbGet),
}

func checkGetRequest(m method) []Finding {
	return m.requestHas(m.resourceRefFields())
}

var ListMethodShape = Rule{
	Name: "list-method-shape",
	Description: "A List method's request has a ListOptions options field, plus an atespace field for an atespace-scoped top-level resource, " +
		"or for a subresource an ObjectRef field named after the parent (none for Global). Its response has a repeated field of the resource, " +
		"named after the resource's plural, plus a next_page_token field - nothing else.",
	Check: forEachStandardMethod(checkListMethod, verbList),
}

var ListResponseNameMatchesMethod = Rule{
	Name:        "list-response-name-matches-method",
	Description: `Every "List*" method's response message is named "{MethodName}Response".`,
	Check:       forEachStandardMethod(checkListResponseName, verbList),
}

func checkListMethod(m method) []Finding {
	return append(
		m.requestHas(m.listScopeField(), listOptions),
		m.responseHas(m.resourcePageField(), nextPageToken)...,
	)
}

func checkListResponseName(m method) []Finding {
	if got, want := m.response.FullName.Name(), m.rpc.Name+"Response"; got != want {
		return []Finding{m.findingf("response message is named %q, want %q", got, want)}
	}
	return nil
}

var CreateRequestShape = Rule{
	Name: "create-request-shape",
	Description: "A Create method's request has exactly one field, of the resource's own type and named after its snake_case type name, plus an optional CreateOptions field named \"options\" - nothing else. " +
		"For a subresource, it also has the parent's ObjectRef, named after the parent (none for Global).",
	Check: forEachStandardMethod(checkCreateRequest, verbCreate),
}

func checkCreateRequest(m method) []Finding {
	return m.requestHas(m.parentRefField(), m.embeddedResourceField(), optionalCreateOptions)
}

var UpdateRequestShape = Rule{
	Name: "update-request-shape",
	Description: "An Update method's request has exactly one field, of the resource's own type and named after its snake_case type name - nothing else. " +
		"For a subresource, it also has the parent's ObjectRef, named after the parent (none for Global).",
	Check: forEachStandardMethod(checkUpdateRequest, verbUpdate),
}

func checkUpdateRequest(m method) []Finding {
	return m.requestHas(m.parentRefField(), m.embeddedResourceField())
}

var DeleteRequestShape = Rule{
	Name: "delete-request-shape",
	Description: "A Delete method's request has exactly one ObjectRef field, named after the resource's snake_case type name, plus an optional DeleteOptions field named \"options\" - nothing else. " +
		`For a subresource, the parent's ObjectRef, named after the parent (none for Global), plus a string "name" field replace the resource's ObjectRef.`,
	Check: forEachStandardMethod(checkDeleteRequest, verbDelete),
}

func checkDeleteRequest(m method) []Finding {
	return m.requestHas(m.resourceRefFields(), optionalDeleteOptions)
}

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
	return checkFields(opts.FullName, "DeleteOptions", opts,
		optional("version", scalar(protoreflect.Int64Kind)),
		optional("uid", scalar(protoreflect.StringKind)),
	), nil
}

var SingletonNoList = Rule{
	Name:        "singleton-no-list",
	Description: "A singleton resource has no List method.",
	Check:       forEachStandardMethod(checkSingletonList, verbList),
}

func checkSingletonList(m method) []Finding {
	if !m.resource.ResourceAnnotation.Singleton {
		return nil
	}
	return []Finding{m.findingf("lists %s, which is a singleton - a singleton has no List method", m.resource.FullName)}
}

// Fields that every request or response of their kind has the same way.
var (
	listOptions           = required("options", message(listOptionsTypeFullName))
	nextPageToken         = required("next_page_token", scalar(protoreflect.StringKind))
	optionalCreateOptions = optional("options", message(createOptionsTypeFullName))
	optionalDeleteOptions = optional("options", message(deleteOptionsTypeFullName))
)

// requestHas checks that m's request has exactly the fields want lists.
func (m method) requestHas(want ...wantFields) []Finding {
	return checkFields(subjectOf(m.rpc), "request", m.request, want...)
}

// responseHas checks that m's response has exactly the fields want lists.
func (m method) responseHas(want ...wantFields) []Finding {
	return checkFields(subjectOf(m.rpc), "response", m.response, want...)
}

// resourceRefFields are how m's request references m's resource: the
// resource's own ObjectRef, or for a subresource, the parent's plus a name
// (style guide section #2.4).
func (m method) resourceRefFields() wantFields {
	if !m.isSubresource() {
		return required(snakeCase(m.resource.Name), message(objectRefTypeFullName))
	}
	return append(m.parentRefField(), required("name", scalar(protoreflect.StringKind))...)
}

// parentRefField is how m's request identifies m's parent: the parent's
// ObjectRef, named after it. None for a top-level resource, which has no
// parent, or for a subresource of Global, which has no identity.
func (m method) parentRefField() wantFields {
	if !m.isSubresource() || m.parent == model.GlobalParent {
		return nil
	}
	return required(snakeCase(m.parent), message(objectRefTypeFullName))
}

// embeddedResourceField is m's resource, embedded whole in a Create or
// Update request.
func (m method) embeddedResourceField() wantFields {
	return required(snakeCase(m.resource.Name), message(m.resource.FullName))
}

// listScopeField is how m's List request scopes the listing: an atespace
// for an atespace-scoped top-level resource, nothing for a global one, or the
// parent's ObjectRef for a subresource. Sorting and filtering per AIP-132 are
// not supported.
func (m method) listScopeField() wantFields {
	switch {
	case m.isSubresource():
		return m.parentRefField()
	case m.resource.ResourceAnnotation.Scope == ateapipb.Scope_SCOPE_ATESPACE:
		return required("atespace", scalar(protoreflect.StringKind))
	default:
		return nil
	}
}

// resourcePageField is the repeated field of m's List response holding a
// page of resources, named after their plural.
func (m method) resourcePageField() wantFields {
	return required(snakeCase(m.resource.ResourceAnnotation.Plural), repeatedMessage(m.resource.FullName))
}

const (
	objectRefTypeFullName     protoreflect.FullName = "ateapi.ObjectRef"
	createOptionsTypeFullName protoreflect.FullName = "ateapi.CreateOptions"
	deleteOptionsTypeFullName protoreflect.FullName = "ateapi.DeleteOptions"
	listOptionsTypeFullName   protoreflect.FullName = "ateapi.ListOptions"
)
