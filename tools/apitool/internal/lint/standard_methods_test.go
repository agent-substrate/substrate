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

package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func TestStandardMethodReturnsResource(t *testing.T) {
	tests := []struct {
		name    string
		api     *model.API
		want    []lint.Finding
		wantErr bool
	}{
		{
			name: "Get returns the resource directly",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest")),
		},
		{
			name: "Get returns a wrapper instead",
			api: methodAPI(rpc("GetWidget", "Widget", "GetWidgetResponse"),
				resource("Widget", nil),
				msg("GetWidgetRequest"),
				msg("GetWidgetResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: "Get method returns test.GetWidgetResponse, want the resource itself (test.Widget)"},
			},
		},
		{
			name: "Create returns a wrapper instead",
			api: methodAPI(rpc("CreateWidget", "Widget", "CreateWidgetResponse"),
				resource("Widget", nil),
				msg("CreateWidgetRequest"),
				msg("CreateWidgetResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: "Create method returns test.CreateWidgetResponse, want the resource itself (test.Widget)"},
			},
		},
		{
			name: "Update returns a wrapper instead",
			api: methodAPI(rpc("UpdateWidget", "Widget", "UpdateWidgetResponse"),
				resource("Widget", nil),
				msg("UpdateWidgetRequest"),
				msg("UpdateWidgetResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateWidget", Message: "Update method returns test.UpdateWidgetResponse, want the resource itself (test.Widget)"},
			},
		},
		{
			name: "Delete returns a wrapper instead",
			api: methodAPI(rpc("DeleteWidget", "Widget", "DeleteWidgetResponse"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest"),
				msg("DeleteWidgetResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: "Delete method returns test.DeleteWidgetResponse, want the resource itself (test.Widget)"},
			},
		},
		{
			name: "a List method is not checked",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest"),
				msg("ListWidgetsResponse")),
		},
		{
			name: "returns a type not declared in this API",
			api: methodAPI(model.Method{Name: "GetWidget", Resource: "Widget", InputName: "test.GetWidgetRequest", OutputName: "google.protobuf.Empty"},
				resource("Widget", nil),
				msg("GetWidgetRequest")),
			wantErr: true,
		},
		{
			name: "subresource: returns the resource directly",
			api: methodAPI(rpc("GetGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"}),
				msg("GetGlobalWidgetRequest")),
		},
		{
			name: "subresource: returns a wrapper instead",
			api: methodAPI(rpc("GetGlobalWidget", "Widget", "GetGlobalWidgetResponse"),
				resource("Widget", []string{"Global"}),
				msg("GetGlobalWidgetRequest"),
				msg("GetGlobalWidgetResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.GetGlobalWidget", Message: "Get method returns test.GetGlobalWidgetResponse, want the resource itself (test.Widget)"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.StandardMethodReturnsResource.Check(tt.api)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check() error = %v, want an error: %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetRequestShape(t *testing.T) {
	widgetRef := model.Field{Name: "widget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	misnamedWidgetRef := model.Field{Name: "thing", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	gadgetRef := model.Field{Name: "gadget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	name := model.Field{Name: "name", TypeDisplay: "string"}

	tests := []struct {
		name    string
		api     *model.API
		want    []lint.Finding
		wantErr bool
	}{
		{
			name: "single ObjectRef field",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", widgetRef)),
		},
		{
			name: "no fields",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest")),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: `request has no "widget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "stray field",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", widgetRef, model.Field{Name: "extra"})),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: `request field "extra" is not allowed - want only "widget" ateapi.ObjectRef`},
			},
		},
		{
			name: "single field, wrong type",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", name)),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: `request field "name" is not allowed - want only "widget" ateapi.ObjectRef`},
				{Subject: "test.Control.GetWidget", Message: `request has no "widget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "single field, wrong name",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", misnamedWidgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: `request field "thing" is ateapi.ObjectRef, want it named "widget"`},
			},
		},
		{
			name: "a Create method is not checked",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", model.Field{Name: "widget", TypeKind: "message", TypeFullName: "test.Widget"})),
		},
		{
			name: "subresource: parent ObjectRef and name",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", gadgetRef, name)),
		},
		{
			name: "subresource: no name",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", gadgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request has no "name" string field`},
			},
		},
		{
			name: "subresource: name not a string",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", gadgetRef, model.Field{Name: "name", TypeDisplay: "int64"})),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request field "name" is int64, want string`},
			},
		},
		{
			name: "subresource: no parent ObjectRef",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", name)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request has no "gadget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field not an ObjectRef",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", model.Field{Name: "gadget"}, name)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request field "gadget" is untyped, want ateapi.ObjectRef`},
			},
		},
		{
			name: "subresource: the resource's own ObjectRef instead",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", widgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request field "widget" is ateapi.ObjectRef, want it named "gadget"`},
				{Subject: "test.Control.GetGadgetWidget", Message: `request has no "name" string field`},
			},
		},
		{
			name: "subresource: stray field",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", gadgetRef, name, model.Field{Name: "extra"})),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request field "extra" is not allowed - want only "gadget" ateapi.ObjectRef and "name" string`},
			},
		},
		{
			name: "subresource of Global: name alone",
			api: methodAPI(rpc("GetGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"}),
				msg("GetGlobalWidgetRequest", name)),
		},
		{
			name: "subresource of Global: no name",
			api: methodAPI(rpc("GetGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"}),
				msg("GetGlobalWidgetRequest")),
			want: []lint.Finding{
				{Subject: "test.Control.GetGlobalWidget", Message: `request has no "name" string field`},
			},
		},
		{
			name: "subresource of Global: a parent ObjectRef is a stray field",
			api: methodAPI(rpc("GetGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"}),
				msg("GetGlobalWidgetRequest", model.Field{Name: "global", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}, name)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGlobalWidget", Message: `request field "global" is not allowed - want only "name" string`},
			},
		},
		{
			name: "method with no resource annotation",
			api: methodAPI(rpc("GetWidget", "", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", widgetRef)),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.GetRequestShape.Check(tt.api)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check() error = %v, want an error: %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestListMethodShape(t *testing.T) {
	pageSize := model.Field{Name: "page_size", TypeDisplay: "int32"}
	pageToken := model.Field{Name: "page_token", TypeDisplay: "string"}
	atespace := model.Field{Name: "atespace", TypeDisplay: "string"}
	gadgetRef := model.Field{Name: "gadget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	widgets := model.Field{Name: "widgets", Repeated: true, TypeKind: "message", TypeFullName: "test.Widget"}
	nextPageToken := model.Field{Name: "next_page_token", TypeDisplay: "string"}

	tests := []struct {
		name    string
		api     *model.API
		want    []lint.Finding
		wantErr bool
	}{
		{
			name: "compliant",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", pageSize, pageToken),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "missing page_size and page_token",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest"),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request has no "page_size" int32 field`},
				{Subject: "test.Control.ListWidgets", Message: `request has no "page_token" string field`},
			},
		},
		{
			name: "missing repeated field and next_page_token",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", pageSize, pageToken),
				msg("ListWidgetsResponse", model.Field{Name: "widget", TypeKind: "message", TypeFullName: "test.Widget"})),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "widget" is not allowed - want only "widgets" repeated message and "next_page_token" string`},
				{Subject: "test.Control.ListWidgets", Message: `response has no "widgets" repeated message field`},
				{Subject: "test.Control.ListWidgets", Message: `response has no "next_page_token" string field`},
			},
		},
		{
			name: "repeated scalar field doesn't satisfy the rule",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", pageSize, pageToken),
				msg("ListWidgetsResponse", model.Field{Name: "ids", Repeated: true}, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "ids" is not allowed - want only "widgets" repeated message and "next_page_token" string`},
				{Subject: "test.Control.ListWidgets", Message: `response has no "widgets" repeated message field`},
			},
		},
		{
			name: "repeated field misnamed - doesn't match the resource's plural",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", pageSize, pageToken),
				msg("ListWidgetsResponse", model.Field{Name: "items", Repeated: true, TypeKind: "message", TypeFullName: "test.Widget"}, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "items" is repeated test.Widget, want it named "widgets"`},
			},
		},
		{
			name: "atespace field is allowed on the request",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, pageSize, pageToken),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "stray field on the request - sorting/filtering is not supported",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", pageSize, pageToken, model.Field{Name: "filter"}),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request field "filter" is not allowed - want only optional "atespace" string, "page_size" int32, and "page_token" string`},
			},
		},
		{
			name: "stray field on the response",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", pageSize, pageToken),
				msg("ListWidgetsResponse", widgets, nextPageToken, model.Field{Name: "total_count"})),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "total_count" is not allowed - want only "widgets" repeated message and "next_page_token" string`},
			},
		},
		{
			name: "a Get method is not checked",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest")),
		},
		{
			name: "request not declared in this API",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			wantErr: true,
		},
		{
			name: "response not declared in this API",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", pageSize, pageToken)),
			wantErr: true,
		},
		{
			name: "subresource: compliant",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", gadgetRef, pageSize, pageToken),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "subresource: missing parent field",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", pageSize, pageToken),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `request has no "gadget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field is not an ObjectRef",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", model.Field{Name: "gadget"}, pageSize, pageToken),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `request field "gadget" is untyped, want ateapi.ObjectRef`},
			},
		},
		{
			name: "subresource: atespace field is not allowed",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", gadgetRef, atespace, pageSize, pageToken),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `request field "atespace" is not allowed - want only "gadget" ateapi.ObjectRef, "page_size" int32, and "page_token" string`},
			},
		},
		{
			name: "subresource: repeated field named after the parent too",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", gadgetRef, pageSize, pageToken),
				msg("ListGadgetWidgetsResponse", model.Field{Name: "gadget_widgets", Repeated: true, TypeKind: "message", TypeFullName: "test.Widget"}, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `response field "gadget_widgets" is repeated test.Widget, want it named "widgets"`},
			},
		},
		{
			name: "subresource of Global: compliant",
			api: methodAPI(rpc("ListGlobalWidgets", "Widget", "ListGlobalWidgetsResponse"),
				resource("Widget", []string{"Global"}),
				msg("ListGlobalWidgetsRequest", pageSize, pageToken),
				msg("ListGlobalWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "subresource of Global: parent field is not allowed",
			api: methodAPI(rpc("ListGlobalWidgets", "Widget", "ListGlobalWidgetsResponse"),
				resource("Widget", []string{"Global"}),
				msg("ListGlobalWidgetsRequest", model.Field{Name: "global", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}, pageSize, pageToken),
				msg("ListGlobalWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGlobalWidgets", Message: `request field "global" is not allowed - want only "page_size" int32 and "page_token" string`},
			},
		},
		{
			name: "subresource: a parent whose name starts with another parent's",
			api: methodAPI(rpc("ListGadgetBoxWidgets", "Widget", "ListGadgetBoxWidgetsResponse"),
				resource("Widget", []string{"Gadget", "GadgetBox"}),
				resource("Gadget", nil),
				resource("GadgetBox", nil),
				msg("ListGadgetBoxWidgetsRequest", model.Field{Name: "gadget_box", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}, pageSize, pageToken),
				msg("ListGadgetBoxWidgetsResponse", widgets, nextPageToken)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.ListMethodShape.Check(tt.api)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check() error = %v, want an error: %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestListResponseNameMatchesMethod(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "compliant",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest"),
				msg("ListWidgetsResponse")),
		},
		{
			name: "wrong name",
			api: methodAPI(rpc("ListWidgets", "Widget", "WidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest"),
				msg("WidgetsResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response message is named "WidgetsResponse", want "ListWidgetsResponse"`},
			},
		},
		{
			name: "a Get method is not checked",
			api: methodAPI(rpc("GetWidget", "Widget", "GetWidgetResponse"),
				resource("Widget", nil),
				msg("GetWidgetRequest"),
				msg("GetWidgetResponse")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.ListResponseNameMatchesMethod.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCreateRequestShape(t *testing.T) {
	widget := model.Field{Name: "widget", TypeKind: "message", TypeFullName: "test.Widget"}
	misnamedWidget := model.Field{Name: "thing", TypeKind: "message", TypeFullName: "test.Widget"}
	options := model.Field{Name: "options", TypeKind: "message", TypeFullName: "ateapi.CreateOptions"}
	gadgetRef := model.Field{Name: "gadget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}

	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "resource field alone",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", widget)),
		},
		{
			name: "resource field plus CreateOptions",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", widget, options)),
		},
		{
			name: "no fields",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest")),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: `request has no "widget" test.Widget field`},
			},
		},
		{
			name: "two resource fields",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", widget, widget)),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: `request has 2 "widget" test.Widget fields, want at most 1`},
			},
		},
		{
			name: "two CreateOptions fields",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", widget, options, options)),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: "request has 2 ateapi.CreateOptions fields, want at most 1"},
			},
		},
		{
			name: "loose control field outside CreateOptions",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", widget, model.Field{Name: "dry_run"})),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: `request field "dry_run" is not allowed - want only "widget" test.Widget and optional ateapi.CreateOptions`},
			},
		},
		{
			name: "resource field, wrong name",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", misnamedWidget)),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: `request field "thing" is test.Widget, want it named "widget"`},
			},
		},
		{
			name: "a Delete method is not checked",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", model.Field{Name: "widget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"})),
		},
		{
			name: "subresource: parent ObjectRef and resource field",
			api: methodAPI(rpc("CreateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("CreateGadgetWidgetRequest", gadgetRef, widget)),
		},
		{
			name: "subresource: parent ObjectRef, resource field, and CreateOptions",
			api: methodAPI(rpc("CreateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("CreateGadgetWidgetRequest", gadgetRef, widget, options)),
		},
		{
			name: "subresource: no parent ObjectRef",
			api: methodAPI(rpc("CreateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("CreateGadgetWidgetRequest", widget)),
			want: []lint.Finding{
				{Subject: "test.Control.CreateGadgetWidget", Message: `request has no "gadget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field not an ObjectRef",
			api: methodAPI(rpc("CreateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("CreateGadgetWidgetRequest", model.Field{Name: "gadget"}, widget)),
			want: []lint.Finding{
				{Subject: "test.Control.CreateGadgetWidget", Message: `request field "gadget" is untyped, want ateapi.ObjectRef`},
			},
		},
		{
			name: "subresource: no resource field",
			api: methodAPI(rpc("CreateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("CreateGadgetWidgetRequest", gadgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.CreateGadgetWidget", Message: `request has no "widget" test.Widget field`},
			},
		},
		{
			name: "subresource of Global: resource field alone",
			api: methodAPI(rpc("CreateGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"}),
				msg("CreateGlobalWidgetRequest", widget)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.CreateRequestShape.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUpdateRequestShape(t *testing.T) {
	widget := model.Field{Name: "widget", TypeKind: "message", TypeFullName: "test.Widget"}
	misnamedWidget := model.Field{Name: "thing", TypeKind: "message", TypeFullName: "test.Widget"}
	gadgetRef := model.Field{Name: "gadget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}

	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "resource field alone",
			api: methodAPI(rpc("UpdateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("UpdateWidgetRequest", widget)),
		},
		{
			name: "no fields",
			api: methodAPI(rpc("UpdateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("UpdateWidgetRequest")),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateWidget", Message: `request has no "widget" test.Widget field`},
			},
		},
		{
			name: "two resource fields",
			api: methodAPI(rpc("UpdateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("UpdateWidgetRequest", widget, widget)),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateWidget", Message: `request has 2 "widget" test.Widget fields, want at most 1`},
			},
		},
		{
			name: "loose control field outside the resource",
			api: methodAPI(rpc("UpdateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("UpdateWidgetRequest", widget, model.Field{Name: "dry_run"})),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateWidget", Message: `request field "dry_run" is not allowed - want only "widget" test.Widget`},
			},
		},
		{
			name: "resource field, wrong name",
			api: methodAPI(rpc("UpdateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("UpdateWidgetRequest", misnamedWidget)),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateWidget", Message: `request field "thing" is test.Widget, want it named "widget"`},
			},
		},
		{
			name: "a Delete method is not checked",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", model.Field{Name: "widget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"})),
		},
		{
			name: "subresource: parent ObjectRef and resource field",
			api: methodAPI(rpc("UpdateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("UpdateGadgetWidgetRequest", gadgetRef, widget)),
		},
		{
			name: "subresource: no parent ObjectRef",
			api: methodAPI(rpc("UpdateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("UpdateGadgetWidgetRequest", widget)),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateGadgetWidget", Message: `request has no "gadget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field not an ObjectRef",
			api: methodAPI(rpc("UpdateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("UpdateGadgetWidgetRequest", model.Field{Name: "gadget"}, widget)),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateGadgetWidget", Message: `request field "gadget" is untyped, want ateapi.ObjectRef`},
			},
		},
		{
			name: "subresource: no resource field",
			api: methodAPI(rpc("UpdateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("UpdateGadgetWidgetRequest", gadgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateGadgetWidget", Message: `request has no "widget" test.Widget field`},
			},
		},
		{
			name: "subresource of Global: resource field alone",
			api: methodAPI(rpc("UpdateGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"}),
				msg("UpdateGlobalWidgetRequest", widget)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.UpdateRequestShape.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDeleteRequestShape(t *testing.T) {
	widgetRef := model.Field{Name: "widget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	misnamedWidgetRef := model.Field{Name: "thing", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	options := model.Field{Name: "options", TypeKind: "message", TypeFullName: "ateapi.DeleteOptions"}
	gadgetRef := model.Field{Name: "gadget", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"}
	name := model.Field{Name: "name", TypeDisplay: "string"}

	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "ObjectRef alone",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef)),
		},
		{
			name: "ObjectRef plus DeleteOptions",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, options)),
		},
		{
			name: "no fields",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest")),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request has no "widget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "two ObjectRef fields",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, widgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request has 2 "widget" ateapi.ObjectRef fields, want at most 1`},
			},
		},
		{
			name: "two DeleteOptions fields",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, options, options)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: "request has 2 ateapi.DeleteOptions fields, want at most 1"},
			},
		},
		{
			name: "loose control field outside DeleteOptions",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, model.Field{Name: "dry_run"})),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request field "dry_run" is not allowed - want only "widget" ateapi.ObjectRef and optional ateapi.DeleteOptions`},
			},
		},
		{
			name: "ObjectRef field, wrong name",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", misnamedWidgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request field "thing" is ateapi.ObjectRef, want it named "widget"`},
			},
		},
		{
			name: "a Create method is not checked",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", model.Field{Name: "widget", TypeKind: "message", TypeFullName: "test.Widget"})),
		},
		{
			name: "subresource: parent ObjectRef and name",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", gadgetRef, name)),
		},
		{
			name: "subresource: parent ObjectRef, name, and DeleteOptions",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", gadgetRef, name, options)),
		},
		{
			name: "subresource: no name",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", gadgetRef, options)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request has no "name" string field`},
			},
		},
		{
			name: "subresource: no parent ObjectRef",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", name, options)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request has no "gadget" ateapi.ObjectRef field`},
			},
		},
		{
			name: "subresource: the resource's own ObjectRef instead",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", widgetRef, options)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request field "widget" is ateapi.ObjectRef, want it named "gadget"`},
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request has no "name" string field`},
			},
		},
		{
			name: "subresource: two DeleteOptions fields",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", gadgetRef, name, options, options)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteGadgetWidget", Message: "request has 2 ateapi.DeleteOptions fields, want at most 1"},
			},
		},
		{
			name: "subresource: loose control field outside DeleteOptions",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", gadgetRef, name, model.Field{Name: "dry_run"})),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request field "dry_run" is not allowed - want only "gadget" ateapi.ObjectRef, "name" string, and optional ateapi.DeleteOptions`},
			},
		},
		{
			name: "subresource of Global: name and DeleteOptions",
			api: methodAPI(rpc("DeleteGlobalWidget", "Widget", "Widget"),
				resource("Widget", []string{"Global"}),
				msg("DeleteGlobalWidgetRequest", name, options)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.DeleteRequestShape.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDeleteOptionsShape(t *testing.T) {
	version := model.Field{Name: "version", TypeDisplay: "int64"}
	uid := model.Field{Name: "uid", TypeDisplay: "string"}
	deleteOptions := func(fields ...model.Field) model.Message {
		return model.Message{FullName: "ateapi.DeleteOptions", Name: "DeleteOptions", Fields: fields}
	}

	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "version and uid",
			api:  messagesAPI(deleteOptions(version, uid)),
		},
		{
			name: "version alone - presence not required",
			api:  messagesAPI(deleteOptions(version)),
		},
		{
			name: "uid alone - presence not required",
			api:  messagesAPI(deleteOptions(uid)),
		},
		{
			name: "no fields at all",
			api:  messagesAPI(deleteOptions()),
		},
		{
			name: "version wrong type",
			api:  messagesAPI(deleteOptions(model.Field{Name: "version", TypeDisplay: "string"}, uid)),
			want: []lint.Finding{
				{Subject: "ateapi.DeleteOptions", Message: `DeleteOptions field "version" is string, want int64`},
			},
		},
		{
			name: "uid wrong type",
			api:  messagesAPI(deleteOptions(version, model.Field{Name: "uid", TypeDisplay: "int64"})),
			want: []lint.Finding{
				{Subject: "ateapi.DeleteOptions", Message: `DeleteOptions field "uid" is int64, want string`},
			},
		},
		{
			name: "extra field beyond version/uid",
			api:  messagesAPI(deleteOptions(version, uid, model.Field{Name: "dry_run", TypeDisplay: "bool"})),
			want: []lint.Finding{
				{Subject: "ateapi.DeleteOptions", Message: `DeleteOptions field "dry_run" is not allowed - want only optional "version" int64 and optional "uid" string`},
			},
		},
		{
			name: "DeleteOptions not declared in this API",
			api:  &model.API{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.DeleteOptionsShape.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSingletonNoList(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "singleton with a List method",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				singletonResource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest"),
				msg("ListGadgetWidgetsResponse")),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: "lists test.Widget, which is a singleton - a singleton has no List method"},
			},
		},
		{
			name: "singleton with a Get method",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				singletonResource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest")),
		},
		{
			name: "non-singleton with a List method",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest"),
				msg("ListGadgetWidgetsResponse")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.SingletonNoList.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
