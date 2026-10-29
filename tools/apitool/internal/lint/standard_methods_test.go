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
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/tools/apitool/internal/lint"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

func TestStandardMethodReturnsResource(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
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
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: "Get method returns google.protobuf.Empty, want the resource itself (test.Widget)"},
			},
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
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetRequestShape(t *testing.T) {
	widgetRef := model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}
	misnamedWidgetRef := model.Field{Name: "thing", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}
	gadgetRef := model.Field{Name: "gadget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}
	name := model.Field{Name: "name", Kind: protoreflect.StringKind, TypeDisplay: "string"}

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
				{Subject: "test.Control.GetWidget", Message: `request has no "widget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "stray field",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", widgetRef, model.Field{Name: "extra"})),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: `request field "extra" is not allowed - want only "widget" substrate.ateapi.v1.ObjectRef`},
			},
		},
		{
			name: "single field, wrong type",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", name)),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: `request field "name" is not allowed - want only "widget" substrate.ateapi.v1.ObjectRef`},
				{Subject: "test.Control.GetWidget", Message: `request has no "widget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "single field, wrong name",
			api: methodAPI(rpc("GetWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("GetWidgetRequest", misnamedWidgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.GetWidget", Message: `request field "thing" is substrate.ateapi.v1.ObjectRef, want it named "widget"`},
			},
		},
		{
			name: "a Create method is not checked",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"})),
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
				msg("GetGadgetWidgetRequest", gadgetRef, model.Field{Name: "name", Kind: protoreflect.Int64Kind, TypeDisplay: "int64"})),
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
				{Subject: "test.Control.GetGadgetWidget", Message: `request has no "gadget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field not an ObjectRef",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", model.Field{Name: "gadget", Kind: protoreflect.StringKind, TypeDisplay: "string"}, name)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request field "gadget" is string, want substrate.ateapi.v1.ObjectRef`},
			},
		},
		{
			name: "subresource: the resource's own ObjectRef instead",
			api: methodAPI(rpc("GetGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("GetGadgetWidgetRequest", widgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.GetGadgetWidget", Message: `request field "widget" is substrate.ateapi.v1.ObjectRef, want it named "gadget"`},
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
				{Subject: "test.Control.GetGadgetWidget", Message: `request field "extra" is not allowed - want only "gadget" substrate.ateapi.v1.ObjectRef and "name" string`},
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
				msg("GetGlobalWidgetRequest", model.Field{Name: "global", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}, name)),
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
	options := model.Field{Name: "options", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ListOptions"}
	atespace := model.Field{Name: "atespace", Kind: protoreflect.StringKind, TypeDisplay: "string"}
	gadgetRef := model.Field{Name: "gadget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}
	widgets := model.Field{Name: "widgets", Repeated: true, Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"}
	nextPageToken := model.Field{Name: "next_page_token", Kind: protoreflect.StringKind, TypeDisplay: "string"}

	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "compliant",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "missing options",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request has no "options" substrate.ateapi.v1.ListOptions field`},
			},
		},
		{
			name: "loose page_size and page_token outside ListOptions",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace,
					model.Field{Name: "page_size", Kind: protoreflect.Int32Kind, TypeDisplay: "int32"},
					model.Field{Name: "page_token", Kind: protoreflect.StringKind, TypeDisplay: "string"}),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request field "page_size" is not allowed - want only "atespace" string and "options" substrate.ateapi.v1.ListOptions`},
				{Subject: "test.Control.ListWidgets", Message: `request field "page_token" is not allowed - want only "atespace" string and "options" substrate.ateapi.v1.ListOptions`},
				{Subject: "test.Control.ListWidgets", Message: `request has no "options" substrate.ateapi.v1.ListOptions field`},
			},
		},
		{
			name: "missing repeated field and next_page_token",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options),
				msg("ListWidgetsResponse", model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"})),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "widget" is not allowed - want only "widgets" repeated test.Widget and "next_page_token" string`},
				{Subject: "test.Control.ListWidgets", Message: `response has no "widgets" repeated test.Widget field`},
				{Subject: "test.Control.ListWidgets", Message: `response has no "next_page_token" string field`},
			},
		},
		{
			name: "repeated scalar field doesn't satisfy the rule",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options),
				msg("ListWidgetsResponse", model.Field{Name: "ids", Repeated: true}, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "ids" is not allowed - want only "widgets" repeated test.Widget and "next_page_token" string`},
				{Subject: "test.Control.ListWidgets", Message: `response has no "widgets" repeated test.Widget field`},
			},
		},
		{
			name: "repeated field misnamed - doesn't match the resource's plural",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options),
				msg("ListWidgetsResponse", model.Field{Name: "items", Repeated: true, Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"}, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "items" is repeated test.Widget, want it named "widgets"`},
			},
		},
		{
			name: "repeated field of another message",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options),
				msg("ListWidgetsResponse", model.Field{Name: "widgets", Repeated: true, Kind: protoreflect.MessageKind, TypeFullName: "test.Gadget"}, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "widgets" is repeated test.Gadget, want repeated test.Widget`},
			},
		},
		{
			name: "atespace-scoped resource without atespace",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", options),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request has no "atespace" string field`},
			},
		},
		{
			name: "global resource",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				globalResource("Widget"),
				msg("ListWidgetsRequest", options),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "global resource with atespace",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				globalResource("Widget"),
				msg("ListWidgetsRequest", atespace, options),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request field "atespace" is not allowed - want only "options" substrate.ateapi.v1.ListOptions`},
			},
		},
		{
			name: "stray field on the request - sorting/filtering is not supported",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options, model.Field{Name: "filter"}),
				msg("ListWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request field "filter" is not allowed - want only "atespace" string and "options" substrate.ateapi.v1.ListOptions`},
			},
		},
		{
			name: "stray field on the response",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options),
				msg("ListWidgetsResponse", widgets, nextPageToken, model.Field{Name: "total_count"})),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response field "total_count" is not allowed - want only "widgets" repeated test.Widget and "next_page_token" string`},
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
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `request has no "atespace" string field`},
				{Subject: "test.Control.ListWidgets", Message: `request has no "options" substrate.ateapi.v1.ListOptions field`},
			},
		},
		{
			name: "response not declared in this API",
			api: methodAPI(rpc("ListWidgets", "Widget", "ListWidgetsResponse"),
				resource("Widget", nil),
				msg("ListWidgetsRequest", atespace, options)),
			want: []lint.Finding{
				{Subject: "test.Control.ListWidgets", Message: `response has no "widgets" repeated test.Widget field`},
				{Subject: "test.Control.ListWidgets", Message: `response has no "next_page_token" string field`},
			},
		},
		{
			name: "subresource: compliant",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", gadgetRef, options),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "subresource: missing parent field",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", options),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `request has no "gadget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field is not an ObjectRef",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", model.Field{Name: "gadget", Kind: protoreflect.StringKind, TypeDisplay: "string"}, options),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `request field "gadget" is string, want substrate.ateapi.v1.ObjectRef`},
			},
		},
		{
			name: "subresource: atespace field is not allowed",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", gadgetRef, atespace, options),
				msg("ListGadgetWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `request field "atespace" is not allowed - want only "gadget" substrate.ateapi.v1.ObjectRef and "options" substrate.ateapi.v1.ListOptions`},
			},
		},
		{
			name: "subresource: repeated field named after the parent too",
			api: methodAPI(rpc("ListGadgetWidgets", "Widget", "ListGadgetWidgetsResponse"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("ListGadgetWidgetsRequest", gadgetRef, options),
				msg("ListGadgetWidgetsResponse", model.Field{Name: "gadget_widgets", Repeated: true, Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"}, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGadgetWidgets", Message: `response field "gadget_widgets" is repeated test.Widget, want it named "widgets"`},
			},
		},
		{
			name: "subresource of Global: compliant",
			api: methodAPI(rpc("ListGlobalWidgets", "Widget", "ListGlobalWidgetsResponse"),
				resource("Widget", []string{"Global"}),
				msg("ListGlobalWidgetsRequest", options),
				msg("ListGlobalWidgetsResponse", widgets, nextPageToken)),
		},
		{
			name: "subresource of Global: parent field is not allowed",
			api: methodAPI(rpc("ListGlobalWidgets", "Widget", "ListGlobalWidgetsResponse"),
				resource("Widget", []string{"Global"}),
				msg("ListGlobalWidgetsRequest", model.Field{Name: "global", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}, options),
				msg("ListGlobalWidgetsResponse", widgets, nextPageToken)),
			want: []lint.Finding{
				{Subject: "test.Control.ListGlobalWidgets", Message: `request field "global" is not allowed - want only "options" substrate.ateapi.v1.ListOptions`},
			},
		},
		{
			name: "subresource: a parent whose name starts with another parent's",
			api: methodAPI(rpc("ListGadgetBoxWidgets", "Widget", "ListGadgetBoxWidgetsResponse"),
				resource("Widget", []string{"Gadget", "GadgetBox"}),
				resource("Gadget", nil),
				resource("GadgetBox", nil),
				msg("ListGadgetBoxWidgetsRequest", model.Field{Name: "gadget_box", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}, options),
				msg("ListGadgetBoxWidgetsResponse", widgets, nextPageToken)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.ListMethodShape.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
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
	widget := model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"}
	misnamedWidget := model.Field{Name: "thing", Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"}
	options := model.Field{Name: "options", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.CreateOptions"}
	gadgetRef := model.Field{Name: "gadget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}

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
				{Subject: "test.Control.CreateWidget", Message: `request has 2 "options" substrate.ateapi.v1.CreateOptions fields, want at most 1`},
			},
		},
		{
			name: "CreateOptions field misnamed",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", widget, model.Field{Name: "opts", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.CreateOptions"})),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: `request field "opts" is substrate.ateapi.v1.CreateOptions, want it named "options"`},
			},
		},
		{
			name: "loose control field outside CreateOptions",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", widget, model.Field{Name: "dry_run"})),
			want: []lint.Finding{
				{Subject: "test.Control.CreateWidget", Message: `request field "dry_run" is not allowed - want only "widget" test.Widget and optional "options" substrate.ateapi.v1.CreateOptions`},
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
				msg("DeleteWidgetRequest", model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"})),
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
				{Subject: "test.Control.CreateGadgetWidget", Message: `request has no "gadget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field not an ObjectRef",
			api: methodAPI(rpc("CreateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("CreateGadgetWidgetRequest", model.Field{Name: "gadget", Kind: protoreflect.StringKind, TypeDisplay: "string"}, widget)),
			want: []lint.Finding{
				{Subject: "test.Control.CreateGadgetWidget", Message: `request field "gadget" is string, want substrate.ateapi.v1.ObjectRef`},
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
	widget := model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"}
	misnamedWidget := model.Field{Name: "thing", Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"}
	gadgetRef := model.Field{Name: "gadget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}

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
				msg("DeleteWidgetRequest", model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"})),
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
				{Subject: "test.Control.UpdateGadgetWidget", Message: `request has no "gadget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "subresource: parent field not an ObjectRef",
			api: methodAPI(rpc("UpdateGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("UpdateGadgetWidgetRequest", model.Field{Name: "gadget", Kind: protoreflect.StringKind, TypeDisplay: "string"}, widget)),
			want: []lint.Finding{
				{Subject: "test.Control.UpdateGadgetWidget", Message: `request field "gadget" is string, want substrate.ateapi.v1.ObjectRef`},
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
	widgetRef := model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}
	misnamedWidgetRef := model.Field{Name: "thing", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}
	options := model.Field{Name: "options", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.DeleteOptions"}
	gadgetRef := model.Field{Name: "gadget", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"}
	name := model.Field{Name: "name", Kind: protoreflect.StringKind, TypeDisplay: "string"}

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
				{Subject: "test.Control.DeleteWidget", Message: `request has no "widget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "two ObjectRef fields",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, widgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request has 2 "widget" substrate.ateapi.v1.ObjectRef fields, want at most 1`},
			},
		},
		{
			name: "two DeleteOptions fields",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, options, options)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request has 2 "options" substrate.ateapi.v1.DeleteOptions fields, want at most 1`},
			},
		},
		{
			name: "DeleteOptions field misnamed",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, model.Field{Name: "opts", Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.DeleteOptions"})),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request field "opts" is substrate.ateapi.v1.DeleteOptions, want it named "options"`},
			},
		},
		{
			name: "loose control field outside DeleteOptions",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", widgetRef, model.Field{Name: "dry_run"})),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request field "dry_run" is not allowed - want only "widget" substrate.ateapi.v1.ObjectRef and optional "options" substrate.ateapi.v1.DeleteOptions`},
			},
		},
		{
			name: "ObjectRef field, wrong name",
			api: methodAPI(rpc("DeleteWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("DeleteWidgetRequest", misnamedWidgetRef)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteWidget", Message: `request field "thing" is substrate.ateapi.v1.ObjectRef, want it named "widget"`},
			},
		},
		{
			name: "a Create method is not checked",
			api: methodAPI(rpc("CreateWidget", "Widget", "Widget"),
				resource("Widget", nil),
				msg("CreateWidgetRequest", model.Field{Name: "widget", Kind: protoreflect.MessageKind, TypeFullName: "test.Widget"})),
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
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request has no "gadget" substrate.ateapi.v1.ObjectRef field`},
			},
		},
		{
			name: "subresource: the resource's own ObjectRef instead",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", widgetRef, options)),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request field "widget" is substrate.ateapi.v1.ObjectRef, want it named "gadget"`},
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
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request has 2 "options" substrate.ateapi.v1.DeleteOptions fields, want at most 1`},
			},
		},
		{
			name: "subresource: loose control field outside DeleteOptions",
			api: methodAPI(rpc("DeleteGadgetWidget", "Widget", "Widget"),
				resource("Widget", []string{"Gadget"}),
				resource("Gadget", nil),
				msg("DeleteGadgetWidgetRequest", gadgetRef, name, model.Field{Name: "dry_run"})),
			want: []lint.Finding{
				{Subject: "test.Control.DeleteGadgetWidget", Message: `request field "dry_run" is not allowed - want only "gadget" substrate.ateapi.v1.ObjectRef, "name" string, and optional "options" substrate.ateapi.v1.DeleteOptions`},
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
	version := model.Field{Name: "version", Kind: protoreflect.Int64Kind, TypeDisplay: "int64"}
	uid := model.Field{Name: "uid", Kind: protoreflect.StringKind, TypeDisplay: "string"}
	deleteOptions := func(fields ...model.Field) model.Message {
		return model.Message{FullName: "substrate.ateapi.v1.DeleteOptions", Name: "DeleteOptions", Fields: fields}
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
			api:  messagesAPI(deleteOptions(model.Field{Name: "version", Kind: protoreflect.StringKind, TypeDisplay: "string"}, uid)),
			want: []lint.Finding{
				{Subject: "substrate.ateapi.v1.DeleteOptions", Message: `DeleteOptions field "version" is string, want int64`},
			},
		},
		{
			name: "uid wrong type",
			api:  messagesAPI(deleteOptions(version, model.Field{Name: "uid", Kind: protoreflect.Int64Kind, TypeDisplay: "int64"})),
			want: []lint.Finding{
				{Subject: "substrate.ateapi.v1.DeleteOptions", Message: `DeleteOptions field "uid" is int64, want string`},
			},
		},
		{
			name: "extra field beyond version/uid",
			api:  messagesAPI(deleteOptions(version, uid, model.Field{Name: "dry_run", Kind: protoreflect.BoolKind, TypeDisplay: "bool"})),
			want: []lint.Finding{
				{Subject: "substrate.ateapi.v1.DeleteOptions", Message: `DeleteOptions field "dry_run" is not allowed - want only optional "version" int64 and optional "uid" string`},
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
