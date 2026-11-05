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

func TestResourceMetadata(t *testing.T) {
	tests := []struct {
		name    string
		api     *model.API
		want    []lint.Finding
		wantErr bool
	}{
		{
			name: "compliant",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ResourceMetadata"})),
		},
		{
			name: "missing entirely",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "name", Number: 1})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `has no "metadata" field`},
			},
		},
		{
			name: "wrong type",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ObjectRef"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" field is substrate.ateapi.v1.ObjectRef, want substrate.ateapi.v1.ResourceMetadata`},
			},
		},
		{
			name: "scalar",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.StringKind, TypeDisplay: "string"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" field is string, want substrate.ateapi.v1.ResourceMetadata`},
			},
		},
		{
			name: "repeated",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Repeated: true, Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ResourceMetadata"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" field is repeated substrate.ateapi.v1.ResourceMetadata, want substrate.ateapi.v1.ResourceMetadata`},
			},
		},
		{
			name: "wrong field number",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 2, Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ResourceMetadata"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" field is number 2, want 1`},
			},
		},
		{
			name: "a message that isn't a resource is not checked",
			api:  messagesAPI(msg("Widget")),
		},
		{
			name:    "parent that isn't a resource",
			api:     messagesAPI(resource("Widget", []string{"Gadget"}, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ResourceMetadata"})),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.ResourceMetadata.Check(tt.api)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check() error = %v, want an error: %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResourceStatusFieldShape(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "no status field at all",
			api:  messagesAPI(resource("Widget", nil)),
		},
		{
			name: "correctly typed status field",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "status", Kind: protoreflect.MessageKind, TypeFullName: "test.WidgetStatus"})),
		},
		{
			name: "wrongly typed status field",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "status", Kind: protoreflect.MessageKind, TypeFullName: "test.WrongStatus"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"status" field is test.WrongStatus, want test.WidgetStatus`},
			},
		},
		{
			name: "status field is repeated",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "status", Repeated: true, Kind: protoreflect.MessageKind, TypeFullName: "test.WidgetStatus"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"status" field is repeated test.WidgetStatus, want test.WidgetStatus`},
			},
		},
		{
			name: "status field is a scalar",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "status", Kind: protoreflect.StringKind, TypeDisplay: "string"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"status" field is string, want test.WidgetStatus`},
			},
		},
		{
			name: "a message that isn't a resource is not checked",
			api:  messagesAPI(msg("Widget", model.Field{Name: "status", Kind: protoreflect.StringKind, TypeDisplay: "string"})),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.ResourceStatusFieldShape.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestResourceFieldOrder(t *testing.T) {
	meta := model.Field{Name: "metadata", Number: 1, Kind: protoreflect.MessageKind, TypeFullName: "substrate.ateapi.v1.ResourceMetadata"}
	status := model.Field{Name: "status", Number: 2, Kind: protoreflect.MessageKind, TypeFullName: "test.WidgetStatus"}
	spec := model.Field{Name: "spec", Number: 3}
	scope := model.Field{Name: "scope", Number: 4}

	tests := []struct {
		name    string
		api     *model.API
		want    []lint.Finding
		wantErr bool
	}{
		{
			name: "metadata first, status last",
			api:  messagesAPI(resource("Widget", nil, meta, spec, status)),
		},
		{
			name: "metadata only",
			api:  messagesAPI(resource("Widget", nil, meta)),
		},
		{
			name: "no status is fine",
			api:  messagesAPI(resource("Widget", nil, meta, spec, scope)),
		},
		{
			// The rule is about declaration order, not numbers: these are
			// numbered 1 and 2 exactly as resource-metadata wants, and still
			// read in the wrong order.
			name: "metadata declared after another field",
			api:  messagesAPI(resource("Widget", nil, spec, meta, status)),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" is declared after "spec", want it declared first`},
			},
		},
		{
			name: "status declared before another field",
			api:  messagesAPI(resource("Widget", nil, meta, status, scope)),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"status" is declared before "scope", want it declared last`},
			},
		},
		{
			name: "both out of place are reported separately",
			api:  messagesAPI(resource("Widget", nil, status, spec, meta)),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" is declared after "status", want it declared first`},
				{Subject: "test.Widget", Message: `"status" is declared before "metadata", want it declared last`},
			},
		},
		{
			// resource-metadata reports a missing "metadata"; this rule must not
			// report it a second time.
			name: "missing metadata is not this rule's finding",
			api:  messagesAPI(resource("Widget", nil, status, spec)),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"status" is declared before "spec", want it declared last`},
			},
		},
		{
			name: "no fields at all",
			api:  messagesAPI(resource("Widget", nil)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lint.ResourceFieldOrder.Check(tt.api)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
