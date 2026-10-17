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
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.MessageKind, TypeFullName: "ateapi.ResourceMetadata"})),
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
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.MessageKind, TypeFullName: "ateapi.ObjectRef"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" field is ateapi.ObjectRef, want ateapi.ResourceMetadata`},
			},
		},
		{
			name: "scalar",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.StringKind, TypeDisplay: "string"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" field is string, want ateapi.ResourceMetadata`},
			},
		},
		{
			name: "repeated",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 1, Repeated: true, Kind: protoreflect.MessageKind, TypeFullName: "ateapi.ResourceMetadata"})),
			want: []lint.Finding{
				{Subject: "test.Widget", Message: `"metadata" field is repeated ateapi.ResourceMetadata, want ateapi.ResourceMetadata`},
			},
		},
		{
			name: "wrong field number",
			api:  messagesAPI(resource("Widget", nil, model.Field{Name: "metadata", Number: 2, Kind: protoreflect.MessageKind, TypeFullName: "ateapi.ResourceMetadata"})),
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
			api:     messagesAPI(resource("Widget", []string{"Gadget"}, model.Field{Name: "metadata", Number: 1, Kind: protoreflect.MessageKind, TypeFullName: "ateapi.ResourceMetadata"})),
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
