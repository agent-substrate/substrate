// Copyright 2026 The Agent Substrate Authors
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

package controlapi

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	_ "google.golang.org/protobuf/types/known/emptypb"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
)

type testMethod struct {
	name            string
	resource        string
	gate            string
	clientStreaming bool
	serverStreaming bool
}

func buildTestServices(t *testing.T, methods ...testMethod) protoreflect.ServiceDescriptors {
	t.Helper()
	var protos []*descriptorpb.MethodDescriptorProto
	for _, m := range methods {
		var opts *descriptorpb.MethodOptions
		if m.resource != "" || m.gate != "" {
			opts = &descriptorpb.MethodOptions{}
			proto.SetExtension(opts, ateapi.E_Method, &ateapi.MethodAnnotation{Resource: m.resource, PreviewGate: m.gate})
		}
		protos = append(protos, &descriptorpb.MethodDescriptorProto{
			Name:            proto.String(m.name),
			InputType:       proto.String(".google.protobuf.Empty"),
			OutputType:      proto.String(".google.protobuf.Empty"),
			ClientStreaming: proto.Bool(m.clientStreaming),
			ServerStreaming: proto.Bool(m.serverStreaming),
			Options:         opts,
		})
	}
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("test.proto"),
		Package:    proto.String("test"),
		Dependency: []string{"google/protobuf/empty.proto"},
		Service:    []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Svc"), Method: protos}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return fd.Services()
}

func TestPreviewMethodGates(t *testing.T) {
	preview.InitForTestFake(t, []preview.Gate{"A"})

	tests := []struct {
		name    string
		methods []testMethod
		want    map[string]preview.Gate
		wantErr string
	}{
		{
			name: "gated and ungated methods",
			methods: []testMethod{
				{name: "NoOptions"},
				{name: "Ungated", resource: "Widget"},
				{name: "Gated", resource: "Widget", gate: "A"},
			},
			want: map[string]preview.Gate{"/test.Svc/Gated": "A"},
		},
		{
			name:    "unknown gate",
			methods: []testMethod{{name: "Gated", gate: "B"}},
			wantErr: "unknown preview gate",
		},
		{
			name:    "ALL pseudo-gate",
			methods: []testMethod{{name: "Gated", gate: preview.All}},
			wantErr: "names every gate",
		},
		{
			name:    "client streaming",
			methods: []testMethod{{name: "Stream", gate: "A", clientStreaming: true}},
			wantErr: "streaming methods",
		},
		{
			name:    "server streaming",
			methods: []testMethod{{name: "Stream", gate: "A", serverStreaming: true}},
			wantErr: "streaming methods",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PreviewMethodGates(buildTestServices(t, tc.methods...))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("PreviewMethodGates() error = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("PreviewMethodGates() error = %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("PreviewMethodGates() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
