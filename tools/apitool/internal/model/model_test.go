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

package model_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	_ "github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func message(t *testing.T, api *model.API, fullName string) model.Message {
	t.Helper()
	for _, m := range api.Messages {
		if m.FullName == fullName {
			return m
		}
	}
	t.Fatalf("message %q not found", fullName)
	return model.Message{}
}

func field(t *testing.T, msg model.Message, name string) model.Field {
	t.Helper()
	for _, f := range msg.Fields {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("field %q not found on %s", name, msg.FullName)
	return model.Field{}
}

func buildRealAPI(t *testing.T) *model.API {
	t.Helper()
	fd, err := protoregistry.GlobalFiles.FindFileByPath("ateapi.proto")
	if err != nil {
		t.Fatalf("FindFileByPath(ateapi.proto) error = %v", err)
	}
	return model.Build(fd)
}

func TestBuild_MapField(t *testing.T) {
	api := buildRealAPI(t)

	f := field(t, message(t, api, "ateapi.Selector"), "match_labels")
	if want := "map<string, string>"; f.TypeDisplay != want {
		t.Errorf("TypeDisplay = %q, want %q", f.TypeDisplay, want)
	}
	if f.TypeKind != "" || f.TypeFullName != "" {
		t.Errorf("map field TypeKind/TypeFullName = %q/%q, want both empty", f.TypeKind, f.TypeFullName)
	}

	for _, m := range api.Messages {
		if strings.Contains(m.Name, "Entry") {
			t.Errorf("synthetic map-entry type leaked into API.Messages: %s", m.FullName)
		}
	}
}

func TestBuild_RepeatedMessageField(t *testing.T) {
	api := buildRealAPI(t)

	f := field(t, message(t, api, "ateapi.Actor"), "actor_volumes")
	if want := "repeated ExternalVolume"; f.TypeDisplay != want {
		t.Errorf("TypeDisplay = %q, want %q", f.TypeDisplay, want)
	}
	if f.TypeKind != "message" {
		t.Errorf("TypeKind = %q, want %q", f.TypeKind, "message")
	}
	if want := "ateapi.ExternalVolume"; f.TypeFullName != want {
		t.Errorf("TypeFullName = %q, want %q", f.TypeFullName, want)
	}
}

func TestBuild_MapValueMessageField(t *testing.T) {
	api := buildRealAPI(t)

	f := field(t, message(t, api, "ateapi.SandboxAssets"), "assets")
	if want := "map<string, ArchAssets>"; f.TypeDisplay != want {
		t.Errorf("TypeDisplay = %q, want %q", f.TypeDisplay, want)
	}
	if f.MapValueKind != "message" {
		t.Errorf("MapValueKind = %q, want %q", f.MapValueKind, "message")
	}
	if want := "ateapi.ArchAssets"; f.MapValueFullName != want {
		t.Errorf("MapValueFullName = %q, want %q", f.MapValueFullName, want)
	}
}

func TestBuild_Oneof(t *testing.T) {
	api := buildRealAPI(t)
	msg := message(t, api, "ateapi.ActorSnapshotRef")

	for _, name := range []string{"snapshot", "tag"} {
		if got := field(t, msg, name).OneofName; got != "reference" {
			t.Errorf("%s.OneofName = %q, want %q", name, got, "reference")
		}
	}
}

func TestBuild_NestedEnums(t *testing.T) {
	api := buildRealAPI(t)

	tests := []struct{ fullName, parent string }{
		{"ateapi.Actor.Status", "ateapi.Actor"},
		{"ateapi.ExternalVolume.Status", "ateapi.ExternalVolume"},
		{"ateapi.Worker.State", "ateapi.Worker"},
	}
	for _, tt := range tests {
		t.Run(tt.fullName, func(t *testing.T) {
			var found *model.Enum
			for i, e := range api.Enums {
				if e.FullName == tt.fullName {
					found = &api.Enums[i]
				}
			}
			if found == nil {
				t.Fatalf("enum %q not found", tt.fullName)
			}
			if found.ParentFullName != tt.parent {
				t.Errorf("ParentFullName = %q, want %q", found.ParentFullName, tt.parent)
			}
			if len(found.Values) == 0 {
				t.Error("expected at least one enum value")
			}
		})
	}
}

// Building directly from protoregistry.GlobalFiles (no protoc invocation)
// never has source info, so every comment comes back empty - this is the
// known limitation of the fast, protoc-free test path used throughout this
// file; internal/parser's tests cover the real, comment-populated path.
func TestBuild_NoSourceInfoMeansNoComments(t *testing.T) {
	api := buildRealAPI(t)
	msg := message(t, api, "ateapi.ResourceMetadata")
	if msg.Comment != "" {
		t.Errorf("Message.Comment = %q, want empty (no source info on this path)", msg.Comment)
	}
	if got := field(t, msg, "atespace").Comment; got != "" {
		t.Errorf("Field.Comment = %q, want empty (no source info on this path)", got)
	}
}

// TestBuild_Proto3Optional exercises a field shape ateapi.proto doesn't
// currently have: an explicit proto3 `optional` scalar field, which
// protoreflect represents internally as a synthetic one-member oneof. A
// hand-built descriptor (still no protoc) is the only way to cover it.
func TestBuild_Proto3Optional(t *testing.T) {
	proto3 := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	fdProto := &descriptorpb.FileDescriptorProto{
		Name:    stringPtr("fixture.proto"),
		Package: stringPtr("fixture"),
		Syntax:  stringPtr("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: stringPtr("Widget"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:           stringPtr("nickname"),
						Number:         int32Ptr(1),
						Label:          &proto3,
						Type:           &stringType,
						OneofIndex:     int32Ptr(0),
						Proto3Optional: boolPtr(true),
					},
				},
				OneofDecl: []*descriptorpb.OneofDescriptorProto{
					{Name: stringPtr("_nickname")},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdProto, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile() error = %v", err)
	}

	api := model.Build(fd)
	f := field(t, message(t, api, "fixture.Widget"), "nickname")
	if !f.Proto3Optional {
		t.Error("Proto3Optional = false, want true")
	}
	if f.OneofName != "" {
		t.Errorf("OneofName = %q, want empty (synthetic oneof must not be shown as a real oneof group)", f.OneofName)
	}
}

func hasMessage(api *model.API, fullName string) bool {
	for _, m := range api.Messages {
		if m.FullName == fullName {
			return true
		}
	}
	return false
}

func hasEnum(api *model.API, fullName string) bool {
	for _, e := range api.Enums {
		if e.FullName == fullName {
			return true
		}
	}
	return false
}

func TestScopeToService(t *testing.T) {
	api := model.ScopeToService(buildRealAPI(t), "Control")

	if len(api.Services) != 1 || api.Services[0].Name != "Control" {
		t.Fatalf("Services = %+v, want exactly [Control]", api.Services)
	}

	for _, fullName := range []string{
		"ateapi.Actor", // directly used by Control's RPCs
		"ateapi.ActorTemplateVersion",
		// Reachable only through map values: ActorTemplateVersion ->
		// SandboxConfig -> SandboxAssets -> (map) -> ArchAssets -> (map) ->
		// AssetFile. Proves ScopeToService follows MapValueFullName, not
		// just TypeFullName.
		"ateapi.SandboxAssets",
		"ateapi.ArchAssets",
		"ateapi.AssetFile",
	} {
		if !hasMessage(api, fullName) {
			t.Errorf("message %q missing from Control's filtered API", fullName)
		}
	}
	if !hasEnum(api, "ateapi.Actor.Status") {
		t.Error("nested enum ateapi.Actor.Status missing from Control's filtered API")
	}

	for _, fullName := range []string{
		"ateapi.DebugClearRequest",  // Debug-only
		"ateapi.DebugClearResponse", // Debug-only
		"ateapi.MintJWTRequest",     // ActorIdentity-only
		"ateapi.MintJWTResponse",    // ActorIdentity-only
	} {
		if hasMessage(api, fullName) {
			t.Errorf("message %q from a non-Control service leaked into Control's filtered API", fullName)
		}
	}
	if hasEnum(api, "ateapi.ActorCertificatePurpose") {
		t.Error("ActorIdentity-only enum ateapi.ActorCertificatePurpose leaked into Control's filtered API")
	}
}

func TestScopeToService_UnknownService(t *testing.T) {
	api := model.ScopeToService(buildRealAPI(t), "NoSuchService")
	if len(api.Services) != 0 || len(api.Messages) != 0 || len(api.Enums) != 0 {
		t.Errorf("ScopeToService(unknown) = %+v, want a completely empty API", api)
	}
}

func TestResources(t *testing.T) {
	api := model.ScopeToService(buildRealAPI(t), "Control")

	groups, err := model.Resources(api)
	if err != nil {
		t.Fatalf("Resources() error = %v", err)
	}

	var actorGroup *model.Resource
	for i := range groups {
		if groups[i].Message.Name == "Actor" {
			actorGroup = &groups[i]
		}
	}
	if actorGroup == nil {
		t.Fatalf("no Resource for Actor; groups = %+v", groups)
	}

	var gotNames []string
	for _, m := range actorGroup.Methods {
		gotNames = append(gotNames, m.Name)
	}
	wantNames := []string{"GetActor", "CreateActor", "UpdateActor", "SuspendActor", "PauseActor", "ResumeActor", "DeleteActor", "ListActors"}
	if len(gotNames) != len(wantNames) {
		t.Fatalf("Actor group methods = %v, want %v", gotNames, wantNames)
	}
	for i, want := range wantNames {
		if gotNames[i] != want {
			t.Errorf("Actor group methods[%d] = %q, want %q (declaration order matters)", i, gotNames[i], want)
		}
	}
}

func TestResources_InvalidMethodName(t *testing.T) {
	tests := []struct {
		name       string
		methodName string
	}{
		{"no resource name matches", "DoThing"},
		{"two equally-specific resource names match", "ActorSnapshotAndActorTemplate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &model.API{
				Services: []model.Service{{
					Name: "Bogus",
					Methods: []model.Method{
						{Name: tt.methodName, ServiceName: "Bogus"},
					},
				}},
			}
			if _, err := model.Resources(api); err == nil {
				t.Errorf("Resources() error = nil for method %q, want an error", tt.methodName)
			}
		})
	}
}

func stringPtr(s string) *string { return &s }
func int32Ptr(i int32) *int32    { return &i }
func boolPtr(b bool) *bool       { return &b }
