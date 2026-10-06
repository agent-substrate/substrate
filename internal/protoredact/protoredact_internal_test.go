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

package protoredact

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestReachesDebugRedactIsMemoizedAndHandlesRecursiveSchemas(t *testing.T) {
	redactOpt := &descriptorpb.FieldOptions{DebugRedact: proto.Bool(true)}
	fdp := &descriptorpb.FileDescriptorProto{
		Name: proto.String("recursive.proto"), Package: proto.String("recursivetest"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			// Node { secret [debug_redact]; children: repeated Node }
			{Name: proto.String("Node"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("secret"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Options: redactOpt},
				{Name: proto.String("children"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), TypeName: proto.String(".recursivetest.Node")},
			}},
			// Tree { children: repeated Tree }, nothing labeled.
			{Name: proto.String("Tree"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("children"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), TypeName: proto.String(".recursivetest.Tree")},
			}},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatal(err)
	}
	node := fd.Messages().ByName("Node")
	tree := fd.Messages().ByName("Tree")

	if _, cached := reachable.Load(node); cached {
		t.Fatal("descriptor cached before first use")
	}
	if !reachesDebugRedact(node) {
		t.Error("Node should reach a labeled field")
	}
	if reachesDebugRedact(tree) {
		t.Error("Tree should not reach a labeled field, and must not loop")
	}
	for _, md := range []protoreflect.MessageDescriptor{node, tree} {
		if _, cached := reachable.Load(md); !cached {
			t.Errorf("%s: answer not memoized", md.FullName())
		}
	}

	// A secret several levels down a recursive type is found and masked;
	// the same shape with the secret unset is left alone.
	newNode := func(secret string, children ...*dynamicpb.Message) *dynamicpb.Message {
		m := dynamicpb.NewMessage(node)
		if secret != "" {
			m.Set(node.Fields().ByName("secret"), protoreflect.ValueOfString(secret))
		}
		for _, c := range children {
			m.Mutable(node.Fields().ByName("children")).List().Append(protoreflect.ValueOfMessage(c))
		}
		return m
	}
	deep := newNode("", newNode("", newNode("deep")))
	if !hasPopulatedDebugRedact(deep) {
		t.Error("populated secret three levels down not found")
	}
	redacted, _ := Redacted(deep)
	got := redacted.ProtoReflect().
		Get(node.Fields().ByName("children")).List().Get(0).Message().
		Get(node.Fields().ByName("children")).List().Get(0).Message().
		Get(node.Fields().ByName("secret")).String()
	if got != Placeholder {
		t.Errorf("deep secret = %q", got)
	}
	unset := newNode("", newNode("", newNode("")))
	if r, changed := Redacted(unset); hasPopulatedDebugRedact(unset) || changed || r != proto.Message(unset) {
		t.Error("recursive message with no secret set should be returned unchanged")
	}
}

func TestExtensionFieldsAreFoundAndMasked(t *testing.T) {
	// Extension fields are invisible to Fields(), so a type that admits them
	// counts as reaching a labeled field, and the populated walk and the
	// masking walk both go through Range, which does visit extensions.
	redactOpt := &descriptorpb.FieldOptions{DebugRedact: proto.Bool(true)}
	fdp := &descriptorpb.FileDescriptorProto{
		Name: proto.String("ext.proto"), Package: proto.String("exttest"), Syntax: proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Open"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("name"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			},
			ExtensionRange: []*descriptorpb.DescriptorProto_ExtensionRange{{Start: proto.Int32(100), End: proto.Int32(200)}},
		}},
		Extension: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("token"), Number: proto.Int32(100), Extendee: proto.String(".exttest.Open"), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Options: redactOpt},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatal(err)
	}
	open := fd.Messages().ByName("Open")
	token := dynamicpb.NewExtensionType(fd.Extensions().ByName("token")).TypeDescriptor()
	if !reachesDebugRedact(open) {
		t.Fatal("a type with extension ranges should be treated as reaching a labeled field")
	}

	plain := dynamicpb.NewMessage(open)
	plain.Set(open.Fields().ByName("name"), protoreflect.ValueOfString("n"))
	if r, changed := Redacted(plain); NeedsRedaction(plain) || changed || r != proto.Message(plain) {
		t.Error("no extension set: should be returned unchanged")
	}

	withToken := dynamicpb.NewMessage(open)
	withToken.Set(token, protoreflect.ValueOfString("secret"))
	if !NeedsRedaction(withToken) {
		t.Fatal("populated labeled extension not detected")
	}
	redacted, changed := Redacted(withToken)
	got := redacted.ProtoReflect()
	if !changed || got == protoreflect.Message(withToken) {
		t.Fatal("expected a copy")
	}
	if v := got.Get(token).String(); v != Placeholder {
		t.Errorf("extension = %q, want placeholder", v)
	}
	if withToken.Get(token).String() != "secret" {
		t.Error("original mutated")
	}
}
