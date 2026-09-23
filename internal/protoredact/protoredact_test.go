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

package protoredact_test

import (
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/protoredact"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestForLogRecursesIntoRealMapFields walks real messages whose maps hold
// messages (atelet sandbox assets) and strings (ateapi selectors): map values
// are visited without panicking and non-sensitive content is left intact.
func TestForLogRecursesIntoRealMapFields(t *testing.T) {
	run := &ateletpb.RunRequest{
		SandboxAssets: &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			Assets: map[string]*ateletpb.ArchAssets{
				"amd64": {Files: map[string]*ateletpb.AssetFile{
					"runsc": {Url: "https://assets.example/runsc", Sha256: "abc123"},
				}},
			},
		},
		Spec: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{
			Name: "main",
			Env:  []*ateletpb.EnvEntry{{Name: "API_KEY", Value: "sk-secret"}},
		}}},
	}
	got := protoredact.ForLog(run).(*ateletpb.RunRequest)
	if v := got.GetSpec().GetContainers()[0].GetEnv()[0].GetValue(); v != protoredact.Placeholder {
		t.Fatalf("env value = %q, want placeholder", v)
	}
	if u := got.GetSandboxAssets().GetAssets()["amd64"].GetFiles()["runsc"].GetUrl(); u != "https://assets.example/runsc" {
		t.Fatalf("map-of-message content was altered: %q", u)
	}
	if run.GetSpec().GetContainers()[0].GetEnv()[0].GetValue() != "sk-secret" {
		t.Fatal("original mutated")
	}

	tpl := &ateapipb.ActorTemplate{
		WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"workload": "agent"}},
		Containers: []*ateapipb.Container{{
			Name: "c", Env: []*ateapipb.EnvVar{{Name: "TOKEN", Value: "t0p"}},
		}},
	}
	gotTpl := protoredact.ForLog(tpl).(*ateapipb.ActorTemplate)
	if gotTpl.GetWorkerSelector().GetMatchLabels()["workload"] != "agent" {
		t.Fatal("string map was altered")
	}
	if gotTpl.GetContainers()[0].GetEnv()[0].GetValue() != protoredact.Placeholder {
		t.Fatal("EnvVar.value not masked")
	}
}

// redactTestSchema builds, at test time, a schema that exercises every branch
// of Redact, including shapes our production protos do not
// have yet: a labeled field inside a map value, a labeled map, a labeled
// repeated string, a labeled scalar and a labeled bytes field.
//
//	message Inner { string secret = 1 [debug_redact]; string name = 2; }
//	message Outer {
//	  map<string, Inner> by_name = 1;
//	  map<string, string> labels = 2 [debug_redact];
//	  repeated string tokens = 3 [debug_redact];
//	  int64 count = 4 [debug_redact];
//	  bytes raw = 5 [debug_redact];
//	  Inner one = 6;
//	  repeated Inner many = 7;
//	  string plain = 8;
//	  Inner secret_one = 9 [debug_redact];
//	  repeated Inner secret_many = 10 [debug_redact];
//	  oneof choice { string secret_choice = 11 [debug_redact]; string other_choice = 12; }
//	}
func redactTestSchema(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	redact := &descriptorpb.FieldOptions{DebugRedact: proto.Bool(true)}
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	msg := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	rep := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	field := func(name string, num int32, typ *descriptorpb.FieldDescriptorProto_Type, label *descriptorpb.FieldDescriptorProto_Label, typeName string, o *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
		f := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(num), Type: typ, Label: label, Options: o}
		if typeName != "" {
			f.TypeName = proto.String(typeName)
		}
		return f
	}
	oneofField := func(name string, num int32, typ *descriptorpb.FieldDescriptorProto_Type, o *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
		f := field(name, num, typ, opt, "", o)
		f.OneofIndex = proto.Int32(0)
		return f
	}
	mapEntry := func(name, valueType string, valueKind *descriptorpb.FieldDescriptorProto_Type) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{
			Name:    proto.String(name),
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
			Field: []*descriptorpb.FieldDescriptorProto{
				field("key", 1, str, opt, "", nil),
				field("value", 2, valueKind, opt, valueType, nil),
			},
		}
	}
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("redacttest.proto"),
		Package: proto.String("redacttest"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Inner"), Field: []*descriptorpb.FieldDescriptorProto{
				field("secret", 1, str, opt, "", redact),
				field("name", 2, str, opt, "", nil),
			}},
			{Name: proto.String("Outer"),
				NestedType: []*descriptorpb.DescriptorProto{
					mapEntry("ByNameEntry", ".redacttest.Inner", msg),
					mapEntry("LabelsEntry", "", str),
				},
				Field: []*descriptorpb.FieldDescriptorProto{
					field("by_name", 1, msg, rep, ".redacttest.Outer.ByNameEntry", nil),
					field("labels", 2, msg, rep, ".redacttest.Outer.LabelsEntry", redact),
					field("tokens", 3, str, rep, "", redact),
					field("count", 4, descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), opt, "", redact),
					field("raw", 5, descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), opt, "", redact),
					field("one", 6, msg, opt, ".redacttest.Inner", nil),
					field("many", 7, msg, rep, ".redacttest.Inner", nil),
					field("plain", 8, str, opt, "", nil),
					field("secret_one", 9, msg, opt, ".redacttest.Inner", redact),
					field("secret_many", 10, msg, rep, ".redacttest.Inner", redact),
					oneofField("secret_choice", 11, str, redact),
					oneofField("other_choice", 12, str, nil),
				},
				OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("choice")}},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("building test schema: %v", err)
	}
	return fd.Messages().ByName("Outer")
}

func TestRedactCoversEveryFieldShape(t *testing.T) {
	outerDesc := redactTestSchema(t)
	innerDesc := outerDesc.ParentFile().Messages().ByName("Inner")
	newInner := func(secret, name string) protoreflect.Message {
		m := dynamicpb.NewMessage(innerDesc)
		m.Set(innerDesc.Fields().ByName("secret"), protoreflect.ValueOfString(secret))
		m.Set(innerDesc.Fields().ByName("name"), protoreflect.ValueOfString(name))
		return m
	}
	f := func(name string) protoreflect.FieldDescriptor {
		return outerDesc.Fields().ByName(protoreflect.Name(name))
	}

	outer := dynamicpb.NewMessage(outerDesc)
	outer.Mutable(f("by_name")).Map().Set(protoreflect.ValueOfString("a").MapKey(), protoreflect.ValueOfMessage(newInner("s1", "n1")))
	outer.Mutable(f("labels")).Map().Set(protoreflect.ValueOfString("k").MapKey(), protoreflect.ValueOfString("v"))
	outer.Mutable(f("tokens")).List().Append(protoreflect.ValueOfString("tok"))
	outer.Set(f("count"), protoreflect.ValueOfInt64(42))
	outer.Set(f("raw"), protoreflect.ValueOfBytes([]byte("bytes")))
	outer.Set(f("one"), protoreflect.ValueOfMessage(newInner("s2", "n2")))
	outer.Mutable(f("many")).List().Append(protoreflect.ValueOfMessage(newInner("s3", "n3")))
	outer.Set(f("plain"), protoreflect.ValueOfString("keep"))
	outer.Set(f("secret_one"), protoreflect.ValueOfMessage(newInner("s4", "n4")))
	outer.Mutable(f("secret_many")).List().Append(protoreflect.ValueOfMessage(newInner("s5", "n5")))
	outer.Set(f("secret_choice"), protoreflect.ValueOfString("chosen-secret"))

	protoredact.Redact(outer)

	secretOf := func(m protoreflect.Message) string { return m.Get(innerDesc.Fields().ByName("secret")).String() }
	nameOf := func(m protoreflect.Message) string { return m.Get(innerDesc.Fields().ByName("name")).String() }

	// labeled field inside a map value: masked, sibling kept, map entry kept
	inMap := outer.Get(f("by_name")).Map().Get(protoreflect.ValueOfString("a").MapKey()).Message()
	if secretOf(inMap) != protoredact.Placeholder || nameOf(inMap) != "n1" {
		t.Errorf("map value: secret=%q name=%q", secretOf(inMap), nameOf(inMap))
	}
	// labeled map, repeated string, scalar and bytes: cleared
	for _, name := range []string{"labels", "tokens", "count", "raw"} {
		if outer.Has(f(name)) {
			t.Errorf("%s should be cleared, got %v", name, outer.Get(f(name)))
		}
	}
	// nested singular and repeated messages: masked, siblings kept
	if one := outer.Get(f("one")).Message(); secretOf(one) != protoredact.Placeholder || nameOf(one) != "n2" {
		t.Errorf("one: secret=%q name=%q", secretOf(one), nameOf(one))
	}
	if many := outer.Get(f("many")).List().Get(0).Message(); secretOf(many) != protoredact.Placeholder || nameOf(many) != "n3" {
		t.Errorf("many[0]: secret=%q name=%q", secretOf(many), nameOf(many))
	}
	// unlabeled field untouched
	if got := outer.Get(f("plain")).String(); got != "keep" {
		t.Errorf("plain = %q", got)
	}
	// a labeled message or repeated message is dropped whole
	for _, name := range []string{"secret_one", "secret_many"} {
		if outer.Has(f(name)) {
			t.Errorf("%s should be cleared", name)
		}
	}
	// a labeled oneof member is masked and stays the selected member
	if got := outer.Get(f("secret_choice")).String(); got != protoredact.Placeholder {
		t.Errorf("secret_choice = %q", got)
	}
	if which := outer.WhichOneof(outerDesc.Oneofs().ByName("choice")); which == nil || which.Name() != "secret_choice" {
		t.Errorf("oneof selection changed: %v", which)
	}
}

func TestRedactLeavesUnsetFieldsUnset(t *testing.T) {
	outerDesc := redactTestSchema(t)
	innerDesc := outerDesc.ParentFile().Messages().ByName("Inner")
	// Inner with no secret set, nested under an unlabeled field.
	inner := dynamicpb.NewMessage(innerDesc)
	inner.Set(innerDesc.Fields().ByName("name"), protoreflect.ValueOfString("only-name"))
	outer := dynamicpb.NewMessage(outerDesc)
	outer.Set(outerDesc.Fields().ByName("one"), protoreflect.ValueOfMessage(inner))
	// A labeled oneof left unselected.

	protoredact.Redact(outer)

	got := outer.Get(outerDesc.Fields().ByName("one")).Message()
	if got.Has(innerDesc.Fields().ByName("secret")) {
		t.Errorf("unset secret gained a value: %q", got.Get(innerDesc.Fields().ByName("secret")).String())
	}
	if got.Get(innerDesc.Fields().ByName("name")).String() != "only-name" {
		t.Error("sibling altered")
	}
	if outer.WhichOneof(outerDesc.Oneofs().ByName("choice")) != nil {
		t.Error("an unselected oneof became selected")
	}
	if outer.Has(outerDesc.Fields().ByName("count")) || outer.Has(outerDesc.Fields().ByName("raw")) {
		t.Error("unset labeled scalars should stay unset")
	}
}

func TestForLogPassesThroughNonProtoValues(t *testing.T) {
	for _, v := range []any{nil, "a string", 42, errors.New("boom"), struct{ X int }{1}} {
		if got := protoredact.ForLog(v); got != v {
			t.Errorf("protoredact.ForLog(%#v) = %#v, want the value unchanged", v, got)
		}
	}
	// A typed nil proto pointer, as a handler may return alongside an error,
	// must not panic and must come back as a nil message.
	var typedNil *ateapipb.MintActorJWTResponse
	got := protoredact.ForLog(typedNil)
	if m, ok := got.(*ateapipb.MintActorJWTResponse); !ok || m != nil {
		t.Errorf("typed nil: got %#v", got)
	}
}
