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
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
	"github.com/agent-substrate/substrate/internal/protoredact"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestRedactedRecursesIntoRealMapFields walks real messages whose maps hold
// messages (atelet sandbox assets) and strings (ateapi selectors): map values
// are visited without panicking, labeled fields in them are masked, and
// non-sensitive content is left intact.
func TestRedactedRecursesIntoRealMapFields(t *testing.T) {
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
	redacted, _ := protoredact.Redacted(run)
	got := redacted.(*ateletpb.RunRequest)
	if v := got.GetSpec().GetContainers()[0].GetEnv()[0].GetValue(); v != protoredact.Placeholder {
		t.Fatalf("env value = %q, want placeholder", v)
	}
	// Inside the map value, the labeled url is masked and the sha256 is kept.
	file := got.GetSandboxAssets().GetAssets()["amd64"].GetFiles()["runsc"]
	if u := file.GetUrl(); u != protoredact.Placeholder {
		t.Fatalf("asset url in a map value = %q, want placeholder", u)
	}
	if sum := file.GetSha256(); sum != "abc123" {
		t.Fatalf("map-of-message content was altered: sha256 = %q", sum)
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
	redactedTpl, _ := protoredact.Redacted(tpl)
	gotTpl := redactedTpl.(*ateapipb.ActorTemplate)
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

func TestRedactedCoversEveryFieldShape(t *testing.T) {
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

	redacted, _ := protoredact.Redacted(outer)
	outer = redacted.(*dynamicpb.Message)

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

func TestRedactedLeavesUnsetFieldsUnset(t *testing.T) {
	outerDesc := redactTestSchema(t)
	innerDesc := outerDesc.ParentFile().Messages().ByName("Inner")
	// Inner with no secret set, nested under an unlabeled field.
	inner := dynamicpb.NewMessage(innerDesc)
	inner.Set(innerDesc.Fields().ByName("name"), protoreflect.ValueOfString("only-name"))
	outer := dynamicpb.NewMessage(outerDesc)
	outer.Set(outerDesc.Fields().ByName("one"), protoreflect.ValueOfMessage(inner))
	// A labeled oneof left unselected.

	redacted, _ := protoredact.Redacted(outer)
	outer = redacted.(*dynamicpb.Message)

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

func TestRedactedHandlesTypedNilWithoutPanicking(t *testing.T) {
	// A typed nil proto pointer, as a handler returns alongside an error and
	// the interceptor then logs, must come back as a nil message of the same
	// type, for a redactable type and for a clean one alike.
	var jwt *ateapipb.MintActorJWTResponse
	if r, changed := protoredact.Redacted(jwt); changed {
		t.Errorf("typed nil redactable: reported changed, got %#v", r)
	} else if m, ok := r.(*ateapipb.MintActorJWTResponse); !ok || m != nil {
		t.Errorf("typed nil redactable: got %#v", r)
	}
	var ref *ateapipb.ObjectRef
	if r, changed := protoredact.Redacted(ref); changed {
		t.Errorf("typed nil clean: reported changed, got %#v", r)
	} else if m, ok := r.(*ateapipb.ObjectRef); !ok || m != nil {
		t.Errorf("typed nil clean: got %#v", r)
	}
}

func TestRedactedReturnsMessagesWithNothingToMaskWithoutCopying(t *testing.T) {
	// Types with no path to a debug_redact field, and messages of a labeled
	// type whose labeled fields are all unset, come back as the same pointer:
	// nothing to mask, nothing to copy.
	for _, m := range []proto.Message{
		&ateapipb.ListActorsResponse{Actors: []*ateapipb.Actor{{Metadata: &ateapipb.ResourceMetadata{Name: "a"}}}},
		&ateapipb.ObjectRef{Atespace: "s", Name: "n"},
		&ateapipb.Worker{},
		&ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Name: "c", Image: "img"}}},
		&ateapipb.ListActorTemplatesResponse{ActorTemplates: []*ateapipb.ActorTemplate{{Containers: []*ateapipb.Container{{Name: "c"}}}}},
		&ateapipb.MintActorJWTResponse{},
		&ateletpb.RunRequest{Spec: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Env: []*ateletpb.EnvEntry{{Name: "K"}}}}}},
	} {
		if protoredact.NeedsRedaction(m) {
			t.Errorf("%T: NeedsRedaction should be false", m)
		}
		if got, changed := protoredact.Redacted(m); changed || got != m {
			t.Errorf("%T: expected the same message back and changed=false, got changed=%v", m, changed)
		}
	}
	// A populated labeled field, however deep, forces a masked copy.
	for _, m := range []proto.Message{
		&ateapipb.MintActorJWTResponse{ActorJwt: "t"},
		&ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Env: []*ateapipb.EnvVar{{Name: "K", Value: "v"}}}}},
		&ateapipb.ListActorTemplatesResponse{ActorTemplates: []*ateapipb.ActorTemplate{{}, {Containers: []*ateapipb.Container{{}, {Env: []*ateapipb.EnvVar{{Name: "K", Value: "v"}}}}}}},
		&ateletpb.RunRequest{Spec: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Env: []*ateletpb.EnvEntry{{Name: "K", Value: "v"}}}}}},
	} {
		if !protoredact.NeedsRedaction(m) {
			t.Errorf("%T: NeedsRedaction should be true", m)
		}
		if got, changed := protoredact.Redacted(m); !changed || got == m {
			t.Errorf("%T: expected a copy and changed=true, got changed=%v", m, changed)
		}
	}
	if r, changed := protoredact.Redacted(nil); protoredact.NeedsRedaction(nil) || changed || r != nil {
		t.Error("nil message should be reported clean and returned as nil")
	}
}

func TestNeedsRedactionToleratesTypedNilOfAnyImplementation(t *testing.T) {
	// A typed nil generated message answers ProtoReflect safely; a typed nil
	// dynamicpb.Message returns the nil receiver, which faults on use. Both
	// must be reported clean and returned unchanged.
	var gen *ateapipb.MintActorJWTResponse
	var dyn *dynamicpb.Message
	for _, m := range []proto.Message{gen, dyn} {
		if protoredact.NeedsRedaction(m) {
			t.Errorf("%T typed nil: NeedsRedaction should be false", m)
		}
		if got, changed := protoredact.Redacted(m); changed || got != m {
			t.Errorf("%T typed nil: got %#v (changed=%v), want the same value back", m, got, changed)
		}
	}
}

func TestNeedsRedactionIsSafeUnderConcurrentFirstUse(t *testing.T) {
	// The per-type answer is memoized on first sight; many goroutines
	// meeting several types at once must agree and not race.
	msgs := []proto.Message{
		&ateapipb.MintActorJWTResponse{ActorJwt: "t"},
		&ateapipb.ListActorsResponse{},
		&ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{Env: []*ateapipb.EnvVar{{Name: "K", Value: "v"}}}}},
		&ateapipb.Worker{},
		&ateletpb.RunRequest{Spec: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Env: []*ateletpb.EnvEntry{{Name: "K", Value: "v"}}}}}},
	}
	want := []bool{true, false, true, false, true}
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, m := range msgs {
				if got := protoredact.NeedsRedaction(m); got != want[i] {
					t.Errorf("%T: NeedsRedaction = %v, want %v", m, got, want[i])
				}
			}
		}()
	}
	wg.Wait()
}

// ourProtoFiles is every proto file in this module whose messages the log
// handler can be handed. TestOurProtoFilesAreAllListed keeps it in step with
// the .proto files on disk.
var ourProtoFiles = []protoreflect.FileDescriptor{
	ateapipb.File_ateapi_proto,
	ateletpb.File_atelet_proto,
	ateompb.File_ateom_proto,
	credproviderpb.File_credprovider_proto,
	glutton.File_glutton_proto,
	grpcechopb.File_grpcecho_proto,
	objectstorev1.File_objectstore_proto,
}

// forEachField calls fn for every field of every message in ourProtoFiles,
// nested messages included.
func forEachField(fn func(protoreflect.FieldDescriptor)) {
	var walk func(protoreflect.MessageDescriptors)
	walk = func(mds protoreflect.MessageDescriptors) {
		for i := 0; i < mds.Len(); i++ {
			md := mds.Get(i)
			fds := md.Fields()
			for j := 0; j < fds.Len(); j++ {
				fn(fds.Get(j))
			}
			walk(md.Messages())
		}
	}
	for _, file := range ourProtoFiles {
		walk(file.Messages())
	}
}

func isDebugRedact(fd protoreflect.FieldDescriptor) bool {
	opts, ok := fd.Options().(*descriptorpb.FieldOptions)
	return ok && opts.GetDebugRedact()
}

// TestDebugRedactFieldsArePinned lists every field across our protos that
// carries debug_redact. It fails when a label is added or removed so the
// change is reviewed as a deliberate decision about what the logs may show.
func TestDebugRedactFieldsArePinned(t *testing.T) {
	want := map[string]bool{
		"ateapi.EnvVar.value":                            true,
		"ateapi.MintActorJWTResponse.actor_jwt":          true,
		"atelet.AssetFile.url":                           true,
		"atelet.EnvEntry.value":                          true,
		"ateom.ContainerSpec.env":                        true,
		"credprovider.FetchSecretResponse.opaque_bytes":  true,
		"objectstore.v1.FetchAssetRequest.asset_uri":     true,
		"objectstore.v1.FetchSnapshotRequest.actor_jwt":  true,
		"objectstore.v1.UploadSnapshotRequest.actor_jwt": true,
	}
	got := map[string]bool{}
	forEachField(func(fd protoreflect.FieldDescriptor) {
		if isDebugRedact(fd) {
			got[string(fd.FullName())] = true
		}
	})
	for name := range want {
		if !got[name] {
			t.Errorf("%s lost its debug_redact label; the log handler would write it in clear", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("%s is newly marked debug_redact; add it to this list if that is intended", name)
		}
	}
}

// secretLikeName matches field names that usually hold a credential.
var secretLikeName = regexp.MustCompile(`(?i)jwt|token|secret|passw|credential|private_?key|api_?key|bearer|authorization|cookie`)

// TestSecretLikeFieldsAreLabeled catches a new field that looks like it holds
// a credential but was added without debug_redact, which the log handler
// would then write in clear. A field that only names or configures a
// credential goes in notSecret with the reason.
func TestSecretLikeFieldsAreLabeled(t *testing.T) {
	notSecret := map[string]string{
		"page_token":                             "opaque list cursor",
		"next_page_token":                        "opaque list cursor",
		"ateapi.CredentialHeader.credential_uri": "ate-secret:// reference to a credential, not the value",
		"ateapi.CredentialHeader.actor_jwt":      "ActorJWTSource: audiences and lifetime for minting, not a token",
	}
	forEachField(func(fd protoreflect.FieldDescriptor) {
		if !secretLikeName.MatchString(string(fd.Name())) || isDebugRedact(fd) {
			return
		}
		if _, ok := notSecret[string(fd.FullName())]; ok {
			return
		}
		if _, ok := notSecret[string(fd.Name())]; ok {
			return
		}
		t.Errorf("%s looks like a credential but has no debug_redact; label it, or add it to notSecret with the reason", fd.FullName())
	})
}

// TestOurProtoFilesAreAllListed fails when a .proto file is added to the
// module's Go source without being added to ourProtoFiles, so the two tests
// above cover it. Only cmd, internal and pkg are walked, where every proto
// with generated Go code lives; vendored and third-party protos are out of
// scope, as are tool installs elsewhere in the tree (the locust codegen
// virtualenv ships google/protobuf/*.proto).
func TestOurProtoFilesAreAllListed(t *testing.T) {
	listed := map[string]bool{}
	for _, file := range ourProtoFiles {
		listed[path.Base(file.Path())] = true
	}
	root := filepath.Join("..", "..")
	for _, dir := range []string{"cmd", "internal", "pkg"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				switch d.Name() {
				case "testdata", "third_party", "vendor":
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(p, ".proto") && !listed[filepath.Base(p)] {
				rel, _ := filepath.Rel(root, p)
				t.Errorf("%s is not in ourProtoFiles", rel)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
