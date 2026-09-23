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

package logredact

import (
	"bytes"
	"context"
	"encoding/base64"
	"log/slog"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	envSecret = "sk-secret"
	jwtSecret = "jwt-secret-token"
)

func TestSanitizeRedactsLabeledFields(t *testing.T) {
	tests := []struct {
		name  string
		value proto.Message
		check func(t *testing.T, got proto.Message)
	}{
		{
			name:  "atelet env entry",
			value: &ateletpb.EnvEntry{Name: "API_KEY", Value: envSecret},
			check: func(t *testing.T, got proto.Message) {
				if v := got.(*ateletpb.EnvEntry); v.GetValue() != "" || v.GetName() != "API_KEY" {
					t.Errorf("env entry = %+v, want the name kept and the value cleared", v)
				}
			},
		},
		{
			name:  "ateapi env var",
			value: &ateapipb.EnvVar{Name: "API_KEY", Value: envSecret},
			check: func(t *testing.T, got proto.Message) {
				if v := got.(*ateapipb.EnvVar); v.GetValue() != "" || v.GetName() != "API_KEY" {
					t.Errorf("env var = %+v, want the name kept and the value cleared", v)
				}
			},
		},
		{
			name:  "actor JWT",
			value: &ateapipb.MintActorJWTResponse{ActorJwt: jwtSecret},
			check: func(t *testing.T, got proto.Message) {
				if v := got.(*ateapipb.MintActorJWTResponse); v.GetActorJwt() != "" {
					t.Errorf("actor jwt = %q, want it cleared", v.GetActorJwt())
				}
			},
		},
		{
			name:  "credential provider bytes",
			value: &credproviderpb.FetchSecretResponse{OpaqueBytes: []byte("hunter2")},
			check: func(t *testing.T, got proto.Message) {
				if v := got.(*credproviderpb.FetchSecretResponse); len(v.GetOpaqueBytes()) != 0 {
					t.Errorf("opaque bytes = %q, want them cleared", v.GetOpaqueBytes())
				}
			},
		},
		{
			name: "nested in a repeated field",
			value: &ateletpb.RunRequest{Spec: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{
				{Name: "main", Env: []*ateletpb.EnvEntry{{Name: "API_KEY", Value: envSecret}}},
			}}},
			check: func(t *testing.T, got proto.Message) {
				req := got.(*ateletpb.RunRequest)
				container := req.GetSpec().GetContainers()[0]
				if got := container.GetEnv()[0].GetValue(); got != "" {
					t.Errorf("env value = %q, want it cleared", got)
				}
				if container.GetName() != "main" {
					t.Errorf("container name = %q, want main", container.GetName())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := proto.Clone(tt.value)
			got, ok := Sanitize(tt.value).(proto.Message)
			if !ok {
				t.Fatalf("Sanitize returned %T, want a proto.Message", Sanitize(tt.value))
			}
			tt.check(t, got)
			if !proto.Equal(tt.value, before) {
				t.Errorf("Sanitize mutated the caller's message: got %v, want %v", tt.value, before)
			}
		})
	}
}

// TestSanitizeReturnsUnchangedValues covers the fast path: a value that cannot
// carry a redacted field must come back untouched, not copied.
func TestSanitizeReturnsUnchangedValues(t *testing.T) {
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "n"}}
	if got := Sanitize(actor); got != any(actor) {
		t.Errorf("Sanitize(actor) = %v, want the same pointer back", got)
	}

	labels := map[string]string{"k": "v"}
	if got := Sanitize(labels); got.(map[string]string)["k"] != "v" {
		t.Errorf("Sanitize(map) = %v, want it unchanged", got)
	}

	plain := []string{"a", "b"}
	if got := Sanitize(plain); len(got.([]string)) != 2 {
		t.Errorf("Sanitize([]string) = %v, want it unchanged", got)
	}
}

// TestSanitizeNilValues covers the shapes an RPC interceptor actually sees: a
// nil response, a typed nil, and a container holding either.
func TestSanitizeNilValues(t *testing.T) {
	if got := Sanitize(nil); got != nil {
		t.Errorf("Sanitize(nil) = %v, want nil", got)
	}

	var nilResp *ateapipb.MintActorJWTResponse
	if got := Sanitize(nilResp); got != any(nilResp) {
		t.Errorf("Sanitize(typed nil) = %v, want the same value back", got)
	}

	got := Sanitize([]*ateapipb.MintActorJWTResponse{nil, {ActorJwt: jwtSecret}}).([]*ateapipb.MintActorJWTResponse)
	if got[0] != nil {
		t.Errorf("element 0 = %v, want nil", got[0])
	}
	if got[1].GetActorJwt() != "" {
		t.Errorf("element 1 jwt = %q, want it cleared", got[1].GetActorJwt())
	}
}

func TestSanitizeRedactsContainers(t *testing.T) {
	if got := Sanitize([]*ateapipb.MintActorJWTResponse{
		{ActorJwt: jwtSecret},
		{ActorJwt: jwtSecret + "-2"},
	}).([]*ateapipb.MintActorJWTResponse); got[0].GetActorJwt() != "" || got[1].GetActorJwt() != "" {
		t.Errorf("slice elements = %+v, want both cleared", got)
	}

	if got := Sanitize([1]*ateapipb.MintActorJWTResponse{{ActorJwt: jwtSecret}}).([1]*ateapipb.MintActorJWTResponse); got[0].GetActorJwt() != "" {
		t.Errorf("array element = %+v, want it cleared", got[0])
	}

	if got := Sanitize(map[string]*credproviderpb.FetchSecretResponse{
		"k": {OpaqueBytes: []byte("hunter2")},
	}).(map[string]*credproviderpb.FetchSecretResponse); len(got["k"].GetOpaqueBytes()) != 0 {
		t.Errorf("map value = %+v, want it cleared", got["k"])
	}

	// A container is copied, so clearing an element must not touch the original.
	original := []*ateapipb.MintActorJWTResponse{{ActorJwt: jwtSecret}}
	Sanitize(original)
	if original[0].GetActorJwt() != jwtSecret {
		t.Errorf("Sanitize mutated the caller's slice element: %q", original[0].GetActorJwt())
	}
}

func TestHandlerRedactsEveryAttrShape(t *testing.T) {
	var log bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&log, nil)))

	logger.Info("Handle RPC",
		slog.Any("resp", &ateapipb.MintActorJWTResponse{ActorJwt: jwtSecret}),
		slog.Group("spec", slog.Any("env", []*ateletpb.EnvEntry{{Name: "API_KEY", Value: envSecret}})),
		slog.Any("secrets", map[string]*credproviderpb.FetchSecretResponse{
			"k": {OpaqueBytes: []byte("hunter2")},
		}),
	)

	got := log.String()
	for _, want := range []string{jwtSecret, envSecret, "hunter2", base64.StdEncoding.EncodeToString([]byte("hunter2"))} {
		if strings.Contains(got, want) {
			t.Errorf("log contains redacted data %q: %s", want, got)
		}
	}
	for _, want := range []string{"API_KEY", `"spec"`, `"secrets"`} {
		if !strings.Contains(got, want) {
			t.Errorf("log is missing non-sensitive data %q: %s", want, got)
		}
	}
}

// TestHandlerForwardsUnchangedRecords pins the no-op path: a record with no
// redactable value reaches the next handler with its values intact, including
// the original pointer of a message with nothing to redact.
func TestHandlerForwardsUnchangedRecords(t *testing.T) {
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "n"}}
	c := &capture{}
	logger := slog.New(NewHandler(&captureHandler{c: c}))

	logger.Info("m", slog.Any("actor", actor), slog.String("k", "v"), slog.Int("n", 3))

	attrs := recordAttrs(t, c)
	if len(attrs) != 3 {
		t.Fatalf("got %d attributes, want 3", len(attrs))
	}
	if attrs[0].Value.Any() != any(actor) {
		t.Errorf("actor = %v, want the same pointer back", attrs[0].Value.Any())
	}
	if attrs[1].Value.Kind() != slog.KindString || attrs[1].Value.String() != "v" {
		t.Errorf("string attr = %v, want KindString %q", attrs[1].Value, "v")
	}
	if attrs[2].Value.Kind() != slog.KindInt64 || attrs[2].Value.Int64() != 3 {
		t.Errorf("int attr = %v, want KindInt64 3", attrs[2].Value)
	}
}

type jwtValuer struct{ jwt string }

func (v jwtValuer) LogValue() slog.Value {
	return slog.AnyValue(&ateapipb.MintActorJWTResponse{ActorJwt: v.jwt})
}

func TestHandlerResolvesLogValuer(t *testing.T) {
	var log bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&log, nil)))

	logger.Info("m", slog.Any("resp", jwtValuer{jwt: jwtSecret}))

	if got := log.String(); strings.Contains(got, jwtSecret) {
		t.Errorf("log contains redacted data: %s", got)
	}
}

func TestHandlerSanitizesWithAttrs(t *testing.T) {
	c := &capture{}
	logger := slog.New(NewHandler(&captureHandler{c: c})).
		With(slog.Any("resp", &ateapipb.MintActorJWTResponse{ActorJwt: jwtSecret}), slog.String("k", "v"))

	logger.Info("m")

	if len(c.attrs) != 2 {
		t.Fatalf("got %d attributes, want 2", len(c.attrs))
	}
	resp, ok := c.attrs[0].Value.Any().(*ateapipb.MintActorJWTResponse)
	if !ok {
		t.Fatalf("attribute %v is not a MintActorJWTResponse", c.attrs[0].Value)
	}
	if resp.GetActorJwt() != "" {
		t.Errorf("actor jwt = %q, want it cleared in a With attribute", resp.GetActorJwt())
	}
	if c.attrs[1].Value.String() != "v" {
		t.Errorf("string attribute = %v, want %q", c.attrs[1].Value, "v")
	}
}

// TestSanitizeDynamicMessages exercises the walk over a schema that carries a
// map of messages and a cycle, which the repository's own protos do not.
func TestSanitizeDynamicMessages(t *testing.T) {
	holderDesc, secretDesc := newTestFile(t)
	tokenField := secretDesc.Fields().ByName("token")
	secretsField := holderDesc.Fields().ByName("secrets")
	byNameField := holderDesc.Fields().ByName("by_name")
	childField := holderDesc.Fields().ByName("child")

	holder := dynamicpb.NewMessage(holderDesc)
	holder.Mutable(secretsField).List().Append(protoreflect.ValueOfMessage(newSecret(secretDesc, "list-secret")))
	holder.Mutable(byNameField).Map().Set(
		protoreflect.ValueOfString("k").MapKey(),
		protoreflect.ValueOfMessage(newSecret(secretDesc, "map-secret")),
	)
	child := dynamicpb.NewMessage(holderDesc)
	child.Mutable(secretsField).List().Append(protoreflect.ValueOfMessage(newSecret(secretDesc, "child-secret")))
	holder.Set(childField, protoreflect.ValueOfMessage(child))

	got, ok := Sanitize(holder).(*dynamicpb.Message)
	if !ok {
		t.Fatalf("Sanitize returned %T, want a dynamic message", Sanitize(holder))
	}

	if v := got.Get(secretsField).List().Get(0).Message().Get(tokenField).String(); v != "" {
		t.Errorf("list token = %q, want it cleared", v)
	}
	mapValue := got.Get(byNameField).Map().Get(protoreflect.ValueOfString("k").MapKey())
	if v := mapValue.Message().Get(tokenField).String(); v != "" {
		t.Errorf("map token = %q, want it cleared", v)
	}
	childValue := got.Get(childField).Message()
	if v := childValue.Get(secretsField).List().Get(0).Message().Get(tokenField).String(); v != "" {
		t.Errorf("nested child token = %q, want it cleared", v)
	}

	// The caller's message keeps every secret.
	if v := holder.Get(secretsField).List().Get(0).Message().Get(tokenField).String(); v != "list-secret" {
		t.Errorf("original list token = %q, want it untouched", v)
	}
}

func TestMayContainRedactedField(t *testing.T) {
	holderDesc, _ := newTestFile(t)

	if !mayContainRedactedField(holderDesc) {
		t.Errorf("Holder may contain a redacted field")
	}
	if md := holderDesc.Fields().ByName("child").Message(); !mayContainRedactedField(md) {
		t.Errorf("Holder.child reaches a redacted field through the cycle")
	}
	if mayContainRedactedField((&ateapipb.Actor{}).ProtoReflect().Descriptor()) {
		t.Errorf("Actor has no redacted field")
	}
}

func TestSensitiveProtoFieldsAreMarkedDebugRedact(t *testing.T) {
	tests := []struct {
		name  string
		msg   proto.Message
		field protoreflect.Name
		want  bool
	}{
		{name: "ateapi env value", msg: &ateapipb.EnvVar{}, field: "value", want: true},
		{name: "ateapi env name", msg: &ateapipb.EnvVar{}, field: "name", want: false},
		{name: "ateapi actor JWT", msg: &ateapipb.MintActorJWTResponse{}, field: "actor_jwt", want: true},
		{name: "atelet env value", msg: &ateletpb.EnvEntry{}, field: "value", want: true},
		{name: "atelet env name", msg: &ateletpb.EnvEntry{}, field: "name", want: false},
		{name: "credential provider opaque bytes", msg: &credproviderpb.FetchSecretResponse{}, field: "opaque_bytes", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fd := tt.msg.ProtoReflect().Descriptor().Fields().ByName(tt.field)
			if fd == nil {
				t.Fatalf("field %q not found on %T", tt.field, tt.msg)
			}
			opts, _ := fd.Options().(*descriptorpb.FieldOptions)
			if got := opts.GetDebugRedact(); got != tt.want {
				t.Errorf("debug_redact on %T.%s = %v, want %v", tt.msg, tt.field, got, tt.want)
			}
		})
	}
}

type capture struct {
	attrs   []slog.Attr
	records []slog.Record
}

type captureHandler struct {
	c *capture
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, rec slog.Record) error {
	h.c.records = append(h.c.records, rec)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.c.attrs = append(h.c.attrs, attrs...)
	return h
}

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func recordAttrs(t *testing.T, c *capture) []slog.Attr {
	t.Helper()
	if len(c.records) != 1 {
		t.Fatalf("got %d records, want 1", len(c.records))
	}
	var attrs []slog.Attr
	c.records[0].Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	return attrs
}

// newTestFile builds a small self-contained file descriptor: a Secret message
// with a redacted token, a Holder with a repeated Secret, a map of Secrets, and
// a Secret-holding child that points back at Holder.
func newTestFile(t *testing.T) (holder, secret protoreflect.MessageDescriptor) {
	t.Helper()

	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("logredact/internal_test.proto"),
		Package: proto.String("logredacttest"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Secret"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name: proto.String("token"), Number: proto.Int32(1), Label: &optional, Type: &stringType,
						Options: &descriptorpb.FieldOptions{DebugRedact: proto.Bool(true)},
					},
				},
			},
			{
				Name: proto.String("Holder"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name: proto.String("secrets"), Number: proto.Int32(1), Label: &repeated, Type: &messageType,
						TypeName: proto.String(".logredacttest.Secret"),
					},
					{
						Name: proto.String("by_name"), Number: proto.Int32(2), Label: &repeated, Type: &messageType,
						TypeName: proto.String(".logredacttest.Holder.ByNameEntry"),
					},
					{
						Name: proto.String("child"), Number: proto.Int32(3), Label: &optional, Type: &messageType,
						TypeName: proto.String(".logredacttest.Holder"),
					},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("ByNameEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("key"), Number: proto.Int32(1), Label: &optional, Type: &stringType},
							{
								Name: proto.String("value"), Number: proto.Int32(2), Label: &optional, Type: &messageType,
								TypeName: proto.String(".logredacttest.Secret"),
							},
						},
						Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					},
				},
			},
		},
	}

	file, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("building test file descriptor: %v", err)
	}
	return file.Messages().ByName("Holder"), file.Messages().ByName("Secret")
}

func newSecret(md protoreflect.MessageDescriptor, token string) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("token"), protoreflect.ValueOfString(token))
	return msg
}
