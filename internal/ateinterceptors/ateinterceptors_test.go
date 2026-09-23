// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package ateinterceptors

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/credproviderpb"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestStatusErrorInterceptor(t *testing.T) {
	tests := []struct {
		name           string
		handlerErr     error
		wantCode       codes.Code
		wantMsg        string
		expectResponse bool
	}{
		{
			name:           "Success",
			handlerErr:     nil,
			expectResponse: true,
		},
		{
			name:       "StatusErrorInChain",
			handlerErr: fmt.Errorf("outer error: %w", status.Error(codes.NotFound, "actor not found")),
			wantCode:   codes.NotFound,
			wantMsg:    "actor not found",
		},
		{
			name:       "RawErrorFallback",
			handlerErr: errors.New("database connection failed"),
			wantCode:   codes.Internal,
			wantMsg:    "internal server error: database connection failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				return "response", tt.handlerErr
			}

			resp, err := ServerUnaryInterceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)

			if tt.expectResponse {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if resp != "response" {
					t.Errorf("expected response 'response', got %v", resp)
				}
				return
			}

			if err == nil {
				t.Fatalf("expected error, got nil")
			}

			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected gRPC status error, got: %v", err)
			}

			if st.Code() != tt.wantCode {
				t.Errorf("expected code %v, got %v", tt.wantCode, st.Code())
			}

			if st.Message() != tt.wantMsg {
				t.Errorf("expected message %q, got %q", tt.wantMsg, st.Message())
			}
		})
	}
}

// errorInfoOf returns the ErrorInfo detail carried by err, or nil if none.
func errorInfoOf(t *testing.T, err error) *epb.ErrorInfo {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) = _, false; want a status error", err)
	}
	for _, d := range st.Details() {
		if info, ok := d.(*epb.ErrorInfo); ok {
			return info
		}
	}
	return nil
}

// TestInternalServerUnaryInterceptorPreservesDetails verifies the interceptor
// returns structured errors (from NewGRPCError) intact — preserving the code and
// the ErrorInfo carrying the Reason — while collapsing plain errors to Internal
// with no ErrorInfo detail.
func TestInternalServerUnaryInterceptorPreservesDetails(t *testing.T) {
	tests := []struct {
		name          string
		handlerErr    error
		wantCode      codes.Code
		wantReason    string
		wantErrorInfo bool
	}{
		{
			name:          "structured error keeps code and reason",
			handlerErr:    ateerrors.NewGRPCError(context.Background(), codes.DataLoss, ateerrors.ReasonFaileSaveSnapshot, ateerrors.ActorCrashedMetadata(), errors.New("boom")),
			wantCode:      codes.DataLoss,
			wantReason:    string(ateerrors.ReasonFaileSaveSnapshot),
			wantErrorInfo: true,
		},
		{
			name:          "plain error collapses to Internal with no ErrorInfo",
			handlerErr:    errors.New("database connection failed"),
			wantCode:      codes.Internal,
			wantErrorInfo: false,
		},
	}

	interceptor := InternalServerUnaryInterceptor
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				return nil, tt.handlerErr
			}

			_, err := interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			st, _ := status.FromError(err)
			if st.Code() != tt.wantCode {
				t.Errorf("code = %v, want %v", st.Code(), tt.wantCode)
			}

			info := errorInfoOf(t, err)
			if !tt.wantErrorInfo {
				if info != nil {
					t.Errorf("ErrorInfo = %v, want none", info)
				}
				return
			}
			if info == nil {
				t.Fatal("status is missing the ErrorInfo detail")
			}
			if got := info.GetReason(); got != tt.wantReason {
				t.Errorf("ErrorInfo.Reason = %q, want %q", got, tt.wantReason)
			}
		})
	}
}

// TestServerUnaryInterceptorPreservesDetails verifies the public interceptor
// returns the handler's status intact: ErrorInfo details (reason and metadata)
// must survive the public wire, even when the status is wrapped.
func TestServerUnaryInterceptorPreservesDetails(t *testing.T) {
	metadata := map[string]string{"want": "0.2.0", "have": "0.1.0"}
	structuredErr := ateerrors.NewGRPCError(context.Background(), codes.FailedPrecondition, ateerrors.ReasonInvalidCheckpointResult, metadata, errors.New("refused"))

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, fmt.Errorf("outer error: %w", structuredErr)
	}

	_, err := ServerUnaryInterceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want %v", st.Code(), codes.FailedPrecondition)
	}

	info := errorInfoOf(t, err)
	if info == nil {
		t.Fatal("status is missing the ErrorInfo detail")
	}
	if got, want := info.GetReason(), string(ateerrors.ReasonInvalidCheckpointResult); got != want {
		t.Errorf("ErrorInfo.Reason = %q, want %q", got, want)
	}
	for k, want := range metadata {
		if got := info.GetMetadata()[k]; got != want {
			t.Errorf("ErrorInfo.Metadata[%q] = %q, want %q", k, got, want)
		}
	}
}

type trailerStream struct {
	method   string
	trailers metadata.MD
}

func (s *trailerStream) Method() string                  { return s.method }
func (s *trailerStream) SetHeader(md metadata.MD) error  { return nil }
func (s *trailerStream) SendHeader(md metadata.MD) error { return nil }
func (s *trailerStream) SetTrailer(md metadata.MD) error {
	if s.trailers == nil {
		s.trailers = metadata.MD{}
	}
	for k, v := range md {
		s.trailers[k] = append(s.trailers[k], v...)
	}
	return nil
}

func TestServerUnaryInterceptorEmitsElapsedTrailer(t *testing.T) {
	const minHandlerDuration = 5 * time.Millisecond
	stream := &trailerStream{method: "/test.Service/Method"}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), stream)

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		time.Sleep(minHandlerDuration)
		return "response", nil
	}

	if _, err := ServerUnaryInterceptor(ctx, "request", &grpc.UnaryServerInfo{FullMethod: stream.method}, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vals := stream.trailers.Get(ServerElapsedTrailer)
	if len(vals) != 1 {
		t.Fatalf("expected one %s trailer, got %v", ServerElapsedTrailer, vals)
	}
	elapsedUs, err := strconv.ParseInt(vals[0], 10, 64)
	if err != nil {
		t.Fatalf("could not parse %s as int64: %v", vals[0], err)
	}
	if got, min := time.Duration(elapsedUs)*time.Microsecond, minHandlerDuration; got < min {
		t.Errorf("trailer reported %s; expected at least %s (handler sleep)", got, min)
	}
}

func TestMaxDeadlineUnaryInterceptor_MaxDeadlineIsEnforced(t *testing.T) {
	const ceiling = 50 * time.Millisecond

	tests := []struct {
		name      string
		callerCtx func() (context.Context, context.CancelFunc)
	}{
		{
			name: "no caller deadline",
			callerCtx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
		{
			name: "caller deadline longer than ceiling",
			callerCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Hour)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			interceptor := MaxDeadlineUnaryInterceptor(ceiling)

			callerCtx, cancel := tt.callerCtx()
			defer cancel()

			handler := func(ctx context.Context, req interface{}) (interface{}, error) {
				gotDeadline, ok := ctx.Deadline()
				if !ok {
					t.Fatalf("expected the ceiling deadline to be present")
				}
				if until := time.Until(gotDeadline); until > ceiling {
					t.Errorf("time until deadline = %v, want at most the %v ceiling", until, ceiling)
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}

			_, err := interceptor(callerCtx, "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("err = %v, want context.DeadlineExceeded (should not have waited out the caller's own deadline)", err)
			}
		})
	}
}

func TestMaxDeadlineUnaryInterceptor_ShorterDeadlineIsPreserved(t *testing.T) {
	interceptor := MaxDeadlineUnaryInterceptor(time.Hour)

	callerCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	callerDeadline, _ := callerCtx.Deadline()

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		gotDeadline, ok := ctx.Deadline()
		if !ok {
			t.Fatalf("expected the caller's shorter deadline to be preserved")
		}
		if !gotDeadline.Equal(callerDeadline) {
			t.Errorf("deadline = %v, want caller's deadline %v (a ceiling above the caller's own deadline must not override it)", gotDeadline, callerDeadline)
		}
		return "response", nil
	}

	if _, err := interceptor(callerCtx, "request", &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}, handler); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func captureDefaultLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var log bytes.Buffer
	origLogger := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(origLogger)
	})
	slog.SetDefault(slog.New(slog.NewJSONHandler(&log, nil)))
	return &log
}

func TestServerUnaryInterceptorRedactsEnvValuesFromProtoRequestLogs(t *testing.T) {
	log := captureDefaultLog(t)

	req := &ateletpb.RunRequest{
		Spec: &ateletpb.WorkloadSpec{
			Containers: []*ateletpb.Container{
				{
					Name: "main",
					Env: []*ateletpb.EnvEntry{
						{Name: "API_KEY", Value: "sk-secret"},
						{Name: "PLAIN", Value: "not-a-secret"},
					},
				},
			},
		},
	}

	_, err := ServerUnaryInterceptor(context.Background(), req, &grpc.UnaryServerInfo{FullMethod: "/atelet.AteomHerder/Run"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return &ateletpb.RunResponse{}, nil
	})
	if err != nil {
		t.Fatalf("ServerUnaryInterceptor failed: %v", err)
	}

	gotLog := log.String()
	for _, secret := range []string{"sk-secret", "not-a-secret"} {
		if strings.Contains(gotLog, secret) {
			t.Fatalf("log contains env value %q: %s", secret, gotLog)
		}
	}
	// Names survive so the log still shows which variables were set.
	for _, want := range []string{`"name":"API_KEY"`, `"name":"PLAIN"`, `"value":"` + redactedPlaceholder + `"`} {
		if !strings.Contains(gotLog, want) {
			t.Fatalf("log missing %s: %s", want, gotLog)
		}
	}
	if got := req.GetSpec().GetContainers()[0].GetEnv()[0].GetValue(); got != "sk-secret" {
		t.Fatalf("interceptor mutated original request: env value = %q", got)
	}
}

func TestServerUnaryInterceptorRedactsActorJWTFromResponseLogs(t *testing.T) {
	log := captureDefaultLog(t)

	const token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhY3RvciJ9.c2lnbmF0dXJl"
	resp := &ateapipb.MintActorJWTResponse{ActorJwt: token}

	got, err := ServerUnaryInterceptor(context.Background(), &ateapipb.MintActorJWTRequest{}, &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/MintActorJWT"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return resp, nil
	})
	if err != nil {
		t.Fatalf("ServerUnaryInterceptor failed: %v", err)
	}

	gotLog := log.String()
	if strings.Contains(gotLog, token) {
		t.Fatalf("log contains the actor JWT: %s", gotLog)
	}
	if !strings.Contains(gotLog, `"actor_jwt":"`+redactedPlaceholder+`"`) {
		t.Fatalf("log does not show the redacted actor_jwt: %s", gotLog)
	}
	if got.(*ateapipb.MintActorJWTResponse).GetActorJwt() != token {
		t.Fatalf("interceptor mutated the response returned to the client")
	}
}

func TestInternalServerUnaryInterceptorRedactsBytesFields(t *testing.T) {
	log := captureDefaultLog(t)

	resp := &credproviderpb.FetchSecretResponse{OpaqueBytes: []byte("hunter2-hunter2")}
	_, err := InternalServerUnaryInterceptor(context.Background(), &credproviderpb.FetchSecretRequest{Uri: "ate-secret://kubernetes.io/ns/name"}, &grpc.UnaryServerInfo{FullMethod: "/credprovider.CredentialProvider/FetchSecret"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return resp, nil
	})
	if err != nil {
		t.Fatalf("InternalServerUnaryInterceptor failed: %v", err)
	}

	gotLog := log.String()
	// encoding/json renders []byte as base64; check both forms are absent.
	for _, leak := range []string{"hunter2-hunter2", "aHVudGVyMi1odW50ZXIy"} {
		if strings.Contains(gotLog, leak) {
			t.Fatalf("log contains the fetched secret: %s", gotLog)
		}
	}
	if !strings.Contains(gotLog, "ate-secret://kubernetes.io/ns/name") {
		t.Fatalf("log lost the non-sensitive request: %s", gotLog)
	}
	if string(resp.GetOpaqueBytes()) != "hunter2-hunter2" {
		t.Fatalf("interceptor mutated the response returned to the client")
	}
}

// TestDebugRedactFieldsArePinned lists every field across our protos that
// carries debug_redact. It fails when a label is added or removed so the
// change is reviewed as a deliberate decision about what the logs may show.
func TestDebugRedactFieldsArePinned(t *testing.T) {
	want := map[string]bool{
		"ateapi.EnvVar.value":                           true,
		"ateapi.MintActorJWTResponse.actor_jwt":         true,
		"atelet.EnvEntry.value":                         true,
		"credprovider.FetchSecretResponse.opaque_bytes": true,
	}
	got := map[string]bool{}
	var walk func(protoreflect.MessageDescriptors)
	walk = func(mds protoreflect.MessageDescriptors) {
		for i := 0; i < mds.Len(); i++ {
			md := mds.Get(i)
			fds := md.Fields()
			for j := 0; j < fds.Len(); j++ {
				fd := fds.Get(j)
				if opts, ok := fd.Options().(*descriptorpb.FieldOptions); ok && opts.GetDebugRedact() {
					got[string(fd.FullName())] = true
				}
			}
			walk(md.Messages())
		}
	}
	for _, file := range []protoreflect.FileDescriptor{ateapipb.File_ateapi_proto, ateletpb.File_atelet_proto, credproviderpb.File_credprovider_proto} {
		walk(file.Messages())
	}
	for name := range want {
		if !got[name] {
			t.Errorf("%s lost its debug_redact label; the interceptor would log it in clear", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("%s is newly marked debug_redact; add it to this list if that is intended", name)
		}
	}
}

// TestSanitizeForLogRecursesIntoRealMapFields walks real messages whose maps
// hold messages (atelet sandbox assets) and strings (ateapi selectors). The
// old walker skipped maps; the new one must descend into map values without
// panicking and leave non-sensitive content intact.
func TestSanitizeForLogRecursesIntoRealMapFields(t *testing.T) {
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
	got := sanitizeForLog(run).(*ateletpb.RunRequest)
	if v := got.GetSpec().GetContainers()[0].GetEnv()[0].GetValue(); v != redactedPlaceholder {
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
	gotTpl := sanitizeForLog(tpl).(*ateapipb.ActorTemplate)
	if gotTpl.GetWorkerSelector().GetMatchLabels()["workload"] != "agent" {
		t.Fatal("string map was altered")
	}
	if gotTpl.GetContainers()[0].GetEnv()[0].GetValue() != redactedPlaceholder {
		t.Fatal("EnvVar.value not masked")
	}
}

// redactTestSchema builds, at test time, a schema that exercises every branch
// of redactDebugRedactFields, including shapes our production protos do not
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

func TestRedactDebugRedactFieldsCoversEveryFieldShape(t *testing.T) {
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

	redactDebugRedactFields(outer)

	secretOf := func(m protoreflect.Message) string { return m.Get(innerDesc.Fields().ByName("secret")).String() }
	nameOf := func(m protoreflect.Message) string { return m.Get(innerDesc.Fields().ByName("name")).String() }

	// labeled field inside a map value: masked, sibling kept, map entry kept
	inMap := outer.Get(f("by_name")).Map().Get(protoreflect.ValueOfString("a").MapKey()).Message()
	if secretOf(inMap) != redactedPlaceholder || nameOf(inMap) != "n1" {
		t.Errorf("map value: secret=%q name=%q", secretOf(inMap), nameOf(inMap))
	}
	// labeled map, repeated string, scalar and bytes: cleared
	for _, name := range []string{"labels", "tokens", "count", "raw"} {
		if outer.Has(f(name)) {
			t.Errorf("%s should be cleared, got %v", name, outer.Get(f(name)))
		}
	}
	// nested singular and repeated messages: masked, siblings kept
	if one := outer.Get(f("one")).Message(); secretOf(one) != redactedPlaceholder || nameOf(one) != "n2" {
		t.Errorf("one: secret=%q name=%q", secretOf(one), nameOf(one))
	}
	if many := outer.Get(f("many")).List().Get(0).Message(); secretOf(many) != redactedPlaceholder || nameOf(many) != "n3" {
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
	if got := outer.Get(f("secret_choice")).String(); got != redactedPlaceholder {
		t.Errorf("secret_choice = %q", got)
	}
	if which := outer.WhichOneof(outerDesc.Oneofs().ByName("choice")); which == nil || which.Name() != "secret_choice" {
		t.Errorf("oneof selection changed: %v", which)
	}
}

func TestRedactDebugRedactFieldsLeavesUnsetFieldsUnset(t *testing.T) {
	outerDesc := redactTestSchema(t)
	innerDesc := outerDesc.ParentFile().Messages().ByName("Inner")
	// Inner with no secret set, nested under an unlabeled field.
	inner := dynamicpb.NewMessage(innerDesc)
	inner.Set(innerDesc.Fields().ByName("name"), protoreflect.ValueOfString("only-name"))
	outer := dynamicpb.NewMessage(outerDesc)
	outer.Set(outerDesc.Fields().ByName("one"), protoreflect.ValueOfMessage(inner))
	// A labeled oneof left unselected.

	redactDebugRedactFields(outer)

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

func TestSanitizeForLogPassesThroughNonProtoValues(t *testing.T) {
	for _, v := range []any{nil, "a string", 42, errors.New("boom"), struct{ X int }{1}} {
		if got := sanitizeForLog(v); got != v {
			t.Errorf("sanitizeForLog(%#v) = %#v, want the value unchanged", v, got)
		}
	}
	// A typed nil proto pointer, as a handler may return alongside an error,
	// must not panic and must come back as a nil message.
	var typedNil *ateapipb.MintActorJWTResponse
	got := sanitizeForLog(typedNil)
	if m, ok := got.(*ateapipb.MintActorJWTResponse); !ok || m != nil {
		t.Errorf("typed nil: got %#v", got)
	}
	// A nil interface with a handler error is the common error path.
	log := captureDefaultLog(t)
	_, err := ServerUnaryInterceptor(context.Background(), &ateapipb.MintActorJWTRequest{}, &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/MintActorJWT"}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, status.Error(codes.PermissionDenied, "no")
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(log.String(), `"resp":null`) {
		t.Errorf("nil response should log as null: %s", log.String())
	}
}
