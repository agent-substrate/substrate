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

package ateinterceptors

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// unknownField encodes a varint field that no descriptor in this binary
// declares, standing in for a field added by a newer client.
func unknownField(num protowire.Number) []byte {
	b := protowire.AppendTag(nil, num, protowire.VarintType)
	return protowire.AppendVarint(b, 42)
}

// withUnknown attaches an unknown field to m and returns m, so cases below read
// as a single expression.
func withUnknown[M proto.Message](m M, num protowire.Number) M {
	m.ProtoReflect().SetUnknown(unknownField(num))
	return m
}

func TestFindUnknownFields(t *testing.T) {
	tests := []struct {
		name string
		in   func() proto.Message
		want field.ErrorList
	}{
		{
			name: "no unknown fields",
			in: func() proto.Message {
				return &ateapipb.CreateActorRequest{
					Actor: &ateapipb.Actor{
						Metadata:       &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
						ActorTemplate:  &ateapipb.ObjectRef{Atespace: "team-a", Name: "tmpl1"},
						WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
					},
				}
			},
			want: nil,
		},
		{
			name: "nil message",
			in: func() proto.Message {
				return (*ateapipb.CreateActorRequest)(nil)
			},
			want: nil,
		},
		{
			name: "unknown field on the request itself",
			in: func() proto.Message {
				return withUnknown(&ateapipb.CreateActorRequest{}, 9999)
			},
			want: field.ErrorList{field.Invalid(field.NewPath("request"), field.OmitValueType{}, "")},
		},
		{
			name: "unknown field on a nested message",
			in: func() proto.Message {
				return &ateapipb.CreateActorRequest{
					Actor: &ateapipb.Actor{
						Metadata: withUnknown(&ateapipb.ResourceMetadata{Name: "actor-1"}, 9999),
					},
				}
			},
			want: field.ErrorList{field.Invalid(field.NewPath("actor", "metadata"), field.OmitValueType{}, "")},
		},
		{
			name: "unknown fields on several messages",
			in: func() proto.Message {
				return withUnknown(&ateapipb.CreateActorRequest{
					Actor: &ateapipb.Actor{
						Metadata:       withUnknown(&ateapipb.ResourceMetadata{Name: "actor-1"}, 9998),
						WorkerSelector: withUnknown(&ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}, 9997),
					},
				}, 9999)
			},
			want: field.ErrorList{
				field.Invalid(field.NewPath("request"), field.OmitValueType{}, ""),
				field.Invalid(field.NewPath("actor", "metadata"), field.OmitValueType{}, ""),
				field.Invalid(field.NewPath("actor", "worker_selector"), field.OmitValueType{}, ""),
			},
		},
		{
			name: "several unknown fields on one message collapse to a single error",
			in: func() proto.Message {
				m := &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl1"}}
				m.ProtoReflect().SetUnknown(append(unknownField(9998), unknownField(9999)...))
				return m
			},
			want: field.ErrorList{field.Invalid(field.NewPath("request"), field.OmitValueType{}, "")},
		},
		{
			name: "unknown field inside a repeated message element",
			in: func() proto.Message {
				return &ateapipb.ListActorsResponse{
					Actors: []*ateapipb.Actor{
						{ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl1"}},
						withUnknown(&ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl2"}}, 9999),
					},
				}
			},
			want: field.ErrorList{field.Invalid(field.NewPath("actors").Index(1), field.OmitValueType{}, "")},
		},
		{
			name: "unknown field inside a map value message",
			in: func() proto.Message {
				return &ateletpb.SandboxAssets{
					Assets: map[string]*ateletpb.ArchAssets{
						"amd64": withUnknown(&ateletpb.ArchAssets{}, 9999),
					},
				}
			},
			want: field.ErrorList{field.Invalid(field.NewPath("assets").Key("amd64"), field.OmitValueType{}, "")},
		},
		{
			name: "scalar map values cannot carry unknown fields",
			in: func() proto.Message {
				return &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid", "region": "us"}}
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findUnknownFields(tt.in())
			field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, tt.want, got)
		})
	}
}

func TestRejectUnknownFieldsUnaryInterceptor(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	handler := func(ctx context.Context, req any) (any, error) {
		return "handled", nil
	}

	t.Run("clean request reaches the handler", func(t *testing.T) {
		resp, err := RejectUnknownFieldsUnaryInterceptor(context.Background(),
			&ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl1"}}}, info, handler)
		if err != nil {
			t.Fatalf("interceptor error = %v, want nil", err)
		}
		if resp != "handled" {
			t.Errorf("resp = %v, want %q", resp, "handled")
		}
	})

	t.Run("unknown field is rejected before the handler", func(t *testing.T) {
		called := false
		_, err := RejectUnknownFieldsUnaryInterceptor(context.Background(),
			withUnknown(&ateapipb.CreateActorRequest{}, 9999), info,
			func(ctx context.Context, req any) (any, error) {
				called = true
				return nil, nil
			})
		if got, want := apierror.Code(err), codes.InvalidArgument; got != want {
			t.Fatalf("status code = %v, want %v (error: %v)", got, want, err)
		}
		if got, want := err.Error(), "request: Invalid value: unknown field with protobuf tag 9999"; got != want {
			t.Errorf("error message = %q, want %q", got, want)
		}
		if called {
			t.Error("handler was called despite the rejection")
		}
	})

	t.Run("non-proto request passes through", func(t *testing.T) {
		resp, err := RejectUnknownFieldsUnaryInterceptor(context.Background(), "not a proto", info, handler)
		if err != nil {
			t.Fatalf("interceptor error = %v, want nil", err)
		}
		if resp != "handled" {
			t.Errorf("resp = %v, want %q", resp, "handled")
		}
	})
}

// templateRequest is a CreateActorTemplateRequest whose snapshot_config carries
// the given unknown bytes.
func templateRequest(unknown []byte) *ateapipb.CreateActorTemplateRequest {
	cfg := &ateapipb.SnapshotConfig{StorageLocation: "s3://bucket/snapshots"}
	cfg.ProtoReflect().SetUnknown(unknown)
	return &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata:       &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tmpl1"},
			SnapshotConfig: cfg,
		},
	}
}

// onPause encodes the removed SnapshotConfig.on_pause as a client built
// against the older API sends it.
func onPause() []byte {
	b := protowire.AppendTag(nil, 1, protowire.VarintType)
	return protowire.AppendVarint(b, 1)
}

func TestRejectUnknownFieldsUnaryInterceptorRemovedFields(t *testing.T) {
	info := &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/CreateActorTemplate"}
	ctx := principal.InjectContext(context.Background(), principal.PrincipalInfo{ID: "spiffe://cluster.local/ns/kagent/sa/kagent-controller", Kind: principal.KindMTLS})

	t.Run("on_pause is accepted and dropped", func(t *testing.T) {
		removedFieldLogged.Clear()
		log := captureDefaultLog(t)
		var got *ateapipb.CreateActorTemplateRequest
		handler := func(ctx context.Context, req any) (any, error) {
			got = req.(*ateapipb.CreateActorTemplateRequest)
			return "handled", nil
		}

		for range 2 {
			if _, err := RejectUnknownFieldsUnaryInterceptor(ctx, templateRequest(onPause()), info, handler); err != nil {
				t.Fatalf("interceptor error = %v, want nil", err)
			}
		}
		if unknown := got.GetActorTemplate().GetSnapshotConfig().ProtoReflect().GetUnknown(); len(unknown) != 0 {
			t.Errorf("snapshot_config unknown fields = %x, want none", unknown)
		}
		if got, want := got.GetActorTemplate().GetSnapshotConfig().GetStorageLocation(), "s3://bucket/snapshots"; got != want {
			t.Errorf("storage_location = %q, want %q", got, want)
		}
		if got, want := strings.Count(log.String(), "Request carries a removed field"), 1; got != want {
			t.Fatalf("removed-field log lines = %d, want %d (once per principal): %s", got, want, log.String())
		}
		for _, want := range []string{`"field":"actor_template.snapshot_config.on_pause"`, `"method":"/ateapi.Control/CreateActorTemplate"`, "kagent-controller"} {
			if !strings.Contains(log.String(), want) {
				t.Errorf("log missing %s: %s", want, log.String())
			}
		}

		other := principal.InjectContext(context.Background(), principal.PrincipalInfo{ID: "another-client", Kind: principal.KindJWT})
		if _, err := RejectUnknownFieldsUnaryInterceptor(other, templateRequest(onPause()), info, handler); err != nil {
			t.Fatalf("interceptor error = %v, want nil", err)
		}
		if got, want := strings.Count(log.String(), "Request carries a removed field"), 2; got != want {
			t.Errorf("removed-field log lines = %d, want %d (a second principal is logged): %s", got, want, log.String())
		}
	})

	tests := []struct {
		name    string
		unknown []byte
		wantErr string
	}{
		{
			name:    "another unknown tag is still refused",
			unknown: unknownField(9999),
			wantErr: "actor_template.snapshot_config: Invalid value: unknown field with protobuf tag 9999",
		},
		{
			name:    "on_pause beside another unknown tag refuses the other one",
			unknown: append(onPause(), unknownField(9999)...),
			wantErr: "actor_template.snapshot_config: Invalid value: unknown field with protobuf tag 9999",
		},
		{
			name:    "tag 1 with another wire type is not on_pause and is refused",
			unknown: protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), "s3://bucket"),
			wantErr: "actor_template.snapshot_config: Invalid value: unknown field with protobuf tag 1",
		},
		{
			name:    "a request without on_pause reaches the handler",
			unknown: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			_, err := RejectUnknownFieldsUnaryInterceptor(ctx, templateRequest(tt.unknown), info,
				func(ctx context.Context, req any) (any, error) {
					called = true
					return "handled", nil
				})
			if tt.wantErr == "" {
				if err != nil || !called {
					t.Fatalf("interceptor error = %v, handler called = %v; want nil, true", err, called)
				}
				return
			}
			if got, want := apierror.Code(err), codes.InvalidArgument; got != want {
				t.Fatalf("status code = %v, want %v (error: %v)", got, want, err)
			}
			if got := err.Error(); got != tt.wantErr {
				t.Errorf("error message = %q, want %q", got, tt.wantErr)
			}
			if called {
				t.Error("handler was called despite the rejection")
			}
		})
	}

	t.Run("tag 1 on another message is refused", func(t *testing.T) {
		_, err := RejectUnknownFieldsUnaryInterceptor(ctx, withUnknown(&ateapipb.CreateActorTemplateRequest{}, 1), info,
			func(ctx context.Context, req any) (any, error) { return nil, nil })
		if got, want := apierror.Code(err), codes.InvalidArgument; got != want {
			t.Fatalf("status code = %v, want %v (error: %v)", got, want, err)
		}
	})
}
