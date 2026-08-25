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

package controlapi

import (
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation/field"

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

func TestValidateNoUnknownFields(t *testing.T) {
	root := field.NewPath("actor")

	tests := []struct {
		name string
		in   func() proto.Message
		want field.ErrorList
	}{
		{
			name: "no unknown fields",
			in: func() proto.Message {
				return &ateapipb.Actor{
					Metadata:          &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
					ActorTemplateName: "tmpl1",
					WorkerSelector:    &ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}},
				}
			},
			want: nil,
		},
		{
			name: "nil message",
			in: func() proto.Message {
				return (*ateapipb.Actor)(nil)
			},
			want: nil,
		},
		{
			name: "unknown field at the top level",
			in: func() proto.Message {
				return withUnknown(&ateapipb.Actor{ActorTemplateName: "tmpl1"}, 9999)
			},
			want: field.ErrorList{field.Invalid(root, field.OmitValueType{}, "")},
		},
		{
			name: "unknown field nested in a singular message",
			in: func() proto.Message {
				return &ateapipb.Actor{
					Metadata: withUnknown(&ateapipb.ResourceMetadata{Name: "actor-1"}, 9999),
				}
			},
			want: field.ErrorList{field.Invalid(root.Child("metadata"), field.OmitValueType{}, "")},
		},
		{
			name: "unknown field several levels deep",
			in: func() proto.Message {
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{
						Name:       "actor-1",
						UpdateTime: withUnknown(timestamppb.New(time.Unix(1, 0)), 9999),
					},
				}
			},
			want: field.ErrorList{field.Invalid(root.Child("metadata").Child("update_time"), field.OmitValueType{}, "")},
		},
		{
			name: "unknown fields at several levels are all reported",
			in: func() proto.Message {
				return withUnknown(&ateapipb.Actor{
					Metadata:       withUnknown(&ateapipb.ResourceMetadata{Name: "actor-1"}, 9998),
					WorkerSelector: withUnknown(&ateapipb.Selector{MatchLabels: map[string]string{"tier": "paid"}}, 9997),
				}, 9999)
			},
			want: field.ErrorList{
				field.Invalid(root, field.OmitValueType{}, ""),
				field.Invalid(root.Child("metadata"), field.OmitValueType{}, ""),
				field.Invalid(root.Child("worker_selector"), field.OmitValueType{}, ""),
			},
		},
		{
			name: "several unknown fields on one message collapse to a single error",
			in: func() proto.Message {
				m := &ateapipb.Actor{ActorTemplateName: "tmpl1"}
				m.ProtoReflect().SetUnknown(append(unknownField(9998), unknownField(9999)...))
				return m
			},
			want: field.ErrorList{field.Invalid(root, field.OmitValueType{}, "")},
		},
		{
			name: "unknown field inside a repeated message element",
			in: func() proto.Message {
				return &ateapipb.ListActorsResponse{
					Actors: []*ateapipb.Actor{
						{ActorTemplateName: "tmpl1"},
						withUnknown(&ateapipb.Actor{ActorTemplateName: "tmpl2"}, 9999),
					},
				}
			},
			want: field.ErrorList{field.Invalid(root.Child("actors").Index(1), field.OmitValueType{}, "")},
		},
		{
			name: "unknown field inside a map value message",
			in: func() proto.Message {
				return &ateapipb.SandboxAssets{
					Assets: map[string]*ateapipb.ArchAssets{
						"amd64": withUnknown(&ateapipb.ArchAssets{}, 9999),
					},
				}
			},
			want: field.ErrorList{field.Invalid(root.Child("assets").Key("amd64"), field.OmitValueType{}, "")},
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
			assertValidateErr(t, validateNoUnknownFields(tt.in(), root), tt.want)
		})
	}
}
