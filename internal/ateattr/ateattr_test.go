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

package ateattr

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func toMap(kvs []attribute.KeyValue) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

// assertAttrs checks each expected key is present with the expected value and
// OTel type. want values are string or int64; int64 doubles as the "version must
// not be stringified" check.
func assertAttrs(t *testing.T, got map[attribute.Key]attribute.Value, want map[attribute.Key]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got %d attributes, want %d: %v", len(got), len(want), got)
	}
	for k, wv := range want {
		v, ok := got[k]
		if !ok {
			t.Errorf("missing attribute %s", k)
			continue
		}
		switch exp := wv.(type) {
		case string:
			if v.Type() != attribute.STRING || v.AsString() != exp {
				t.Errorf("%s = %v (%s), want string %q", k, v.Emit(), v.Type(), exp)
			}
		case int64:
			if v.Type() != attribute.INT64 || v.AsInt64() != exp {
				t.Errorf("%s = %v (%s), want int64 %d", k, v.Emit(), v.Type(), exp)
			}
		default:
			t.Fatalf("unsupported want type for %s: %T", k, wv)
		}
	}
}

func TestActorIdentity(t *testing.T) {
	tests := []struct {
		name  string
		actor *ateapipb.Actor
		want  map[attribute.Key]any
	}{
		{
			name: "full actor",
			actor: &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "support-agent-42", Version: 7},
				ActorTemplateNamespace: "ate-agents",
				ActorTemplateName:      "support-agent",
			},
			want: map[attribute.Key]any{
				AtespaceKey:               "team-a",
				ActorIDKey:                "support-agent-42",
				ActorTemplateNameKey:      "support-agent",
				ActorTemplateNamespaceKey: "ate-agents",
				ActorVersionKey:           int64(7),
			},
		},
		{
			name:  "nil actor yields zero values, not a panic",
			actor: nil,
			want: map[attribute.Key]any{
				AtespaceKey:               "",
				ActorIDKey:                "",
				ActorTemplateNameKey:      "",
				ActorTemplateNamespaceKey: "",
				ActorVersionKey:           int64(0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertAttrs(t, toMap(ActorIdentity(tt.actor)), tt.want)
		})
	}
}

func TestActorRefIdentity(t *testing.T) {
	tests := []struct {
		name     string
		atespace string
		actorID  string
		want     map[attribute.Key]any
	}{
		{
			name:     "atespace and actor id only",
			atespace: "team-a",
			actorID:  "support-agent-42",
			want: map[attribute.Key]any{
				AtespaceKey: "team-a",
				ActorIDKey:  "support-agent-42",
			},
		},
		{
			name:     "empty values still produce both keys",
			atespace: "",
			actorID:  "",
			want: map[attribute.Key]any{
				AtespaceKey: "",
				ActorIDKey:  "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertAttrs(t, toMap(ActorRefIdentity(tt.atespace, tt.actorID)), tt.want)
		})
	}
}
