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

func TestActorIdentity(t *testing.T) {
	a := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "team-a",
			Name:     "support-agent-42",
			Version:  7,
		},
		ActorTemplateNamespace: "ate-agents",
		ActorTemplateName:      "support-agent",
	}

	got := toMap(ActorIdentity(a))

	wantStr := map[attribute.Key]string{
		AtespaceKey:               "team-a",
		ActorIDKey:                "support-agent-42",
		ActorTemplateNameKey:      "support-agent",
		ActorTemplateNamespaceKey: "ate-agents",
	}
	for k, want := range wantStr {
		if v, ok := got[k]; !ok {
			t.Errorf("missing attribute %s", k)
		} else if v.AsString() != want {
			t.Errorf("%s = %q, want %q", k, v.AsString(), want)
		}
	}

	// version must be typed int64, not stringified.
	if v := got[ActorVersionKey]; v.Type() != attribute.INT64 {
		t.Errorf("%s type = %v, want INT64", ActorVersionKey, v.Type())
	} else if v.AsInt64() != 7 {
		t.Errorf("%s = %d, want 7", ActorVersionKey, v.AsInt64())
	}
}

func TestActorIdentityNilSafe(t *testing.T) {
	got := toMap(ActorIdentity(nil))
	if v := got[AtespaceKey]; v.AsString() != "" {
		t.Errorf("%s = %q, want empty", AtespaceKey, v.AsString())
	}
	if v := got[ActorVersionKey]; v.AsInt64() != 0 {
		t.Errorf("%s = %d, want 0", ActorVersionKey, v.AsInt64())
	}
}

func TestActorRefIdentity(t *testing.T) {
	kvs := ActorRefIdentity("team-a", "support-agent-42")
	if len(kvs) != 2 {
		t.Fatalf("len = %d, want 2", len(kvs))
	}
	got := toMap(kvs)
	if got[AtespaceKey].AsString() != "team-a" {
		t.Errorf("%s = %q, want team-a", AtespaceKey, got[AtespaceKey].AsString())
	}
	if got[ActorIDKey].AsString() != "support-agent-42" {
		t.Errorf("%s = %q, want support-agent-42", ActorIDKey, got[ActorIDKey].AsString())
	}
}
