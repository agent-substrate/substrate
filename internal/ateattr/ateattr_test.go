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

	"github.com/agent-substrate/substrate/internal/resources"
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

func TestActorAttributes(t *testing.T) {
	tests := []struct {
		name  string
		actor *ateapipb.Actor
		want  map[attribute.Key]any
	}{
		{
			name: "full actor",
			actor: &ateapipb.Actor{
				Metadata:               &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "support-agent-42", Uid: "uid-abc", Version: 7},
				ActorTemplateNamespace: "ate-agents",
				ActorTemplateName:      "support-agent",
			},
			want: map[attribute.Key]any{
				AtespaceKey:          "team-a",
				ActorNameKey:         "support-agent-42",
				ActorUIDKey:          "uid-abc",
				TemplateNameKey:      "support-agent",
				TemplateNamespaceKey: "ate-agents",
				ActorVersionKey:      int64(7),
			},
		},
		{
			name:  "nil actor yields zero values, not a panic",
			actor: nil,
			want: map[attribute.Key]any{
				AtespaceKey:          "",
				ActorNameKey:         "",
				ActorUIDKey:          "",
				TemplateNameKey:      "",
				TemplateNamespaceKey: "",
				ActorVersionKey:      int64(0),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertAttrs(t, toMap(ActorAttributes(tt.actor)), tt.want)
		})
	}
}

func TestActorRefAttributes(t *testing.T) {
	tests := []struct {
		name     string
		actorRef resources.ActorRef
		want     map[attribute.Key]any
	}{
		{
			name:     "atespace and actor name only",
			actorRef: resources.ActorRef{Atespace: "team-a", Name: "support-agent-42"},
			want: map[attribute.Key]any{
				AtespaceKey:  "team-a",
				ActorNameKey: "support-agent-42",
			},
		},
		{
			name:     "zero ref still produces both keys",
			actorRef: resources.ActorRef{},
			want: map[attribute.Key]any{
				AtespaceKey:  "",
				ActorNameKey: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertAttrs(t, toMap(ActorRefAttributes(tt.actorRef)), tt.want)
		})
	}
}

// TestKeySpellings pins the wire spelling of every key. Renaming one silently
// breaks dashboards, alerts, and the contract between ateapi and atelet, so a
// drift must fail here rather than in production.
func TestKeySpellings(t *testing.T) {
	tests := []struct {
		key  attribute.Key
		want string
	}{
		{AtespaceKey, "ate.atespace"},
		{ActorNameKey, "ate.actor.name"},
		{ActorUIDKey, "ate.actor.uid"},
		{TemplateNameKey, "ate.template.name"},
		{TemplateNamespaceKey, "ate.template.namespace"},
		{ActorVersionKey, "ate.actor.version"},
		{ActorOperationNameKey, "ate.actor.operation.name"},
		{WorkerPoolNamespaceKey, "ate.workerpool.namespace"},
		{WorkerPoolNameKey, "ate.workerpool.name"},
		{WorkerStateKey, "ate.worker.state"},
		{SandboxClassKey, "ate.sandbox.class"},
		{SnapshotKindKey, "ate.snapshot.kind"},
		{SchedulerOutcomeKey, "ate.scheduler.outcome"},
		{ErrorTypeKey, "error.type"},
		{FailureReasonKey, "ate.failure.reason"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if string(tt.key) != tt.want {
				t.Errorf("key = %q, want %q", string(tt.key), tt.want)
			}
		})
	}
}

// TestMetricLabelValues pins the wire value of every bounded metric-label
// constant. These are the exact strings dashboards and alerts group by, so a
// typo or rename must fail here rather than silently fork a time series in
// production.
func TestMetricLabelValues(t *testing.T) {
	tests := []struct {
		got  string
		want string
	}{
		{WorkerStateIdle, "idle"},
		{WorkerStateAssigned, "assigned"},

		{OperationCreate, "create"},
		{OperationResume, "resume"},
		{OperationSuspend, "suspend"},
		{OperationPause, "pause"},
		{OperationDelete, "delete"},
		{OperationUnknown, "unknown"},

		{SchedulerOutcomeAssigned, "assigned"},
		{SchedulerOutcomeNoFreeWorker, "no_free_worker"},
		{SchedulerOutcomeError, "error"},

		{SnapshotKindGolden, "golden"},
		{SnapshotKindLatest, "latest"},
		{SnapshotKindLocal, "local"},
		{SnapshotKindBoot, "boot"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestActorMetricAttributes(t *testing.T) {
	actor := &ateapipb.Actor{
		ActorTemplateNamespace: "default",
		ActorTemplateName:      "counter-template",
		WorkerAssignment: &ateapipb.WorkerAssignment{
			WorkerPool: "default-pool",
		},
	}

	t.Run("explicit operation and reason", func(t *testing.T) {
		got := toMap(ActorMetricAttributes(actor, "gvisor", OperationResume, ReasonCorruptedAssignment))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:  "default",
			TemplateNameKey:       "counter-template",
			WorkerPoolNameKey:     "default-pool",
			SandboxClassKey:       "gvisor",
			ActorOperationNameKey: OperationResume,
			FailureReasonKey:      ReasonCorruptedAssignment,
		}

		assertAttrs(t, got, want)
	})

	t.Run("default unknown values", func(t *testing.T) {
		got := toMap(ActorMetricAttributes(actor, "gvisor", "", ""))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:  "default",
			TemplateNameKey:       "counter-template",
			WorkerPoolNameKey:     "default-pool",
			SandboxClassKey:       "gvisor",
			ActorOperationNameKey: OperationUnknown,
			FailureReasonKey:      ReasonUnknown,
		}

		assertAttrs(t, got, want)
	})

	t.Run("out of range operation name is normalized to unknown", func(t *testing.T) {
		got := toMap(ActorMetricAttributes(actor, "gvisor", "invalid_op", ""))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:  "default",
			TemplateNameKey:       "counter-template",
			WorkerPoolNameKey:     "default-pool",
			SandboxClassKey:       "gvisor",
			ActorOperationNameKey: OperationUnknown,
			FailureReasonKey:      ReasonUnknown,
		}

		assertAttrs(t, got, want)
	})
}

func TestNormalizeOperationName(t *testing.T) {
	tests := []struct {
		op   string
		want string
	}{
		{OperationCreate, OperationCreate},
		{OperationResume, OperationResume},
		{OperationSuspend, OperationSuspend},
		{OperationPause, OperationPause},
		{OperationDelete, OperationDelete},
		{"", OperationUnknown},
		{"invalid", OperationUnknown},
		{"crash", OperationUnknown},
	}
	for _, tt := range tests {
		if got := NormalizeOperationName(tt.op); got != tt.want {
			t.Errorf("NormalizeOperationName(%q) = %q, want %q", tt.op, got, tt.want)
		}
	}
}
