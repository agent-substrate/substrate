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
	"context"
	"errors"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
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
				t.Errorf("%s = %v (%s), want string %q", k, v.String(), v.Type(), exp)
			}
		case int64:
			if v.Type() != attribute.INT64 || v.AsInt64() != exp {
				t.Errorf("%s = %v (%s), want int64 %d", k, v.String(), v.Type(), exp)
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
		{ActorContainerNameKey, "ate.actor.container.name"},
		{TemplateNameKey, "ate.template.name"},
		{TemplateNamespaceKey, "ate.template.namespace"},
		{ActorVersionKey, "ate.actor.version"},
		{ActorOperationNameKey, "ate.actor.operation.name"},
		{WorkerPoolNamespaceKey, "ate.workerpool.namespace"},
		{WorkerPoolNameKey, "ate.workerpool.name"},
		{WorkerStateKey, "ate.worker.state"},
		{SandboxClassKey, "ate.sandbox.class"},
		{SnapshotKindKey, "ate.snapshot.kind"},
		{SnapshotScopeKey, "ate.snapshot.scope"},
		{SnapshotPhaseKey, "ate.snapshot.phase"},
		{ImageCacheOutcomeKey, "ate.imagecache.outcome"},
		{SchedulerOutcomeKey, "ate.scheduler.outcome"},
		{ErrorTypeKey, "error.type"},
		{FailureReasonKey, "ate.failure.reason"},
		{OTLPRelayKey, "ate.otlp.relay"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if string(tt.key) != tt.want {
				t.Errorf("key = %q, want %q", string(tt.key), tt.want)
			}
		})
	}
}

// TestLogFieldSpellings pins the trace-context field names. These are fixed by
// the OTel spec for non-OTLP log formats, not ours to choose: a collector only
// recognizes them at these exact spellings.
func TestLogFieldSpellings(t *testing.T) {
	tests := []struct {
		got  string
		want string
	}{
		{LogTraceIDField, "trace_id"},
		{LogSpanIDField, "span_id"},
		{LogTraceFlagsField, "trace_flags"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("field = %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestActorLogLabels(t *testing.T) {
	const containerName = "counter"

	attribution := resources.ActorAttribution{
		Ref:               resources.ActorRef{Atespace: "team-a", Name: "support-agent-42"},
		UID:               "uid-abc",
		TemplateNamespace: "ate-agents",
		TemplateName:      "support-agent",
	}

	tests := []struct {
		name          string
		attribution   resources.ActorAttribution
		containerName string
		want          map[string]string
	}{
		{
			name:          "container output carries the container name",
			attribution:   attribution,
			containerName: containerName,
			want: map[string]string{
				"ate.atespace":             "team-a",
				"ate.actor.name":           "support-agent-42",
				"ate.actor.uid":            "uid-abc",
				"ate.actor.container.name": containerName,
				"ate.template.namespace":   "ate-agents",
				"ate.template.name":        "support-agent",
			},
		},
		{
			name:          "no container name omits the key rather than emitting it empty",
			attribution:   attribution,
			containerName: "",
			want: map[string]string{
				"ate.atespace":           "team-a",
				"ate.actor.name":         "support-agent-42",
				"ate.actor.uid":          "uid-abc",
				"ate.template.namespace": "ate-agents",
				"ate.template.name":      "support-agent",
			},
		},
		{
			name:          "zero attribution still produces the five identity keys",
			attribution:   resources.ActorAttribution{},
			containerName: "",
			want: map[string]string{
				"ate.atespace":           "",
				"ate.actor.name":         "",
				"ate.actor.uid":          "",
				"ate.template.namespace": "",
				"ate.template.name":      "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ActorLogLabels(tt.attribution, tt.containerName)
			if len(got) != len(tt.want) {
				t.Errorf("got %d labels, want %d: %v", len(got), len(tt.want), got)
			}
			for k, want := range tt.want {
				if v, ok := got[k]; !ok {
					t.Errorf("missing label %s", k)
				} else if v != want {
					t.Errorf("%s = %q, want %q", k, v, want)
				}
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

		{ImageCacheOutcomeHit, "hit"},
		{ImageCacheOutcomeMiss, "miss"},
		{ImageCacheOutcomeError, "error"},
		{ImageCacheOutcomeCancelled, "cancelled"},
		{ImageCacheOutcomeTimeout, "timeout"},

		{SchedulerOutcomeAssigned, "assigned"},
		{SchedulerOutcomeNoFreeWorker, "no_free_worker"},
		{SchedulerOutcomeError, "error"},

		{SnapshotKindGolden, "golden"},
		{SnapshotKindLatest, "latest"},
		{SnapshotKindLocal, "local"},
		{SnapshotKindBoot, "boot"},

		{SnapshotScopeFull, "full"},
		{SnapshotScopeData, "data"},
		{SnapshotScopeDataOnGolden, "data_on_golden"},
		{SnapshotScopeUnknown, "unknown"},

		{SnapshotPhaseVolumeMount, "volume_mount"},
		{SnapshotPhaseManifestFetch, "manifest_fetch"},
		{SnapshotPhaseSandboxAssets, "sandbox_assets"},
		{SnapshotPhaseDownload, "download"},
		{SnapshotPhaseOCIUnpack, "oci_unpack"},
		{SnapshotPhaseAteomRestore, "ateom_restore"},
		{SnapshotPhaseAteomCheckpoint, "ateom_checkpoint"},
		{SnapshotPhasePersist, "persist"},
		{SnapshotPhaseTotal, "total"},

		{SandboxClassUnknown, "unknown"},
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
		Status: &ateapipb.ActorStatus{
			WorkerAssignment: &ateapipb.WorkerAssignment{
				WorkerNamespace: "ate-workers",
				WorkerPool:      "default-pool",
			},
		},
	}

	t.Run("explicit operation and reason", func(t *testing.T) {
		got := toMap(ActorMetricAttributes(actor, "gvisor", OperationResume, ReasonCorruptedAssignment))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:   "default",
			TemplateNameKey:        "counter-template",
			WorkerPoolNamespaceKey: "ate-workers",
			WorkerPoolNameKey:      "default-pool",
			SandboxClassKey:        "gvisor",
			ActorOperationNameKey:  OperationResume,
			FailureReasonKey:       ReasonCorruptedAssignment,
		}

		assertAttrs(t, got, want)
	})

	t.Run("default unknown values", func(t *testing.T) {
		got := toMap(ActorMetricAttributes(actor, "gvisor", "", ""))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:   "default",
			TemplateNameKey:        "counter-template",
			WorkerPoolNamespaceKey: "ate-workers",
			WorkerPoolNameKey:      "default-pool",
			SandboxClassKey:        "gvisor",
			ActorOperationNameKey:  OperationUnknown,
			FailureReasonKey:       ReasonUnknown,
		}

		assertAttrs(t, got, want)
	})

	t.Run("out of range operation name is normalized to unknown", func(t *testing.T) {
		got := toMap(ActorMetricAttributes(actor, "gvisor", "invalid_op", ""))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:   "default",
			TemplateNameKey:        "counter-template",
			WorkerPoolNamespaceKey: "ate-workers",
			WorkerPoolNameKey:      "default-pool",
			SandboxClassKey:        "gvisor",
			ActorOperationNameKey:  OperationUnknown,
			FailureReasonKey:       ReasonUnknown,
		}

		assertAttrs(t, got, want)
	})

	t.Run("empty template ref reports unknown", func(t *testing.T) {
		noTemplate := &ateapipb.Actor{
			Status: &ateapipb.ActorStatus{
				WorkerAssignment: &ateapipb.WorkerAssignment{
					WorkerNamespace: "ate-workers",
					WorkerPool:      "default-pool",
				},
			},
		}
		got := toMap(ActorMetricAttributes(noTemplate, "gvisor", OperationResume, ReasonUnknown))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:   TemplateUnknown,
			TemplateNameKey:        TemplateUnknown,
			WorkerPoolNamespaceKey: "ate-workers",
			WorkerPoolNameKey:      "default-pool",
			SandboxClassKey:        "gvisor",
			ActorOperationNameKey:  OperationResume,
			FailureReasonKey:       ReasonUnknown,
		}

		assertAttrs(t, got, want)
	})

	// An actor that crashed before it reached a worker has no pool. Reporting
	// one key of the pair, or an empty-string name, would put that crash in a
	// series that looks like a real pool.
	t.Run("unassigned actor omits both pool keys", func(t *testing.T) {
		unassigned := &ateapipb.Actor{
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "counter-template",
		}
		got := toMap(ActorMetricAttributes(unassigned, "gvisor", OperationCreate, ReasonUnknown))
		want := map[attribute.Key]any{
			TemplateNamespaceKey:  "default",
			TemplateNameKey:       "counter-template",
			SandboxClassKey:       "gvisor",
			ActorOperationNameKey: OperationCreate,
			FailureReasonKey:      ReasonUnknown,
		}

		assertAttrs(t, got, want)
	})
}

// TestWorkerPoolAttributes pins the both-or-neither rule. A WorkerPool is
// namespaced, so a name on its own merges same-named pools from different
// namespaces and cannot join against the instruments that carry the pair.
func TestWorkerPoolAttributes(t *testing.T) {
	t.Run("known pool returns the pair", func(t *testing.T) {
		got := toMap(WorkerPoolAttributes("ate-workers", "pool-a"))
		want := map[attribute.Key]any{
			WorkerPoolNamespaceKey: "ate-workers",
			WorkerPoolNameKey:      "pool-a",
		}

		assertAttrs(t, got, want)
	})

	t.Run("unknown pool returns neither key", func(t *testing.T) {
		for _, namespace := range []string{"", "ate-workers"} {
			if got := WorkerPoolAttributes(namespace, ""); len(got) != 0 {
				t.Errorf("WorkerPoolAttributes(%q, \"\") = %v, want no attributes", namespace, got)
			}
		}
	})

	// The reverse of the case this helper exists for: a name without a namespace
	// half-identifies the pool, which joins to nothing on the paired instruments.
	t.Run("name without a namespace returns neither key", func(t *testing.T) {
		if got := WorkerPoolAttributes("", "pool-a"); len(got) != 0 {
			t.Errorf("WorkerPoolAttributes(\"\", \"pool-a\") = %v, want no attributes", got)
		}
	})
}

// TestSnapshotScopeValue pins the enum-to-label mapping ateapi and atelet share.
// An unmapped enum value must report unknown rather than its stringified form,
// which would let a wire value widen the label set.
func TestSnapshotScopeValue(t *testing.T) {
	tests := []struct {
		name  string
		scope ateletpb.SnapshotScope
		want  string
	}{
		{name: "full", scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL, want: SnapshotScopeFull},
		{name: "data", scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA, want: SnapshotScopeData},
		{name: "data on golden", scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN, want: SnapshotScopeDataOnGolden},
		{name: "unspecified", scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED, want: SnapshotScopeUnknown},
		{name: "value outside the enum", scope: ateletpb.SnapshotScope(9999), want: SnapshotScopeUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SnapshotScopeValue(tt.scope); got != tt.want {
				t.Errorf("SnapshotScopeValue(%v) = %q, want %q", tt.scope, got, tt.want)
			}
		})
	}
}

// TestNormalizeSandboxClass covers the cardinality guard: atelet reads the class
// out of a snapshot manifest nothing validates, so anything unrecognized has to
// collapse onto a single value.
func TestNormalizeSandboxClass(t *testing.T) {
	tests := []struct {
		name  string
		class string
		want  string
	}{
		{name: "gvisor", class: string(atev1alpha1.SandboxClassGvisor), want: string(atev1alpha1.SandboxClassGvisor)},
		{name: "microvm", class: string(atev1alpha1.SandboxClassMicroVM), want: string(atev1alpha1.SandboxClassMicroVM)},
		{name: "empty", class: "", want: SandboxClassUnknown},
		{name: "unknown runtime", class: "kvm", want: SandboxClassUnknown},
		{name: "casing is not normalized away", class: "GVISOR", want: SandboxClassUnknown},
		{name: "attacker-controlled manifest value", class: "gvisor\";evil=\"1", want: SandboxClassUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeSandboxClass(tt.class); got != tt.want {
				t.Errorf("NormalizeSandboxClass(%q) = %q, want %q", tt.class, got, tt.want)
			}
		})
	}
}

// TestFailureReason pins the error-to-label mapping: only the registered
// ateerrors taxonomy may reach the label, so anything unclassified collapses
// onto UNKNOWN instead of leaking an unbounded error message.
func TestFailureReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "wrapped reason",
			err:  fmt.Errorf("%w: while uploading external snapshot", ateerrors.ReasonFaileSaveSnapshot),
			want: string(ateerrors.ReasonFaileSaveSnapshot),
		},
		{
			name: "reason nested several wraps deep",
			err:  fmt.Errorf("restore: %w", fmt.Errorf("%w: bad manifest", ateerrors.ReasonInvalidSandboxAsset)),
			want: string(ateerrors.ReasonInvalidSandboxAsset),
		},
		{
			name: "gRPC status carrying the reason as an ErrorInfo detail",
			err:  ateerrors.NewGRPCError(context.Background(), codes.DataLoss, ateerrors.ReasonTerminalFileSystemError, nil, errors.New("no space left on device")),
			want: string(ateerrors.ReasonTerminalFileSystemError),
		},
		{
			name: "infrastructure error with no reason attached",
			err:  errors.New("dial tcp 10.96.192.187:9000: connect: connection refused"),
			want: ReasonUnknown,
		},
		{
			name: "plain gRPC status with no ErrorInfo",
			err:  status.Error(codes.Unavailable, "unavailable"),
			want: ReasonUnknown,
		},
		{
			name: "nil error",
			err:  nil,
			want: ReasonUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FailureReason(tt.err)
			if got != tt.want {
				t.Errorf("FailureReason() = %q, want %q", got, tt.want)
			}
			if !ateerrors.IsValidReason(got) {
				t.Errorf("FailureReason() = %q, which is not a registered reason", got)
			}
		})
	}
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
