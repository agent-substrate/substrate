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

// Package ateattr is the single source of truth for substrate's ate.* telemetry
// attributes: the identity keys stamped on spans/logs, and the bounded value
// sets used as metric labels. Centralizing them keeps a key (and value) meaning
// the same thing across every signal and binary.
package ateattr

import (
	"slices"

	"go.opentelemetry.io/otel/attribute"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Dotted ate.* matches the metric-instrument naming (atenet.*, atelet.*), not the
// ate.dev/ slash form, which is k8s labels only.
// name vs uid mirror the k8s object model that ResourceMetadata follows:
// ate.actor.name is the atespace-scoped addressable name, ate.actor.uid is the
// server-assigned globally-unique key. There is deliberately no ate.actor.id
// (an ambiguous term when both a name and a uid exist).
// atespace and template are their own top-level namespaces (ate.atespace,
// ate.template.*) rather than nested under actor: both are first-class resources
// that also appear in non-actor telemetry, so the keys must mean the same thing
// regardless of what a span is about. ActorContainerNameKey nests under actor for
// the mirror-image reason: it names a container the ActorTemplate declared, which
// exists only within an actor. It is deliberately not the registry's
// k8s.container.name or container.name, both of which the collector already
// assigns to the worker pod's own containers.
const (
	AtespaceKey           = attribute.Key("ate.atespace")
	ActorNameKey          = attribute.Key("ate.actor.name")
	ActorUIDKey           = attribute.Key("ate.actor.uid")
	ActorContainerNameKey = attribute.Key("ate.actor.container.name")
	TemplateNameKey       = attribute.Key("ate.template.name")
	TemplateNamespaceKey  = attribute.Key("ate.template.namespace")
	ActorVersionKey       = attribute.Key("ate.actor.version")
)

// ReservedNamespace is substrate's. A producer that merges untrusted fields into a
// record drops everything under it, so nothing a workload sets can read as
// platform-issued attribution downstream.
const ReservedNamespace = "ate."

// Trace-context fields for structured logs, per the OTel spec for non-OTLP log
// formats: these exact names, top-level in the record, lowercase hex. Not ate.*
// and not attributes - a collector maps them onto the log record's own
// TraceId/SpanId/flags fields.
// https://opentelemetry.io/docs/specs/otel/compatibility/logging_trace_context/
const (
	LogTraceIDField    = "trace_id"
	LogSpanIDField     = "span_id"
	LogTraceFlagsField = "trace_flags"
)

// OTLPRelayKey is a resource attribute rather than a subject one: it describes
// how the emitting component reached the collector, not what the signal is about.
// Only the components that have a relay to take or miss carry it.
const OTLPRelayKey = attribute.Key("ate.otlp.relay")

// Metric-label keys: the only ate.* attributes allowed on metric datapoints,
// each with a small bounded value set. High-cardinality identity (actor
// name/uid, atespace) is absent by design; it belongs on spans and logs.
// ActorOperationNameKey follows the registry's *.operation.name pattern
// (db.operation.name, gen_ai.operation.name). WorkerStateKey stays worker-rooted
// rather than nesting under the pool so it can grow siblings.
// WorkerPoolNamespaceKey pairs with WorkerPoolNameKey: a WorkerPool is
// namespaced, so the name alone does not identify one.
// The snapshot keys are orthogonal: kind is which snapshot, scope is what
// content it covers, and phase is which step of the operation an observation
// timed. Naming one image within a snapshot is the registry's file.name, not an
// ate.* key of its own.
// ImageCacheOutcomeKey is rooted at the subsystem, not under actor: the layer
// pool is node state every actor shares. For the same reason it is the only
// ate.* label on its counter.
const (
	ActorOperationNameKey   = attribute.Key("ate.actor.operation.name")
	WorkerPoolNamespaceKey  = attribute.Key("ate.workerpool.namespace")
	WorkerPoolNameKey       = attribute.Key("ate.workerpool.name")
	WorkerStateKey          = attribute.Key("ate.worker.state")
	SandboxClassKey         = attribute.Key("ate.sandbox.class")
	SnapshotKindKey         = attribute.Key("ate.snapshot.kind")
	SnapshotScopeKey        = attribute.Key("ate.snapshot.scope")
	SnapshotPhaseKey        = attribute.Key("ate.snapshot.phase")
	ImageCacheOutcomeKey    = attribute.Key("ate.imagecache.outcome")
	SchedulerOutcomeKey     = attribute.Key("ate.scheduler.outcome")
	SchedulingConstraintKey = attribute.Key("ate.scheduling.constraint")
	RouterResumeKey         = attribute.Key("ate.router.resume")
	RouterOutcomeKey        = attribute.Key("ate.router.outcome")
	FailureReasonKey        = attribute.Key("ate.failure.reason")
)

// Values for SchedulingConstraintKey.
const (
	ConstraintNone          = "none"
	ConstraintRequiredNodes = "required_nodes"
	ConstraintSelector      = "selector"
)

// Control-plane failure reasons for ate.actor.crashes metric.
const (
	ReasonCorruptedAssignment = string(ateerrors.ReasonCorruptedAssignment)
	ReasonWorkerReassigned    = string(ateerrors.ReasonWorkerReassigned)
	ReasonWorkerPodGone       = string(ateerrors.ReasonWorkerPodGone)
	ReasonUnknown             = string(ateerrors.ReasonUnknown)
)

// Values for RouterResumeKey.
const (
	// RouterResumeNone indicates the actor was already running (steady-state route).
	RouterResumeNone = "none"
	// RouterResumeTriggered indicates this request won the singleflight lock and initiated cold activation.
	RouterResumeTriggered = "triggered"
	// RouterResumeJoined indicates this request parked on an in-flight singleflight resume.
	RouterResumeJoined = "joined"
)

// Values for ImageCacheOutcomeKey. A hit is a complete image record; a miss
// must pull. A failed lookup is neither: Error is the only one that carries an
// error.type, Cancelled and Timeout mean the caller gave up.
const (
	ImageCacheOutcomeHit       = "hit"
	ImageCacheOutcomeMiss      = "miss"
	ImageCacheOutcomeError     = "error"
	ImageCacheOutcomeCancelled = "cancelled"
	ImageCacheOutcomeTimeout   = "timeout"
)

// ErrorTypeKey is the OTel registry attribute, reused verbatim (not aliased into
// ate.*): failures are reported on the same instrument via this key, its absence
// meaning success, never as a parallel _failures counter.
const ErrorTypeKey = attribute.Key("error.type")

// Values for WorkerStateKey. Only idle and assigned are representable today;
// starting and unhealthy workers are not modeled in the cache.
const (
	WorkerStateIdle     = "idle"
	WorkerStateAssigned = "assigned"
)

// Values for ActorOperationNameKey: the actor lifecycle operations ateapi
// serves.
const (
	OperationCreate  = "create"
	OperationResume  = "resume"
	OperationSuspend = "suspend"
	OperationPause   = "pause"
	OperationDelete  = "delete"
	OperationUnknown = "unknown"
)

// AllOperations lists all registered bounded actor lifecycle operations.
var AllOperations = []string{
	OperationCreate,
	OperationResume,
	OperationSuspend,
	OperationPause,
	OperationDelete,
}

// NormalizeOperationName ensures op is one of the bounded lifecycle operations.
// Any unlisted or empty operation maps to OperationUnknown.
func NormalizeOperationName(op string) string {
	if slices.Contains(AllOperations, op) {
		return op
	}
	return OperationUnknown
}

// Values for SchedulerOutcomeKey. NoFreeWorker is a capacity signal, not a
// failure, so it is a distinct outcome rather than an error.type value; only the
// Error outcome carries an error.type.
const (
	SchedulerOutcomeAssigned     = "assigned"
	SchedulerOutcomeNoFreeWorker = "no_free_worker"
	SchedulerOutcomeError        = "error"
)

// Values for SnapshotKindKey, set by ateapi from its own resume branching, so
// the label is bounded at the producer: Local restores an in-node snapshot,
// Latest pulls the actor's durable snapshot from object storage, Golden pulls the
// template's golden image, Boot is a from-scratch start (not a restore).
// atelet derives the same values for its own histograms, where the kind is the
// snapshot a restore reads or a checkpoint writes; Boot never appears there.
const (
	SnapshotKindGolden = "golden"
	SnapshotKindLatest = "latest"
	SnapshotKindLocal  = "local"
	SnapshotKindBoot   = "boot"
)

// Values for SnapshotScopeKey, mirroring ateletpb.SnapshotScope. Checkpoints
// only ever capture Full or Data; DataOnGolden is restore-only.
const (
	SnapshotScopeFull         = "full"
	SnapshotScopeData         = "data"
	SnapshotScopeDataOnGolden = "data_on_golden"
	SnapshotScopeUnknown      = "unknown"
)

// SnapshotScopeValue maps the wire enum onto its label value, shared so ateapi
// (which sets the scope) and atelet (which receives it) cannot drift. An
// unrecognized scope reports as unknown rather than stringified, so no wire
// value can widen the label set.
func SnapshotScopeValue(scope ateletpb.SnapshotScope) string {
	switch scope {
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		return SnapshotScopeFull
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		return SnapshotScopeData
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN:
		return SnapshotScopeDataOnGolden
	default:
		return SnapshotScopeUnknown
	}
}

// Values for SnapshotPhaseKey. Phases overlap (the download runs concurrently
// with the asset fetch and OCI unpack), so they are independent observations,
// not a partition of Total: summing across them is meaningless.
const (
	SnapshotPhaseVolumeMount     = "volume_mount"
	SnapshotPhaseManifestFetch   = "manifest_fetch"
	SnapshotPhaseSandboxAssets   = "sandbox_assets"
	SnapshotPhaseDownload        = "download"
	SnapshotPhaseOCIUnpack       = "oci_unpack"
	SnapshotPhaseAteomRestore    = "ateom_restore"
	SnapshotPhaseAteomCheckpoint = "ateom_checkpoint"
	// Persist is one step with two destinations (upload for external, rename
	// for local); SnapshotKindKey already says which.
	SnapshotPhasePersist = "persist"
	SnapshotPhaseTotal   = "total"
)

// FailureReason classifies err onto the bounded ateerrors taxonomy, reading the
// wrapped Reason or the AIP-193 ErrorInfo detail. An error carrying neither
// reports ReasonUnknown rather than anything derived from its message, which is
// what keeps the label bounded.
func FailureReason(err error) string {
	if r := ateerrors.ExtractReason(err); r != "" {
		return r
	}
	return ReasonUnknown
}

// SandboxClassUnknown is the NormalizeSandboxClass fallback.
const SandboxClassUnknown = "unknown"

// NormalizeSandboxClass bounds the label: atelet reads the class from a
// snapshot manifest in object storage that nothing validates on the way in.
// Empty reports as unknown rather than the gvisor default, so a manifest
// problem stays visible.
func NormalizeSandboxClass(class string) string {
	switch atev1alpha1.SandboxClass(class) {
	case atev1alpha1.SandboxClassGvisor, atev1alpha1.SandboxClassMicroVM:
		return class
	default:
		return SandboxClassUnknown
	}
}

// WorkerPoolAttributes returns the namespaced identity of a WorkerPool. A
// WorkerPool is namespaced, so half the pair identifies no pool: either key
// missing drops both, rather than emit an empty-string series that merges
// same-named pools and joins to nothing.
//
// This is for the actor-centric instruments, where an unknown pool is omitted.
// Pool-centric ones that record a deliberate zero-valued series for "no pool
// matched" build the pair themselves.
func WorkerPoolAttributes(namespace, name string) []attribute.KeyValue {
	if name == "" || namespace == "" {
		return nil
	}
	return []attribute.KeyValue{
		WorkerPoolNamespaceKey.String(namespace),
		WorkerPoolNameKey.String(name),
	}
}

// ActorRefAttributes returns the subset knowable before the Actor record
// resolves: only the (atespace, name) the request addresses. The uid and version
// are server-assigned and unknown until the record loads, so they are omitted.
func ActorRefAttributes(actorRef resources.ActorRef) []attribute.KeyValue {
	return []attribute.KeyValue{
		AtespaceKey.String(actorRef.Atespace),
		ActorNameKey.String(actorRef.Name),
	}
}

// ActorAttributes is nil-safe; a nil Actor yields zero-valued attributes.
func ActorAttributes(a *ateapipb.Actor) []attribute.KeyValue {
	return []attribute.KeyValue{
		AtespaceKey.String(a.GetMetadata().GetAtespace()),
		ActorNameKey.String(a.GetMetadata().GetName()),
		ActorUIDKey.String(a.GetMetadata().GetUid()),
		TemplateNameKey.String(a.GetActorTemplateName()),
		TemplateNamespaceKey.String(a.GetActorTemplateNamespace()),
		ActorVersionKey.Int64(a.GetMetadata().GetVersion()),
	}
}

// ActorLogLabels returns the actor identity stamped on every actor log record. A
// string map because GKE promotes the record's label group into LogEntry.labels,
// which is string-valued. An empty containerName omits the key rather than
// emitting it empty, so a consumer filtering on it gets container output only.
func ActorLogLabels(a resources.ActorAttribution, containerName string) map[string]string {
	labels := map[string]string{
		string(AtespaceKey):          a.Ref.Atespace,
		string(ActorNameKey):         a.Ref.Name,
		string(ActorUIDKey):          a.UID,
		string(TemplateNamespaceKey): a.TemplateNamespace,
		string(TemplateNameKey):      a.TemplateName,
	}
	if containerName != "" {
		labels[string(ActorContainerNameKey)] = containerName
	}
	return labels
}

// ActorMetricAttributes returns the metric labels for an Actor.
// High-cardinality attributes (atespace, actor name, actor uid) are omitted.
// The worker-pool pair is omitted while the actor holds no assignment, so a
// crash before the actor reaches a worker reports no pool rather than an
// empty-string one.
func ActorMetricAttributes(a *ateapipb.Actor, sandboxClass, operationName, reason string) []attribute.KeyValue {
	if a == nil {
		return nil
	}

	// Default values for unknown/unset attributes.
	if reason == "" {
		reason = ReasonUnknown
	}
	operationName = NormalizeOperationName(operationName)

	ass := a.GetStatus().GetWorkerAssignment()
	attrs := []attribute.KeyValue{
		TemplateNamespaceKey.String(a.GetActorTemplateNamespace()),
		TemplateNameKey.String(a.GetActorTemplateName()),
		SandboxClassKey.String(sandboxClass),
		ActorOperationNameKey.String(operationName),
		FailureReasonKey.String(reason),
	}
	return append(attrs, WorkerPoolAttributes(ass.GetWorkerNamespace(), ass.GetWorkerPool())...)
}
