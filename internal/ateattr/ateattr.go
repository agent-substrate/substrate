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
	"github.com/agent-substrate/substrate/internal/resources"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Dotted ate.* matches the metric-instrument naming (atenet.*, atelet.*), not the
// ate.dev/ slash form used for k8s labels and stdout log fields.
// name vs uid mirror the k8s object model that ResourceMetadata follows:
// ate.actor.name is the atespace-scoped addressable name, ate.actor.uid is the
// server-assigned globally-unique key. There is deliberately no ate.actor.id
// (an ambiguous term when both a name and a uid exist).
// atespace and template are their own top-level namespaces (ate.atespace,
// ate.template.*) rather than nested under actor: both are first-class resources
// that also appear in non-actor telemetry, so the keys must mean the same thing
// regardless of what a span is about.
const (
	AtespaceKey          = attribute.Key("ate.atespace")
	ActorNameKey         = attribute.Key("ate.actor.name")
	ActorUIDKey          = attribute.Key("ate.actor.uid")
	TemplateNameKey      = attribute.Key("ate.template.name")
	TemplateNamespaceKey = attribute.Key("ate.template.namespace")
	ActorVersionKey      = attribute.Key("ate.actor.version")
)

// Metric-label keys: the only ate.* attributes allowed on metric datapoints,
// each with a small bounded value set. High-cardinality identity (actor
// name/uid, atespace) is absent by design; it belongs on spans and logs.
// ActorOperationNameKey follows the registry's *.operation.name pattern
// (db.operation.name, gen_ai.operation.name). WorkerStateKey stays worker-rooted
// rather than nesting under the pool so it can grow siblings.
// WorkerPoolNamespaceKey pairs with WorkerPoolNameKey: a WorkerPool is
// namespaced, so the name alone does not identify one.
const (
	ActorOperationNameKey  = attribute.Key("ate.actor.operation.name")
	WorkerPoolNamespaceKey = attribute.Key("ate.workerpool.namespace")
	WorkerPoolNameKey      = attribute.Key("ate.workerpool.name")
	WorkerStateKey         = attribute.Key("ate.worker.state")
	SandboxClassKey        = attribute.Key("ate.sandbox.class")
	SnapshotKindKey        = attribute.Key("ate.snapshot.kind")
	SchedulerOutcomeKey    = attribute.Key("ate.scheduler.outcome")
	RouterResumeKey        = attribute.Key("ate.router.resume")
	RouterOutcomeKey       = attribute.Key("ate.router.outcome")
	FailureReasonKey       = attribute.Key("ate.failure.reason")
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
const (
	SnapshotKindGolden = "golden"
	SnapshotKindLatest = "latest"
	SnapshotKindLocal  = "local"
	SnapshotKindBoot   = "boot"
)

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

// ActorMetricAttributes returns the metric labels for an Actor.
// High-cardinality attributes (atespace, actor name, actor uid) are omitted.
func ActorMetricAttributes(a *ateapipb.Actor, sandboxClass, operationName, reason string) []attribute.KeyValue {
	if a == nil {
		return nil
	}

	// Default values for unknown/unset attributes.
	if reason == "" {
		reason = ReasonUnknown
	}
	operationName = NormalizeOperationName(operationName)

	pool := ""
	if ass := a.GetWorkerAssignment(); ass != nil {
		pool = ass.GetWorkerPool()
	}

	return []attribute.KeyValue{
		TemplateNamespaceKey.String(a.GetActorTemplateNamespace()),
		TemplateNameKey.String(a.GetActorTemplateName()),
		WorkerPoolNameKey.String(pool),
		SandboxClassKey.String(sandboxClass),
		ActorOperationNameKey.String(operationName),
		FailureReasonKey.String(reason),
	}
}
