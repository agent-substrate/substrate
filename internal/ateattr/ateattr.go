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
	"go.opentelemetry.io/otel/attribute"

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
// WorkerStateKey stays worker-rooted rather than nesting under the pool so it
// can grow siblings.
// WorkerPoolNamespaceKey pairs with WorkerPoolNameKey: a WorkerPool is
// namespaced, so the name alone does not identify one.
const (
	WorkerPoolNamespaceKey = attribute.Key("ate.workerpool.namespace")
	WorkerPoolNameKey      = attribute.Key("ate.workerpool.name")
	WorkerStateKey         = attribute.Key("ate.worker.state")
	SandboxClassKey        = attribute.Key("ate.sandbox.class")
	OperationNameKey       = attribute.Key("operation.name")
	FailureReasonKey       = attribute.Key("ate.failure.reason")
	ReasonKey              = FailureReasonKey
)

// Control-plane failure reasons for ate.actor.crashes metric.
const (
	ReasonCorruptedAssignment = "corrupted_assignment"
	ReasonWorkerReassigned    = "worker_reassigned"
	ReasonWorkerPodGone       = "worker_pod_gone"
	ReasonUnknown             = "unknown"
)


// Values for WorkerStateKey. Only idle and assigned are representable today;
// starting and unhealthy workers are not modeled in the cache.
const (
	WorkerStateIdle     = "idle"
	WorkerStateAssigned = "assigned"
)

// Values for OperationNameKey representing actor lifecycle operations.
const (
	OperationNameResume  = "resume"
	OperationNameSuspend = "suspend"
	OperationNamePause   = "pause"
	OperationNameUnknown = "unknown"
)

// NormalizeOperationName ensures op is one of the bounded lifecycle operations
// {resume, suspend, pause, unknown}. Any other value maps to OperationNameUnknown.
func NormalizeOperationName(op string) string {
	switch op {
	case OperationNameResume, OperationNameSuspend, OperationNamePause:
		return op
	default:
		return OperationNameUnknown
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

	return []attribute.KeyValue{
		TemplateNamespaceKey.String(a.GetActorTemplateNamespace()),
		TemplateNameKey.String(a.GetActorTemplateName()),
		WorkerPoolNameKey.String(a.GetWorkerPoolName()),
		SandboxClassKey.String(sandboxClass),
		OperationNameKey.String(operationName),
		FailureReasonKey.String(reason),
	}
}
