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

// Package ateattr projects an Actor onto substrate's ate.* span attributes.
// Identity is a span-level subject attribute (the producer is the substrate
// component, the actor is the subject), so it belongs on spans rather than the
// resource, and uses substrate's own ate.* namespace rather than service.*.
package ateattr

import (
	"go.opentelemetry.io/otel/attribute"

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

// ActorRefAttributes returns the subset knowable before the Actor record
// resolves: only the (atespace, name) the request addresses. The uid and version
// are server-assigned and unknown until the record loads, so they are omitted.
func ActorRefAttributes(atespace, name string) []attribute.KeyValue {
	return []attribute.KeyValue{
		AtespaceKey.String(atespace),
		ActorNameKey.String(name),
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
