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

// Package ateattr projects an Actor onto substrate's ate.* identity attributes.
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
const (
	AtespaceKey               = attribute.Key("ate.atespace")
	ActorIDKey                = attribute.Key("ate.actor.id")
	ActorTemplateNameKey      = attribute.Key("ate.actor.template.name")
	ActorTemplateNamespaceKey = attribute.Key("ate.actor.template.namespace")
	ActorVersionKey           = attribute.Key("ate.actor.version")
)

// ActorRefIdentity returns the subset knowable before the Actor record resolves.
func ActorRefIdentity(atespace, actorID string) []attribute.KeyValue {
	return []attribute.KeyValue{
		AtespaceKey.String(atespace),
		ActorIDKey.String(actorID),
	}
}

// ActorIdentity is nil-safe; a nil Actor yields zero-valued attributes.
func ActorIdentity(a *ateapipb.Actor) []attribute.KeyValue {
	return []attribute.KeyValue{
		AtespaceKey.String(a.GetMetadata().GetAtespace()),
		ActorIDKey.String(a.GetMetadata().GetName()),
		ActorTemplateNameKey.String(a.GetActorTemplateName()),
		ActorTemplateNamespaceKey.String(a.GetActorTemplateNamespace()),
		ActorVersionKey.Int64(a.GetMetadata().GetVersion()),
	}
}
