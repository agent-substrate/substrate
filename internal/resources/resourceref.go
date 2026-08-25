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

package resources

import (
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Resource is any Atespaced resource message carrying the common metadata.
type Resource interface {
	GetMetadata() *ateapipb.ResourceMetadata
}

// ResourceRef identifies an Atespaced resource by the (atespace, name).
type ResourceRef[R Resource] struct {
	// Atespace is the isolation boundary the resource was created into. Required.
	Atespace string
	// Name is the resource's name, unique within Atespace. Required.
	Name string
}

func (r ResourceRef[R]) String() string {
	return r.Atespace + "/" + r.Name
}

// LogValue implements slog.LogValuer so that slog.Any("template", ref) records
// the two components as a group ("template.atespace", "template.name") rather
// than flattening them into one opaque string.
func (r ResourceRef[R]) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("type", reflect.TypeFor[R]().String()),
		slog.String("atespace", r.Atespace),
		slog.String("name", r.Name),
	)
}

// ToObjectRef converts the reference to its wire form.
func (r ResourceRef[R]) ToObjectRef() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: r.Atespace, Name: r.Name}
}

// resourceRefFromObjectRef converts a wire reference to the in-process form.
func resourceRefFromObjectRef[R Resource](ref *ateapipb.ObjectRef) ResourceRef[R] {
	return ResourceRef[R]{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}

// ActorRef identifies an actor by the (atespace, name).
type ActorRef = ResourceRef[*ateapipb.Actor]

// ActorRefFromObjectRef converts a wire reference to an ActorRef.
func ActorRefFromObjectRef(ref *ateapipb.ObjectRef) ActorRef {
	return resourceRefFromObjectRef[*ateapipb.Actor](ref)
}

// ActorRefFromActor returns the reference addressing the given actor.
func ActorRefFromActor(a *ateapipb.Actor) ActorRef {
	return ActorRef{
		Atespace: a.GetMetadata().GetAtespace(),
		Name:     a.GetMetadata().GetName(),
	}
}

// ActorDNSName returns the uniform DNS name the actor is reachable at.
// This is: "<name>.<atespace>.actors.resources.substrate.ate.dev".
func ActorDNSName(r ActorRef) string {
	return r.Name + "." + r.Atespace + "." + ActorDNSSuffix
}

// ParseActorDNSName parses a DNS name for a given actor.
func ParseActorDNSName(name string) (ActorRef, error) {
	rest, found := strings.CutSuffix(strings.TrimSuffix(name, "."), "."+ActorDNSSuffix)
	if !found {
		return ActorRef{}, fmt.Errorf("invalid actor DNS name: must end with %s, got %q", ActorDNSSuffix, name)
	}
	actorName, atespace, found := strings.Cut(rest, ".")
	if !found {
		return ActorRef{}, fmt.Errorf("invalid actor DNS name: expected <actor_name>.<atespace>.%s, got %q", ActorDNSSuffix, name)
	}
	if !IsValidResourceName(actorName) {
		return ActorRef{}, fmt.Errorf("invalid actor DNS name %q: %q is not a valid actor name", name, actorName)
	}
	if !IsValidResourceName(atespace) {
		return ActorRef{}, fmt.Errorf("invalid actor DNS name %q: %q is not a valid atespace", name, atespace)
	}
	return ActorRef{Atespace: atespace, Name: actorName}, nil
}

// ActorTemplateRef identifies an ActorTemplate by the (atespace, name).
type ActorTemplateRef = ResourceRef[*ateapipb.ActorTemplate]

// ActorTemplateRefFromObjectRef converts a wire reference to an ActorTemplateRef.
func ActorTemplateRefFromObjectRef(ref *ateapipb.ObjectRef) ActorTemplateRef {
	return resourceRefFromObjectRef[*ateapipb.ActorTemplate](ref)
}

// ActorTemplateRefFromActorTemplate returns the reference addressing the given
// template.
func ActorTemplateRefFromActorTemplate(t *ateapipb.ActorTemplate) ActorTemplateRef {
	return ActorTemplateRef{
		Atespace: t.GetMetadata().GetAtespace(),
		Name:     t.GetMetadata().GetName(),
	}
}
