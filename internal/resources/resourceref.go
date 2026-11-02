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
	"log/slog"
	"reflect"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// Resource is any Atespaced resource message carrying the common metadata.
type Resource interface {
	GetMetadata() *ateapi.ResourceMetadata
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
func (r ResourceRef[R]) ToObjectRef() *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Atespace: r.Atespace, Name: r.Name}
}

// resourceRefFromObjectRef converts a wire reference to the in-process form.
func resourceRefFromObjectRef[R Resource](ref *ateapi.ObjectRef) ResourceRef[R] {
	return ResourceRef[R]{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}

// ActorRef identifies an actor by the (atespace, name).
type ActorRef = ResourceRef[*ateapi.Actor]

// ActorRefFromObjectRef converts a wire reference to an ActorRef.
func ActorRefFromObjectRef(ref *ateapi.ObjectRef) ActorRef {
	return resourceRefFromObjectRef[*ateapi.Actor](ref)
}

// ActorRefFromActor returns the reference addressing the given actor.
func ActorRefFromActor(a *ateapi.Actor) ActorRef {
	return ActorRef{
		Atespace: a.GetMetadata().GetAtespace(),
		Name:     a.GetMetadata().GetName(),
	}
}

// ActorTemplateRef identifies an ActorTemplate by the (atespace, name).
type ActorTemplateRef = ResourceRef[*ateapi.ActorTemplate]

// ActorTemplateRefFromObjectRef converts a wire reference to an ActorTemplateRef.
func ActorTemplateRefFromObjectRef(ref *ateapi.ObjectRef) ActorTemplateRef {
	return resourceRefFromObjectRef[*ateapi.ActorTemplate](ref)
}

// ActorTemplateRefFromActorTemplate returns the reference addressing the given
// template.
func ActorTemplateRefFromActorTemplate(t *ateapi.ActorTemplate) ActorTemplateRef {
	return ActorTemplateRef{
		Atespace: t.GetMetadata().GetAtespace(),
		Name:     t.GetMetadata().GetName(),
	}
}

// TagRef identifies a Tag by the (atespace, name).
type TagRef = ResourceRef[*ateapi.Tag]

// TagRefFromObjectRef converts an ObjectRef to a TagRef.
func TagRefFromObjectRef(ref *ateapi.ObjectRef) TagRef {
	return resourceRefFromObjectRef[*ateapi.Tag](ref)
}

// TagRefFromTag returns the reference addressing the given tag.
func TagRefFromTag(t *ateapi.Tag) TagRef {
	return TagRef{
		Atespace: t.GetMetadata().GetAtespace(),
		Name:     t.GetMetadata().GetName(),
	}
}
