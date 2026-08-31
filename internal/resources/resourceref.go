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

// lowerASCII folds A-Z and leaves every other byte alone. Resource names are
// DNS-1123 labels, so ASCII is the whole alphabet here, and strings.ToLower
// would additionally fold characters outside it onto ASCII letters (the Kelvin
// sign U+212A onto "k", U+017F onto "s"), letting a non-ASCII host reach an
// actor under a spelling that is not its name.
func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// ParseActorDNSName parses a DNS name for a given actor.
//
// The name is folded to lower case first. DNS lookups are case-insensitive
// (RFC 4343), so a client that resolved "MyActor.MySpace.<suffix>" reaches us
// with that spelling preserved in the Host header, while actor and atespace
// names are always lower case. Folding keeps the request addressed to the same
// actor its DNS lookup resolved to instead of failing to parse.
func ParseActorDNSName(name string) (ActorRef, error) {
	normalized := lowerASCII(strings.TrimSuffix(name, "."))
	rest, found := strings.CutSuffix(normalized, "."+ActorDNSSuffix)
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

// ActorSnapshotRef identifies an ActorSnapshot by the (atespace, name).
type ActorSnapshotRef = ResourceRef[*ateapipb.ActorSnapshot]

// ActorSnapshotRefFromObjectRef converts an ObjectRef to an ActorSnapshotRef.
func ActorSnapshotRefFromObjectRef(ref *ateapipb.ObjectRef) ActorSnapshotRef {
	return resourceRefFromObjectRef[*ateapipb.ActorSnapshot](ref)
}

// ActorSnapshotRefFromActorSnapshot returns the reference addressing the given
// snapshot.
func ActorSnapshotRefFromActorSnapshot(s *ateapipb.ActorSnapshot) ActorSnapshotRef {
	return ActorSnapshotRef{
		Atespace: s.GetMetadata().GetAtespace(),
		Name:     s.GetMetadata().GetName(),
	}
}

// ActorSnapshotTagRef identifies an ActorSnapshotTag by the (atespace, name).
type ActorSnapshotTagRef = ResourceRef[*ateapipb.ActorSnapshotTag]

// ActorSnapshotTagRefFromObjectRef converts an Ibjectref to an ActorSnapshotTagRef.
func ActorSnapshotTagRefFromObjectRef(ref *ateapipb.ObjectRef) ActorSnapshotTagRef {
	return resourceRefFromObjectRef[*ateapipb.ActorSnapshotTag](ref)
}

// ActorSnapshotTagRefFromActorSnapshotTag returns the reference addressing the
// given tag.
func ActorSnapshotTagRefFromActorSnapshotTag(t *ateapipb.ActorSnapshotTag) ActorSnapshotTagRef {
	return ActorSnapshotTagRef{
		Atespace: t.GetMetadata().GetAtespace(),
		Name:     t.GetMetadata().GetName(),
	}
}
