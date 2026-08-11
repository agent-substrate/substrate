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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ActorTemplateRef identifies an ActorTemplate by the (atespace, name).
//
// ActorTemplateRef is the in-process form of the identity that
// ateapipb.ObjectRef carries on the wire.
type ActorTemplateRef struct {
	// Atespace is the isolation boundary the template was created into. Required.
	Atespace string
	// Name is the template's name, unique within Atespace. Required.
	Name string
}

func (r ActorTemplateRef) String() string {
	return r.Atespace + "/" + r.Name
}

// LogValue implements slog.LogValuer so that slog.Any("template", ref) records
// the two components as a group ("template.atespace", "template.name") rather
// than flattening them into one opaque string.
func (r ActorTemplateRef) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("atespace", r.Atespace),
		slog.String("name", r.Name),
	)
}

// ToObjectRef converts the reference to its wire form.
func (r ActorTemplateRef) ToObjectRef() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: r.Atespace, Name: r.Name}
}

// ActorTemplateRefFromObjectRef converts a wire reference to an ActorTemplateRef.
func ActorTemplateRefFromObjectRef(ref *ateapipb.ObjectRef) ActorTemplateRef {
	return ActorTemplateRef{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}

// ActorTemplateRefFromActorTemplate returns the reference addressing the given
// template.
func ActorTemplateRefFromActorTemplate(t *ateapipb.ActorTemplate) ActorTemplateRef {
	return ActorTemplateRef{
		Atespace: t.GetMetadata().GetAtespace(),
		Name:     t.GetMetadata().GetName(),
	}
}

// ActorTemplateVersionRef identifies an ActorTemplateVersion by the
// (atespace, name).
//
// ActorTemplateVersionRef is the in-process form of the identity that
// ateapipb.ObjectRef carries on the wire.
type ActorTemplateVersionRef struct {
	// Atespace is the isolation boundary the version was created into. Required.
	Atespace string
	// Name is the version's name, unique within Atespace. Required.
	Name string
}

func (r ActorTemplateVersionRef) String() string {
	return r.Atespace + "/" + r.Name
}

// LogValue implements slog.LogValuer so that slog.Any("version", ref) records
// the two components as a group ("version.atespace", "version.name") rather
// than flattening them into one opaque string.
func (r ActorTemplateVersionRef) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("atespace", r.Atespace),
		slog.String("name", r.Name),
	)
}

// ToObjectRef converts the reference to its wire form.
func (r ActorTemplateVersionRef) ToObjectRef() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: r.Atespace, Name: r.Name}
}

// ActorTemplateVersionRefFromObjectRef converts a wire reference to an
// ActorTemplateVersionRef.
func ActorTemplateVersionRefFromObjectRef(ref *ateapipb.ObjectRef) ActorTemplateVersionRef {
	return ActorTemplateVersionRef{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}

// ActorTemplateVersionRefFromActorTemplateVersion returns the reference
// addressing the given version.
func ActorTemplateVersionRefFromActorTemplateVersion(v *ateapipb.ActorTemplateVersion) ActorTemplateVersionRef {
	return ActorTemplateVersionRef{
		Atespace: v.GetMetadata().GetAtespace(),
		Name:     v.GetMetadata().GetName(),
	}
}
