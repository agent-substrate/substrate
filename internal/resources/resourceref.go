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

// ResourceRef identifies an Atespaced resource by the (atespace, name). It is
// the in-process form of the identity that ateapipb.ObjectRef carries on the
// wire.
//
// The kind parameter is a phantom marker: it never affects the value, but it
// keeps references to different resource kinds distinct at compile time, so a
// reference to one kind cannot be passed where another is expected. Each kind
// declares an alias of its instantiation (e.g. ActorTemplateRef) and shares
// this one implementation.
type ResourceRef[kind any] struct {
	// Atespace is the isolation boundary the resource was created into. Required.
	Atespace string
	// Name is the resource's name, unique within Atespace. Required.
	Name string
}

func (r ResourceRef[kind]) String() string {
	return r.Atespace + "/" + r.Name
}

// LogValue implements slog.LogValuer so that slog.Any("template", ref) records
// the two components as a group ("template.atespace", "template.name") rather
// than flattening them into one opaque string.
func (r ResourceRef[kind]) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("atespace", r.Atespace),
		slog.String("name", r.Name),
	)
}

// ToObjectRef converts the reference to its wire form.
func (r ResourceRef[kind]) ToObjectRef() *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: r.Atespace, Name: r.Name}
}

// resourceRefFromObjectRef converts a wire reference to the in-process form.
func resourceRefFromObjectRef[kind any](ref *ateapipb.ObjectRef) ResourceRef[kind] {
	return ResourceRef[kind]{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}
