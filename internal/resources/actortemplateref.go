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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Phantom ResourceRef markers keeping template and version references
// distinct types.
type (
	actorTemplateKind        struct{}
	actorTemplateVersionKind struct{}
)

// ActorTemplateRef identifies an ActorTemplate by the (atespace, name).
type ActorTemplateRef = ResourceRef[actorTemplateKind]

// ActorTemplateVersionRef identifies an ActorTemplateVersion by the
// (atespace, name).
type ActorTemplateVersionRef = ResourceRef[actorTemplateVersionKind]

// ActorTemplateRefFromObjectRef converts a wire reference to an ActorTemplateRef.
func ActorTemplateRefFromObjectRef(ref *ateapipb.ObjectRef) ActorTemplateRef {
	return resourceRefFromObjectRef[actorTemplateKind](ref)
}

// ActorTemplateRefFromActorTemplate returns the reference addressing the given
// template.
func ActorTemplateRefFromActorTemplate(t *ateapipb.ActorTemplate) ActorTemplateRef {
	return ActorTemplateRef{
		Atespace: t.GetMetadata().GetAtespace(),
		Name:     t.GetMetadata().GetName(),
	}
}

// ActorTemplateVersionRefFromObjectRef converts a wire reference to an
// ActorTemplateVersionRef.
func ActorTemplateVersionRefFromObjectRef(ref *ateapipb.ObjectRef) ActorTemplateVersionRef {
	return resourceRefFromObjectRef[actorTemplateVersionKind](ref)
}

// ActorTemplateVersionRefFromActorTemplateVersion returns the reference
// addressing the given version.
func ActorTemplateVersionRefFromActorTemplateVersion(v *ateapipb.ActorTemplateVersion) ActorTemplateVersionRef {
	return ActorTemplateVersionRef{
		Atespace: v.GetMetadata().GetAtespace(),
		Name:     v.GetMetadata().GetName(),
	}
}
