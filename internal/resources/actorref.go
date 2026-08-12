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
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Phantom ResourceRef marker keeping actor references a distinct type.
type actorKind struct{}

// ActorRef identifies an actor by the (atespace, name).
type ActorRef = ResourceRef[actorKind]

// ActorRefFromObjectRef converts a wire reference to an ActorRef.
func ActorRefFromObjectRef(ref *ateapipb.ObjectRef) ActorRef {
	return resourceRefFromObjectRef[actorKind](ref)
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
