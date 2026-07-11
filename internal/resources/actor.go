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
)

const (
	// ResourceNameRegexPattern is the regular expression pattern for a valid
	// Substrate resource name.
	ResourceNameRegexPattern = `[a-z0-9]([-a-z0-9]*[a-z0-9])?`
	// ActorDNSSuffix is suffix to the DNS name for direct access to Actor
	// "<actor_name>.<atespace>.actors.resources.substrate.ate.dev."
	ActorDNSSuffix = "actors.resources.substrate.ate.dev"
	// GoldenActorAtespace is the reserved system atespace that per-template golden
	// actors live in.
	GoldenActorAtespace = "ate-golden"
)

// ActorDNSName returns the mesh DNS name an actor is reachable at:
// "<actor_name>.<atespace>.actors.resources.substrate.ate.dev". The atespace is
// part of the name because an actor name is only unique within its atespace.
func ActorDNSName(atespace, actorName string) string {
	return actorName + "." + atespace + "." + ActorDNSSuffix
}

// ParseActorDNSName parses a mesh DNS name of the form
// "<actor_name>.<atespace>.actors.resources.substrate.ate.dev" (a trailing dot
// is tolerated) into its atespace and actor name, validating both. It does not
// accept a host:port; callers must strip the port first.
func ParseActorDNSName(name string) (atespace, actorName string, err error) {
	rest, found := strings.CutSuffix(strings.TrimSuffix(name, "."), "."+ActorDNSSuffix)
	if !found {
		return "", "", fmt.Errorf("invalid actor DNS name: must end with %s, got %q", ActorDNSSuffix, name)
	}
	actorName, atespace, found = strings.Cut(rest, ".")
	if !found {
		return "", "", fmt.Errorf("invalid actor DNS name: expected <actor_name>.<atespace>.%s, got %q", ActorDNSSuffix, name)
	}
	if !IsValidResourceName(actorName) {
		return "", "", fmt.Errorf("invalid actor DNS name %q: %q is not a valid actor name", name, actorName)
	}
	if !IsValidResourceName(atespace) {
		return "", "", fmt.Errorf("invalid actor DNS name %q: %q is not a valid atespace", name, atespace)
	}
	return atespace, actorName, nil
}
