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
	"regexp"
	"strings"
)

const (
	// ActorIDRegexPattern is the regular expression pattern for matching valid actor IDs.
	ActorIDRegexPattern = `[a-z0-9]([-a-z0-9]*[a-z0-9])?`
	// ActorDNSSuffix is suffix to the DNS name for direct access to Actor
	// "<actor id>.<atespace>.actors.resources.substrate.ate.dev."
	ActorDNSSuffix = "actors.resources.substrate.ate.dev"
)

var actorIDRegex = regexp.MustCompile("^" + ActorIDRegexPattern + "$")

// TODO: unify actor/atespace validation across the control API RPCs — some only
// reject empty strings (get/pause/resume/suspend), others run the full validator.

// ValidateActorID validates whether the provided actor ID is valid or not.
// Actor IDs must be valid DNS-1123 labels.
//
// 1. Must be between 1 and 63 characters in length.
// 2. Must start with a lower-case alphanumeric character (a-z, 0-9).
// 3. Must contain only lower-case alphanumeric characters and hyphens (a-z, 0-9, -).
// 4. Must end with a lower-case alphanumeric character (cannot end with a hyphen).
func ValidateActorID(id string) error {
	if len(id) > 63 {
		return fmt.Errorf("invalid actor_id: must be no more than 63 characters")
	}
	if !actorIDRegex.MatchString(id) {
		return fmt.Errorf("invalid actor_id: must start and end with a lower case alphanumeric character, and consist only of lower case alphanumeric characters or '-'")
	}
	return nil
}

// ValidateAtespace validates whether the provided atespace name is valid. An
// atespace must be a valid DNS-1123 label (same rules as an actor ID above).
func ValidateAtespace(atespace string) error {
	if len(atespace) > 63 {
		return fmt.Errorf("invalid atespace: must be no more than 63 characters")
	}
	if !actorIDRegex.MatchString(atespace) {
		return fmt.Errorf("invalid atespace: must start and end with a lower case alphanumeric character, and consist only of lower case alphanumeric characters or '-'")
	}
	return nil
}

// ActorDNSName returns the mesh DNS name an actor is reachable at:
// "<actor_id>.<atespace>.actors.resources.substrate.ate.dev". The atespace is
// part of the name because an actor id is only unique within its atespace.
func ActorDNSName(atespace, actorID string) string {
	return actorID + "." + atespace + "." + ActorDNSSuffix
}

// ParseActorDNSName parses a mesh DNS name of the form
// "<actor_id>.<atespace>.actors.resources.substrate.ate.dev" (a trailing dot is
// tolerated) into its atespace and actor id, validating both. It does not accept
// a host:port; callers must strip the port first.
func ParseActorDNSName(name string) (atespace, actorID string, err error) {
	rest, found := strings.CutSuffix(strings.TrimSuffix(name, "."), "."+ActorDNSSuffix)
	if !found {
		return "", "", fmt.Errorf("invalid actor DNS name: must end with %s, got %q", ActorDNSSuffix, name)
	}
	actorID, atespace, found = strings.Cut(rest, ".")
	if !found {
		return "", "", fmt.Errorf("invalid actor DNS name: expected <actor_id>.<atespace>.%s, got %q", ActorDNSSuffix, name)
	}
	if err := ValidateActorID(actorID); err != nil {
		return "", "", err
	}
	if err := ValidateAtespace(atespace); err != nil {
		return "", "", err
	}
	return atespace, actorID, nil
}
