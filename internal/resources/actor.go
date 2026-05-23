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
)

const (
	// ActorIDRegexPattern is the regular expression pattern for matching valid actor IDs.
	ActorIDRegexPattern = `[a-z0-9]([-a-z0-9]*[a-z0-9])?`
	// ActorDNSSuffix is suffix to the DNS name for direct access to Actor
	// "<actor id>.actors.resources.substrate.ate.dev."
	ActorDNSSuffix = "actors.resources.substrate.ate.dev"
)

var actorIDRegex = regexp.MustCompile("^" + ActorIDRegexPattern + "$")

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
