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

// Package atenet defines the shared contract for Substrate actor networking.
package atenet

import (
	"fmt"
	"strconv"
	"strings"

	"go.ate.dev/substrate/internal/resources"
)

const (
	// TargetActorHeader identifies the actor selected for ingress routing as
	// "<atespace>/<actor>". HTTP field names are case-insensitive; this uses its
	// HTTP/2 wire form so dataplane configuration and metadata are native.
	TargetActorHeader = "ate-target-actor"
)

// ParseTargetActor parses and validates a TargetActorHeader value.
func ParseTargetActor(value string) (resources.ActorRef, error) {
	atespace, actorName, ok := strings.Cut(value, "/")
	if !ok || strings.Contains(actorName, "/") ||
		!resources.IsValidResourceName(atespace) || !resources.IsValidResourceName(actorName) {
		return resources.ActorRef{}, fmt.Errorf("invalid actor reference %q", value)
	}
	return resources.ActorRef{Atespace: atespace, Name: actorName}, nil
}

// ParsePort parses s as a TCP port number, returning ok=false for anything
// outside the valid 1-65535 range (including non-numeric input).
func ParsePort(s string) (port int, ok bool) {
	p, err := strconv.Atoi(s)
	if err != nil || p < 1 || p > 65535 {
		return 0, false
	}
	return p, true
}
