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

package inner

import (
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
)

// actorFilterStateName is the filter-state object the gateway publishes the
// calling actor under.
const actorFilterStateName = "ate.actor.identity"

// actorAttribute is the CEL expression the gateway's ext_proc filter has to
// list under request_attributes.
const actorAttribute = "filter_state['" + actorFilterStateName + "']"

// actorFromAttributes returns the actor the gateway asserted in filter state,
// or "" when it asserted none.
func actorFromAttributes(attr map[string]*structpb.Struct) string {
	for _, attrs := range attr {
		fields := attrs.GetFields()
		if v, ok := fields[actorAttribute]; ok {
			return v.GetStringValue()
		}
		if v, ok := fields[actorFilterStateName]; ok {
			return v.GetStringValue()
		}
	}
	return ""
}

// actorURIPrefix is everything before the atespace in an actor's SPIFFE ID.
// The trust domain is part of it on purpose: it is what separates an actor
// certificate from every other identity in the cluster, so matching without it
// would accept a name-shaped path under some other authority.
const actorURIPrefix = "spiffe://substrate-actor.local/atespace/"

// actorSeparator divides the atespace from the actor name.
const actorSeparator = "/actor/"

// parseActor splits an actor's SPIFFE ID into its atespace and name, returning
// "", "" for anything that is not exactly one.
func parseActor(uri string) (atespace, name string) {
	rest, ok := strings.CutPrefix(uri, actorURIPrefix)
	if !ok {
		return "", ""
	}
	atespace, name, ok = strings.Cut(rest, actorSeparator)
	if !ok || atespace == "" || name == "" {
		return "", ""
	}
	// Kubernetes object names cannot contain a slash, so anything further down
	// the path is a shape this does not know how to read.
	if strings.Contains(atespace, "/") || strings.Contains(name, "/") {
		return "", ""
	}
	return atespace, name
}
