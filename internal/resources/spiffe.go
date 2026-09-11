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
	"net/url"
	"path"
	"strings"
)

// ActorSPIFFETrustDomain is the trust domain of the SPIFFE ID an actor's
// certificate carries as its URI SAN.
const ActorSPIFFETrustDomain = "substrate-actor.local"

// ActorSPIFFEID returns "spiffe://substrate-actor.local/atespace/<atespace>/actor/<name>",
// which ateapi mints into the actor certificate's URI SAN.
func ActorSPIFFEID(r ActorRef) *url.URL {
	return &url.URL{
		Scheme: "spiffe",
		Host:   ActorSPIFFETrustDomain,
		Path:   path.Join("atespace", r.Atespace, "actor", r.Name),
	}
}

// ActorRefFromSPIFFEID parses an ID built by ActorSPIFFEID. Anything else is an
// error, so a URI SAN that merely resembles an actor ID never resolves to one.
func ActorRefFromSPIFFEID(id string) (ActorRef, error) {
	u, err := url.Parse(id)
	if err != nil {
		return ActorRef{}, fmt.Errorf("invalid actor SPIFFE ID %q: %w", id, err)
	}
	if u.Scheme != "spiffe" || u.Host != ActorSPIFFETrustDomain || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ActorRef{}, fmt.Errorf("invalid actor SPIFFE ID %q: must be spiffe://%s/atespace/<atespace>/actor/<name>", id, ActorSPIFFETrustDomain)
	}
	segments := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(segments) != 4 || segments[0] != "atespace" || segments[2] != "actor" {
		return ActorRef{}, fmt.Errorf("invalid actor SPIFFE ID %q: must be spiffe://%s/atespace/<atespace>/actor/<name>", id, ActorSPIFFETrustDomain)
	}
	atespace, name := segments[1], segments[3]
	if !IsValidResourceName(atespace) {
		return ActorRef{}, fmt.Errorf("invalid actor SPIFFE ID %q: %q is not a valid atespace", id, atespace)
	}
	if !IsValidResourceName(name) {
		return ActorRef{}, fmt.Errorf("invalid actor SPIFFE ID %q: %q is not a valid actor name", id, name)
	}
	return ActorRef{Atespace: atespace, Name: name}, nil
}
