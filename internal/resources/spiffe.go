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
//
// TODO(identity): Must be configurable per-install, so that each install can set it to a unique value.
const ActorSPIFFETrustDomain = "substrate-actor.local"

// ActorSPIFFEID returns
// "spiffe://substrate-actor.local/actor/<atespace>/<name>", which ateapi mints
// into the actor certificate's URI SAN.
func ActorSPIFFEID(r ActorRef) *url.URL {
	return &url.URL{
		Scheme: "spiffe",
		Host:   ActorSPIFFETrustDomain,
		Path:   path.Join("actor", r.Atespace, r.Name),
	}
}

// ActorRefFromActorSPIFFEID parses an ID built by ActorSPIFFEID.
func ActorRefFromActorSPIFFEID(id string) (ActorRef, error) {
	u, err := url.Parse(id)
	if err != nil {
		return ActorRef{}, fmt.Errorf("invalid actor SPIFFE ID %q: %w", id, err)
	}
	return ActorRefFromActorSPIFFEURL(u)
}

func ActorRefFromActorSPIFFEURL(u *url.URL) (ActorRef, error) {
	if u.Scheme != "spiffe" || u.Host != ActorSPIFFETrustDomain || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ActorRef{}, fmt.Errorf("%q does not have format spiffe://<trust.domain>/actor/<atespace>/<name>", u.String())
	}
	segments := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(segments) != 3 || segments[0] != "actor" {
		return ActorRef{}, fmt.Errorf("%q does not have format spiffe://<trust.domain>/actor/<atespace>/<name>", u.String())
	}
	atespace, name := segments[1], segments[2]
	if !IsValidResourceName(atespace) {
		return ActorRef{}, fmt.Errorf("%q is not a valid atespace", atespace)
	}
	if !IsValidResourceName(name) {
		return ActorRef{}, fmt.Errorf("%q is not a valid actor name", name)
	}
	return ActorRef{Atespace: atespace, Name: name}, nil
}

// AteomForActorSPIFFEID returns
// "spiffe://substrate-actor.local/ateom-for-actor/<atespace>/<name>", which
// ateapi mints into the actor certificate's URI SAN.
func AteomForActorSPIFFEID(r ActorRef) *url.URL {
	return &url.URL{
		Scheme: "spiffe",
		Host:   ActorSPIFFETrustDomain,
		// TODO(identity): Prefix with "atunnel" to prevent
		// confusion between atunnel and an actor pretending to be
		// an atunnel.
		Path: path.Join("ateom-for-actor", r.Atespace, r.Name),
	}
}

// ActorRefFromAteomForActorSPIFFEID parses an ID built by AteomForActorSPIFFEID.
func ActorRefFromAteomForActorSPIFFEID(id string) (ActorRef, error) {
	u, err := url.Parse(id)
	if err != nil {
		return ActorRef{}, fmt.Errorf("invalid ateom-for-actor SPIFFE ID %q: %w", id, err)
	}
	return ActorRefFromAteomForActorSPIFFEURL(u)
}

func ActorRefFromAteomForActorSPIFFEURL(u *url.URL) (ActorRef, error) {
	if u.Scheme != "spiffe" || u.Host != ActorSPIFFETrustDomain || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ActorRef{}, fmt.Errorf("%q does not have format spiffe://<trust.domain>/ateom-for-actor/<atespace>/<name>", u.String())
	}
	segments := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if len(segments) != 3 || segments[0] != "ateom-for-actor" {
		return ActorRef{}, fmt.Errorf("%q does not have format spiffe://<trust.domain>/ateom-for-actor/<atespace>/<name>", u.String())
	}
	atespace, name := segments[1], segments[2]
	if !IsValidResourceName(atespace) {
		return ActorRef{}, fmt.Errorf("%q is not a valid atespace", atespace)
	}
	if !IsValidResourceName(name) {
		return ActorRef{}, fmt.Errorf("%q is not a valid actor name", name)
	}
	return ActorRef{Atespace: atespace, Name: name}, nil
}
