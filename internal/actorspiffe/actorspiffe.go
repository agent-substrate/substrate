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

// Package actorspiffe parses the SPIFFE URI that identifies an actor. The URI is
// minted by cmd/ateapi/internal/actoridentity and carried on the actor's
// CA-signed client certificate; the egress gateway and the credential provider
// both authorize on the atespace and actor name it encodes.
package actorspiffe

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

// TrustDomain is the SPIFFE host an actor identity URI carries, matching the URI
// minted in cmd/ateapi/internal/actoridentity.
const TrustDomain = "substrate-actor.local"

// Parse extracts the atespace and actor name from an actor SPIFFE URI of the
// form spiffe://substrate-actor.local/atespace/<space>/actor/<name>.
func Parse(raw string) (atespace, name string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("parsing actor identity %q: %w", raw, err)
	}
	if u.Scheme != "spiffe" {
		return "", "", fmt.Errorf("actor identity %q: scheme is %q, want spiffe", raw, u.Scheme)
	}
	if u.Host != TrustDomain {
		return "", "", fmt.Errorf("actor identity %q: trust domain is %q, want %q", raw, u.Host, TrustDomain)
	}
	segments := strings.Split(strings.Trim(path.Clean(u.Path), "/"), "/")
	if len(segments) != 4 || segments[0] != "atespace" || segments[2] != "actor" {
		return "", "", fmt.Errorf("actor identity %q: want /atespace/<space>/actor/<name>", raw)
	}
	if segments[1] == "" || segments[3] == "" {
		return "", "", fmt.Errorf("actor identity %q: empty atespace or actor name", raw)
	}
	return segments[1], segments[3], nil
}
