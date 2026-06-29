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

package router

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/agent-substrate/substrate/internal/resources"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

type requestMetadata struct {
	headers map[string]string
	path    string
	host    string
}

func newRequestMetadata(headers []*corev3.HeaderValue) *requestMetadata {
	headersMap := make(map[string]string)
	var path string
	var host string

	for _, h := range headers {
		k := strings.ToLower(h.Key)
		val := h.Value
		if val == "" && len(h.RawValue) > 0 {
			val = string(h.RawValue)
		}

		headersMap[k] = val
		if k == ":path" {
			path = val
		}
		if k == ":authority" || k == "host" {
			host = val
		}
	}

	return &requestMetadata{
		headers: headersMap,
		path:    path,
		host:    host,
	}
}

// defaultActorPort is the actor service port used when the host has no
// "<port>-" prefix.
const defaultActorPort = "80"

// portPrefixRegex matches a "<port>-<actorID>" left-most host label. A leading
// numeric run followed by "-" is treated as the target service port on the
// actor. Actor IDs minted for multi-port access must therefore not themselves
// begin with "<digits>-"; IDs without that shape keep routing to the default
// port and remain backwards compatible.
var portPrefixRegex = regexp.MustCompile(`^([0-9]+)-(.+)$`)

// parseActorID extracts the actor ID and target service port from an actor
// host. The default form
//
//	<actor-id>.<ActorDNSSuffix>            -> port 80
//
// while a port-prefixed form selects an explicit service port, letting a single
// actor expose multiple services:
//
//	<port>-<actor-id>.<ActorDNSSuffix>     -> port <port>
//
// Any trailing ":<port>" on the host is the client connection port and is
// stripped before parsing; it does not affect the routed service port.
func parseActorID(host string) (string, string, error) {
	var err error
	if strings.Contains(host, ":") {
		host, _, err = net.SplitHostPort(host)
	}
	if err != nil {
		return "", "", err
	}
	label, found := strings.CutSuffix(strings.TrimSuffix(host, "."), "."+resources.ActorDNSSuffix)
	if !found {
		return "", "", fmt.Errorf("invalid actor_id: must end with %s, got %q", resources.ActorDNSSuffix, host)
	}

	port := defaultActorPort
	actorID := label
	if m := portPrefixRegex.FindStringSubmatch(label); m != nil {
		p, perr := strconv.Atoi(m[1])
		if perr != nil || p < 1 || p > 65535 {
			return "", "", fmt.Errorf("invalid actor port %q in host %q: must be in 1..65535", m[1], host)
		}
		port = strconv.Itoa(p)
		actorID = m[2]
	}

	if err := resources.ValidateActorID(actorID); err != nil {
		return "", "", err
	}

	return actorID, port, nil
}
