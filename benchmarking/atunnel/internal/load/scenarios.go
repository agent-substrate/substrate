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

// Package load is the benchmark's load generator: an open-loop pacer, the
// traffic modes that exercise each atunnel path, the scenarios built from
// them, and the ladder that runs a scenario at rising rates while reading
// atunnel's own CPU and memory from its admin endpoint.
package load

import (
	"fmt"
	"strings"
)

// Scenario is a mode and how to drive it.
type Scenario struct {
	Name        string
	Description string
	// Unit names one unit of work, for reading the report.
	Unit string
	mode Mode
	// newConnectionPerRequest turns keep-alive off.
	newConnectionPerRequest bool
	// holdsConnections makes each rung a connection count to open, at
	// idleOpenRate per second, and hold.
	holdsConnections bool
	// streams makes the scenario a bandwidth measurement over this many
	// connections, run once at whatever the first rung is.
	streams int
}

// idleOpenRate is how many held connections per second EgressIdleConns opens.
// Each costs atunnel a TCP accept and a CONNECT stream to the gateway.
const idleOpenRate = 200

// Scenarios are every scenario, in the order they run.
var Scenarios = []Scenario{
	{
		Name:        "IngressH1",
		Description: "router-style HTTPS requests over HTTP/1.1 keep-alive into ingress, proxied to the actor",
		Unit:        "request",
		mode:        IngressH1,
	},
	{
		Name:        "IngressH2",
		Description: "IngressH1 over HTTP/2, many requests multiplexed on few connections",
		Unit:        "request",
		mode:        IngressH2,
	},
	{
		Name:        "Connect",
		Description: "per request: new TLS connection, CONNECT to the actor's port, one request inside",
		Unit:        "tunnel",
		mode:        Connect,
	},
	{
		Name:        "EgressHTTP",
		Description: "actor-style plain HTTP keep-alive out through egress; one CONNECT stream to the gateway per connection",
		Unit:        "request",
		mode:        EgressHTTP,
	},
	{
		Name:                    "EgressNewConnection",
		Description:             "EgressHTTP with a new connection per request, so a new CONNECT stream per request",
		Unit:                    "request",
		mode:                    EgressHTTP,
		newConnectionPerRequest: true,
	},
	{
		Name:             "EgressIdleConns",
		Description:      fmt.Sprintf("rungs are connection counts: open at %d/s through egress, one request each, then hold", idleOpenRate),
		Unit:             "held connection",
		mode:             EgressIdle,
		holdsConnections: true,
	},
	{
		Name:        "EgressTCP",
		Description: "4 connections write 64 KiB chunks as fast as they can through egress to a discarding gateway; one rung",
		Unit:        "write",
		mode:        EgressStream,
		streams:     4,
	},
}

// Select returns the scenarios named in a comma-separated list, or all of
// them for "all".
func Select(list string) ([]Scenario, error) {
	if list == "" || list == "all" {
		return Scenarios, nil
	}
	var out []Scenario
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		found := false
		for _, s := range Scenarios {
			if strings.EqualFold(s.Name, name) {
				out = append(out, s)
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown scenario %q; known: %s", name, Names())
		}
	}
	return out, nil
}

// Names lists the scenario names.
func Names() string {
	var names []string
	for _, s := range Scenarios {
		names = append(names, s.Name)
	}
	return strings.Join(names, ",")
}
