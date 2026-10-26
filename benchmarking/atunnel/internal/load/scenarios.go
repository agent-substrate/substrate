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
	"time"
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
	// fill is how long a scenario that holds connections waits, once they
	// are open, before reading atunnel's live memory, so that queues behind
	// slow peers are full.
	fill time.Duration
}

// slowDrain is what a slow-peer scenario's stalled reader reads at full speed
// before it stalls, so the kernel has grown its queues.
const slowDrain = 4 << 20

// slowFill is the fill of the slow-peer scenarios.
const slowFill = 3 * time.Second

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
	{
		Name:        "EgressTCPDownload",
		Description: "4 connections read as fast as they can from a gateway writing without end through egress; one rung",
		Unit:        "read",
		mode:        EgressStreamDown,
		streams:     4,
	},
	{
		Name:        "ConnectTCP",
		Description: "4 CONNECT tunnels read as fast as they can from actors writing without end; one rung",
		Unit:        "read",
		mode:        ConnectStream,
		streams:     4,
	},
	{
		Name:        "ConnectTCPUpload",
		Description: "4 CONNECT tunnels write 64 KiB chunks as fast as they can to actors that discard them; one rung",
		Unit:        "write",
		mode:        ConnectStreamUp,
		streams:     4,
	},
}

// SlowScenarios hold connections whose far end reads, or the near end
// reads, nothing, so bytes back up through atunnel. Rungs are connection
// counts. They run only when named, or all of them with "slow".
var SlowScenarios = []Scenario{
	{
		Name:             "EgressSlowReader",
		Description:      fmt.Sprintf("rungs are connection counts: open at %d/s through egress to a gateway that writes into each without end; the actor reads one byte, then nothing", idleOpenRate),
		Unit:             "held connection",
		mode:             EgressSlowRead,
		holdsConnections: true,
		fill:             slowFill,
	},
	{
		Name:             "EgressSlowUpload",
		Description:      fmt.Sprintf("rungs are connection counts: open at %d/s through egress; the actor writes into each without end and the gateway reads nothing", idleOpenRate),
		Unit:             "held connection",
		mode:             EgressSlowWrite,
		holdsConnections: true,
		fill:             slowFill,
	},
	{
		Name:             "ConnectSlowReader",
		Description:      fmt.Sprintf("rungs are connection counts: open CONNECT tunnels at %d/s to actors that write into each without end; the router side reads one byte, then nothing", idleOpenRate),
		Unit:             "held connection",
		mode:             ConnectSlowRead,
		holdsConnections: true,
		fill:             slowFill,
	},
	{
		Name:             "ConnectSlowUpload",
		Description:      fmt.Sprintf("rungs are connection counts: open CONNECT tunnels at %d/s; the router side writes into each without end and the actor reads nothing", idleOpenRate),
		Unit:             "held connection",
		mode:             ConnectSlowWrite,
		holdsConnections: true,
		fill:             slowFill,
	},
}

// Select returns the scenarios named in a comma-separated list, all of the
// regular ones for "all", or all slow-peer ones for "slow".
func Select(list string) ([]Scenario, error) {
	if list == "" || list == "all" {
		return Scenarios, nil
	}
	var out []Scenario
	for _, name := range strings.Split(list, ",") {
		name = strings.TrimSpace(name)
		if strings.EqualFold(name, "slow") {
			out = append(out, SlowScenarios...)
			continue
		}
		found := false
		for _, s := range append(Scenarios[:len(Scenarios):len(Scenarios)], SlowScenarios...) {
			if strings.EqualFold(s.Name, name) {
				out = append(out, s)
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown scenario %q; known: %s, and slow", name, Names())
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
