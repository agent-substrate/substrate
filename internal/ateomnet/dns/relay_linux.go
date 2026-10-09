//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dns

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
)

// Serve serves UDP and TCP DNS in the sandbox's local gateway namespace.
func (r *Relay) Serve(ctx context.Context, ns netns.Handle) (*Server, error) {
	// Bind the wildcard because the microVM tap's gateway address is added later.
	address := net.JoinHostPort("0.0.0.0", strconv.Itoa(dnsPort))

	netC := netConn{
		dialer: *r.dialer,
	}

	if err := netns.Do(ctx, ns, func(context.Context) error {
		pc, err := net.ListenPacket("udp", address)
		if err != nil {
			return fmt.Errorf("while opening the actor DNS socket: %w", err)
		}
		netC.udp = pc
		l, err := net.Listen("tcp", address)
		if err != nil {
			_ = pc.Close()
			return fmt.Errorf("while opening the actor DNS listener: %w", err)
		}
		netC.tcpListener = l
		return nil
	}); err != nil {
		return nil, err
	}

	egressUDP, err := net.ListenPacket("udp", ":0")
	if err != nil {
		_ = netC.udp.Close()
		_ = netC.tcpListener.Close()
		return nil, fmt.Errorf("while opening the worker DNS egress socket: %w", err)
	}
	netC.egressUDP = egressUDP

	return r.serveOn(ctx, &netC)
}
