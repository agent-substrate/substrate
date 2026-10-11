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

package dns

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"time"
)

const (
	// dnsPort is the relay port on the sandbox's gateway.
	dnsPort = 53

	// dnsExchangeTimeout bounds each upstream attempt.
	dnsExchangeTimeout = 5 * time.Second
)

// Relay validates and forwards UDP and TCP DNS queries to the worker pod's
// resolvers. It listens in the sandbox's gateway namespace and dials from the
// worker's. DNS bypasses the egress tunnel and is not checked against egress
// policy.
type Relay struct {
	upstreams []string

	// dialer reaches upstream resolvers from the worker namespace.
	dialer *net.Dialer
}

// NewRelay reads nameservers from resolvConfPath and forwards to each on port 53.
func NewRelay(resolvConfPath string) (*Relay, error) {
	upstreams, err := resolvConfNameservers(resolvConfPath)
	if err != nil {
		return nil, err
	}
	return NewRelayForUpstreams(upstreams)
}

func normalizeAddr(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	switch a := addr.(type) {
	case *net.UDPAddr:
		ap := a.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()).String()
	case *net.TCPAddr:
		ap := a.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()).String()
	default:
		return normalizeAddrString(addr.String())
	}
}

func normalizeAddrString(s string) string {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()).String()
	}
	return s
}

// NewRelayForUpstreams forwards to upstreams, each "host:port", tried in order.
func NewRelayForUpstreams(upstreams []string) (*Relay, error) {
	if len(upstreams) == 0 {
		return nil, fmt.Errorf("dns: at least one upstream resolver is required")
	}
	for _, u := range upstreams {
		if _, _, err := net.SplitHostPort(u); err != nil {
			return nil, fmt.Errorf("dns: invalid upstream resolver %q: %w", u, err)
		}
		if _, err := net.ResolveUDPAddr("udp", u); err != nil {
			return nil, fmt.Errorf("dns: invalid upstream resolver %q: %w", u, err)
		}
	}

	slog.Info("DNS relay configured", slog.Any("upstreams", upstreams))

	return &Relay{
		upstreams: upstreams,
		dialer:    &net.Dialer{Timeout: dnsExchangeTimeout},
	}, nil
}

// serveOn serves DNS on netC's sockets, which the caller has already bound,
// and dials upstreams with netC's dialer.
func (r *Relay) serveOn(ctx context.Context, netC *netConn) (*Server, error) {
	return newServer(ctx, &serverConfig{upstreams: r.upstreams}, netC)
}
