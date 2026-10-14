//go:build linux

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

package ateomnet

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

const maxEgressBytesPerSecond = 1000000000

// egressPolice shares one token bucket across all packets from this Actor.
// The burst is 100ms of traffic, with room for one maximum-size IP packet.
func egressPolice(rate int64) *netlink.PoliceAction {
	police := netlink.NewPoliceAction()
	police.Rate = uint32(rate)
	police.Burst = uint32(max(rate/10, 64*1024))
	police.Mtu = 64 * 1024
	police.ExceedAction = netlink.TC_POLICE_SHOT
	police.NotExceedAction = netlink.TC_POLICE_OK
	return police
}

// ApplyEgressBandwidth installs an ingress policer on the kernel-owned peer:
// ingress there is egress from the Actor. Both runtimes use this boundary so
// gateways, direct traffic, and every container share the same limit.
// The caller creates a fresh interface for every activation.
func ApplyEgressBandwidth(ctx context.Context, ns netns.Handle, name string, rate int64) error {
	if rate != 0 && (rate < 1024 || rate > maxEgressBytesPerSecond) {
		return fmt.Errorf("egress bandwidth must be zero or between 1024 and %d bytes/s", maxEgressBytesPerSecond)
	}
	if rate == 0 {
		return nil
	}
	return netns.Do(ctx, ns, func(context.Context) error {
		link, err := netlink.LinkByName(name)
		if err != nil {
			return err
		}
		if err := netlink.QdiscAdd(&netlink.Clsact{QdiscAttrs: netlink.QdiscAttrs{
			LinkIndex: link.Attrs().Index, Handle: netlink.MakeHandle(0xffff, 0), Parent: netlink.HANDLE_CLSACT,
		}}); err != nil {
			return fmt.Errorf("installing Actor clsact: %w", err)
		}
		filter := &netlink.MatchAll{FilterAttrs: netlink.FilterAttrs{
			LinkIndex: link.Attrs().Index, Parent: netlink.HANDLE_MIN_INGRESS,
			Priority: 1, Protocol: unix.ETH_P_ALL,
		}, Actions: []netlink.Action{egressPolice(rate)}}
		if err := netlink.FilterAdd(filter); err != nil {
			return fmt.Errorf("installing Actor egress policer: %w", err)
		}
		return nil
	})
}

// NetworkStats reports this activation's effective rate and cumulative traffic.
// A failed read is unavailable, rather than a measured zero.
func (s *SandboxSession) NetworkStats(ctx context.Context) (*ateompb.NetworkStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Network == nil {
		return nil, net.ErrClosed
	}
	result := &ateompb.NetworkStats{EgressBytesPerSecond: s.egressBytesPerSecond}
	err := netns.Do(ctx, s.Network.GatewayNetNS, func(context.Context) error {
		link, err := netlink.LinkByName(s.egressLink)
		if err != nil {
			return err
		}
		if stats := link.Attrs().Statistics; stats != nil {
			result.EgressBytes = stats.RxBytes
		} else {
			return errors.New("actor interface statistics unavailable")
		}
		if s.egressBytesPerSecond == 0 {
			return nil
		}
		filters, err := netlink.FilterList(link, netlink.HANDLE_MIN_INGRESS)
		if err != nil {
			return err
		}
		for _, filter := range filters {
			match, ok := filter.(*netlink.MatchAll)
			if !ok || match.Priority != 1 {
				continue
			}
			for _, action := range match.Actions {
				if police, ok := action.(*netlink.PoliceAction); ok {
					stats := police.Attrs().Statistics
					if stats == nil || stats.Queue == nil {
						return errors.New("actor policing statistics unavailable")
					}
					result.EgressDroppedPackets = stats.Queue.Drops
					return nil
				}
			}
		}
		return errors.New("actor egress policer missing")
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
