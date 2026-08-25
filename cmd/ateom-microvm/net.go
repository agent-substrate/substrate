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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/ateomnet"
)

const (
	// hostVethMAC is deliberately FIXED (locally administered), unlike
	// ateom-gvisor where the kernel's random veth MAC is fine. A CH snapshot
	// freezes the guest kernel's ARP cache, including the entry for the
	// gateway 169.254.17.1; restoring against a new veth pair with a random
	// MAC would blackhole guest egress until that entry expires. A constant
	// gateway MAC keeps the frozen entry valid on every pod.
	hostVethMAC = "02:a8:1e:00:00:01"

	// actorGuestMAC is the FIXED MAC for the guest's eth0 (the CH virtio-net).
	// Fixed for the same reason as hostVethMAC: a cold boot freezes this MAC into
	// the guest+snapshot, and restore re-adds the
	// virtio-net under the same MAC (SnapshotNetDevices reads it back), so the
	// guest's frozen interface config stays valid across pods. Distinct from the
	// gateway MAC (…:01).
	actorGuestMAC = "02:a8:1e:00:00:02"
)

var (
	hostVethHWAddr = ateomnet.MustParseMAC(hostVethMAC)
)

// setupRestoreTap recreates, in the interior netns, the tap + TC-mirror wiring
// kata's tcfilter network model builds at boot: a tap device cross-connected to
// eth0 (the actor veth peer) with mirred-redirect ingress filters in both
// directions. Returns the open tap FDs (one per queue pair) for
// cloud-hypervisor to adopt via vm.restore net_fds (the snapshot's virtio-net
// device is fd-backed, so CH requires fresh FDs on restore). Call after
// setupActorNetwork.
func (s *AteomService) setupRestoreTap(ctx context.Context, name string, queuePairs int) ([]*os.File, error) {
	var fds []*os.File
	err := ateomnet.NetNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		eth0, err := netlink.LinkByName(ateomnet.ActorVethName)
		if err != nil {
			return fmt.Errorf("acquiring actor veth in interior netns: %w", err)
		}
		if old, lerr := netlink.LinkByName(name); lerr == nil {
			_ = netlink.LinkDel(old)
		}
		flags := netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR
		if queuePairs > 1 {
			flags |= netlink.TUNTAP_MULTI_QUEUE
		}
		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: name, MTU: eth0.Attrs().MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     flags,
			Queues:    queuePairs,
		}
		if err := netlink.LinkAdd(tap); err != nil {
			return fmt.Errorf("creating tap %q: %w", name, err)
		}
		fds = tap.Fds
		if err := netlink.LinkSetUp(tap); err != nil {
			return fmt.Errorf("bringing up tap %q: %w", name, err)
		}
		// Cross-connect: everything arriving on the veth peer redirects out the
		// tap and vice versa (kata's TCFilterModel: ingress qdisc + match-all u32
		// with a mirred egress-redirect action, here via U32.RedirIndex).
		for _, pair := range [][2]netlink.Link{{eth0, tap}, {tap, eth0}} {
			qdisc := &netlink.Ingress{QdiscAttrs: netlink.QdiscAttrs{
				LinkIndex: pair[0].Attrs().Index,
				Parent:    netlink.HANDLE_INGRESS,
				Handle:    netlink.MakeHandle(0xffff, 0),
			}}
			if err := netlink.QdiscReplace(qdisc); err != nil {
				return fmt.Errorf("adding ingress qdisc to %q: %w", pair[0].Attrs().Name, err)
			}
			filter := &netlink.U32{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: pair[0].Attrs().Index,
					Parent:    netlink.MakeHandle(0xffff, 0),
					Priority:  1,
					Protocol:  unix.ETH_P_ALL,
				},
				ClassId:    netlink.MakeHandle(1, 1),
				RedirIndex: pair[1].Attrs().Index,
			}
			if err := netlink.FilterAdd(filter); err != nil {
				return fmt.Errorf("adding mirred filter %s -> %s: %w", pair[0].Attrs().Name, pair[1].Attrs().Name, err)
			}
		}
		return nil
	})
	if err != nil {
		for _, f := range fds {
			_ = f.Close()
		}
		return nil, err
	}
	return fds, nil
}

// actorVethMTU reads the MTU of the actor veth (eth0 in the interior netns) so
// ateom can configure the guest eth0 with a matching MTU via the agent
// (UpdateInterface). Defaults to 1500 if the link can't be read.
func (s *AteomService) actorVethMTU(ctx context.Context) int {
	mtu := 1500
	_ = ateomnet.NetNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		if l, err := netlink.LinkByName(ateomnet.ActorVethName); err == nil {
			mtu = l.Attrs().MTU
		} else {
			slog.WarnContext(ctx, "Failed to read actor veth MTU; using default",
				slog.String("link", ateomnet.ActorVethName), slog.Int("default_mtu", mtu), slog.Any("err", err))
		}
		return nil
	})
	return mtu
}
