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

//go:build linux

package ateomnet

import (
	"context"
	"errors"
	"fmt"

	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// SetupKataTap creates the persistent host TAP consumed by runtime-rs DAN and
// cross-connects it to the actor veth with ingress redirect filters.
func SetupKataTap(ctx context.Context, interiorNetNS netns.NsHandle, tapName string) error {
	if tapName == "" {
		return errors.New("kata TAP name is required")
	}
	return NetNSDo(ctx, interiorNetNS, func(context.Context) (retErr error) {
		actorVeth, err := netlink.LinkByName(ActorVethName)
		if err != nil {
			return fmt.Errorf("acquire actor veth: %w", err)
		}
		if old, err := netlink.LinkByName(tapName); err == nil {
			if err := netlink.LinkDel(old); err != nil {
				return fmt.Errorf("remove stale Kata TAP %q: %w", tapName, err)
			}
		} else if _, notFound := errors.AsType[netlink.LinkNotFoundError](err); !notFound {
			return fmt.Errorf("look up Kata TAP %q: %w", tapName, err)
		}

		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: tapName, MTU: actorVeth.Attrs().MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR,
			Queues:    1,
		}
		if err := netlink.LinkAdd(tap); err != nil {
			return fmt.Errorf("create Kata TAP %q: %w", tapName, err)
		}
		for _, fd := range tap.Fds {
			defer fd.Close()
		}
		defer func() {
			if retErr != nil {
				_ = netlink.LinkDel(tap)
			}
		}()
		if err := netlink.LinkSetUp(tap); err != nil {
			return fmt.Errorf("bring up Kata TAP %q: %w", tapName, err)
		}

		for _, pair := range [][2]netlink.Link{{actorVeth, tap}, {tap, actorVeth}} {
			qdisc := &netlink.Ingress{QdiscAttrs: netlink.QdiscAttrs{
				LinkIndex: pair[0].Attrs().Index,
				Parent:    netlink.HANDLE_INGRESS,
				Handle:    netlink.MakeHandle(0xffff, 0),
			}}
			if err := netlink.QdiscReplace(qdisc); err != nil {
				return fmt.Errorf("add ingress qdisc to %q: %w", pair[0].Attrs().Name, err)
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
				return fmt.Errorf("add redirect %s -> %s: %w", pair[0].Attrs().Name, pair[1].Attrs().Name, err)
			}
		}
		return nil
	})
}

// CleanupKataTap removes only the named TAP. Deleting the TAP also removes its
// qdisc; CleanupActorNetwork separately owns the actor veth and nftables state.
func CleanupKataTap(ctx context.Context, interiorNetNS netns.NsHandle, tapName string) error {
	if tapName == "" {
		return errors.New("kata TAP name is required")
	}
	return NetNSDo(ctx, interiorNetNS, func(context.Context) error {
		tap, err := netlink.LinkByName(tapName)
		if _, notFound := errors.AsType[netlink.LinkNotFoundError](err); notFound {
			return nil
		}
		if err != nil {
			return fmt.Errorf("look up Kata TAP %q: %w", tapName, err)
		}
		if err := netlink.LinkDel(tap); err != nil {
			return fmt.Errorf("delete Kata TAP %q: %w", tapName, err)
		}
		return nil
	})
}
