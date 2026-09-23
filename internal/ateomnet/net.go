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

// Package ateomnet provides shared networking configuration logic for Substrate runtime agents.
package ateomnet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	ActorVethName    = "eth0"
	ActorVethGateway = "169.254.17.1"
	ActorVethIP      = "169.254.17.2"

	// hostVethLocalAddress is the gateway interface's IP address and prefix length.
	hostVethLocalAddress = "169.254.17.1/30"
	// actorVethLocalAddress is the actor interface's IP address and prefix length.
	actorVethLocalAddress = "169.254.17.2/30"

	// ActorVethSubnet is the point-to-point /30 the actor veth lives on.
	ActorVethSubnet = "169.254.17.0/30"
)

var (
	HostVethAddr  = MustParseAddr(hostVethLocalAddress)
	ActorVethAddr = MustParseAddr(actorVethLocalAddress)
	ActorVethGwIP = MustParseIP(ActorVethGateway)
)

// MustParseAddr parses a CIDR string into a netlink.Addr, panicking on error.
func MustParseAddr(cidr string) *netlink.Addr {
	a, err := netlink.ParseAddr(cidr)
	if err != nil {
		panic(fmt.Sprintf("parsing constant CIDR %q: %v", cidr, err))
	}
	return a
}

// MustParseIP parses an IPv4 string into a net.IP, panicking on error.
func MustParseIP(s string) net.IP {
	ip := net.ParseIP(s).To4()
	if ip == nil {
		panic(fmt.Sprintf("parsing constant IPv4 %q", s))
	}
	return ip
}

// MustParseMAC parses a MAC address string into a net.HardwareAddr, panicking on error.
func MustParseMAC(s string) net.HardwareAddr {
	m, err := net.ParseMAC(s)
	if err != nil {
		panic(fmt.Sprintf("parsing constant MAC %q: %v", s, err))
	}
	return m
}

// AllowUnprivilegedPorts lets this namespace bind ports below 1024 without
// CAP_NET_BIND_SERVICE, which is how atunnel answers a sandbox's DNS on 53.
// The sysctl is per-namespace and grants nothing outside it.
func AllowUnprivilegedPorts() error {
	return setNetSysctl("net/ipv4/ip_unprivileged_port_start", "0")
}

// setNetSysctl writes value to the named sysctl in the current network
// namespace, remounting /proc/sys read-write when the runtime bind-mounted it
// read-only. A no-op when it already reads that way.
func setNetSysctl(key, value string) error {
	path := "/proc/sys/" + key
	if b, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(b)) == value {
		return nil
	}
	// Only EROFS is worth remounting for; any other error is returned as is.
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); !errors.Is(err, unix.EROFS) {
		if err != nil {
			return fmt.Errorf("while setting %s in worker pod netns: %w", key, err)
		}
		return nil
	}
	if err := unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT, ""); err != nil {
		return fmt.Errorf("while remounting /proc/sys read-write to set %s: %w", key, err)
	}
	defer func() {
		_ = unix.Mount("none", "/proc/sys", "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY, "")
	}()
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); err != nil {
		return fmt.Errorf("while setting %s in worker pod netns: %w", key, err)
	}
	return nil
}

func l4ProtocolEqual(proto byte) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{proto},
		},
	}
}

// CreateNetNSWithoutSwitching creates a named netns and returns its handle,
// restoring the caller's current netns before returning.
//
// The caller owns the name exclusively, so a name still present when this
// runs was left behind by an earlier incarnation and is removed first. The
// kernel creates the name with O_EXCL, so without that removal a single
// failed teardown would wedge the name for good: nothing could ever create
// it again. Removal only unmounts and unlinks the name. Anything still
// holding the namespace keeps it alive, and existing handles stay usable.
func CreateNetNSWithoutSwitching(name string) (netns.NsHandle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := removeNamedNetNS(name); err != nil {
		return -1, fmt.Errorf("while removing the leftover netns %s: %w", name, err)
	}

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := netns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace: %w", err)
	}
	return interiorNetNS, nil
}

func removeNamedNetNS(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return fmt.Errorf("invalid network namespace name %q: %w", name, os.ErrInvalid)
	}
	path := filepath.Join("/run/netns", name)
	if err := unix.Unmount(path, unix.MNT_DETACH|unix.UMOUNT_NOFOLLOW); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EINVAL) {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// NetNSDo runs do() with the OS thread switched into targetNS, then restores it.
func NetNSDo(ctx context.Context, targetNS netns.NsHandle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	if err := netns.Set(targetNS); err != nil {
		return fmt.Errorf("setting target netns: %w", err)
	}
	if err := do(ctx); err != nil {
		return fmt.Errorf("while executing function in target netns: %w", err)
	}
	return nil
}
