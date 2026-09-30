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

// Package netns creates Linux network namespaces and runs code, opens sockets
// and dials inside them. Entering a namespace is a property of the OS thread,
// so everything here locks a thread for as long as it is in one and restores
// the caller's namespace before returning.
package netns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	vishnetns "github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// Handle is a descriptor for a network namespace. Callers name it through this
// package so that nothing else has to import the one underneath.
type Handle = vishnetns.NsHandle

// GetFromName opens the namespace of that name under /run/netns.
func GetFromName(name string) (Handle, error) {
	return vishnetns.GetFromName(name)
}

// CreateNamed creates a named netns and returns its handle, restoring the
// caller's current netns before returning.
//
// The caller owns the name exclusively, so a name still present when this
// runs was left behind by an earlier incarnation and is removed first. The
// kernel creates the name with O_EXCL, so without that removal a single
// failed teardown would wedge the name for good: nothing could ever create
// it again. Removal only unmounts and unlinks the name. Anything still
// holding the namespace keeps it alive, and existing handles stay usable.
func CreateNamed(name string) (Handle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := RemoveNamed(name); err != nil {
		return -1, fmt.Errorf("while removing the leftover netns %s: %w", name, err)
	}

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := vishnetns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := vishnetns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := vishnetns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace: %w", err)
	}
	return interiorNetNS, nil
}

// RemoveNamed unmounts and unlinks a name under /run/netns, without following
// it if it is a symlink. A name that is already gone is not an error, and the
// namespace itself survives for as long as something holds it open.
func RemoveNamed(name string) error {
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

// Do runs do() with the OS thread switched into targetNS, then restores it.
func Do(ctx context.Context, targetNS Handle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := vishnetns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	// Registered before the restoring defer below since deferred calls are LIFO.
	defer curNetNS.Close()
	defer func() {
		if err := vishnetns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	if err := vishnetns.Set(targetNS); err != nil {
		return fmt.Errorf("setting target netns: %w", err)
	}
	if err := do(ctx); err != nil {
		return fmt.Errorf("while executing function in target netns: %w", err)
	}
	return nil
}

// Listen opens wildcard TCP listeners inside ns.
// Sockets retain their namespace and can be served from another namespace.
func Listen(ctx context.Context, ns Handle, ports []uint16) (_ []net.Listener, retErr error) {
	var listeners []net.Listener
	defer func() {
		if retErr != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
		}
	}()
	if err := Do(ctx, ns, func(context.Context) error {
		for _, port := range ports {
			l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
			if err != nil {
				return fmt.Errorf("while listening on port %d: %w", port, err)
			}
			listeners = append(listeners, l)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return listeners, nil
}
