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

package netns

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"syscall"

	vishnetns "github.com/vishvananda/netns"
)

// Dialer dials TCP or UDP IP literals in ns, pinning a thread only until
// the socket is created.
func Dialer(ns Handle) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateDialTarget(network, addr); err != nil {
			return nil, err
		}

		var conn net.Conn
		dialErr := with(ns, func(restore func() error) error {
			// Only creating the socket needs the namespace, and
			// ControlContext runs once it exists: restore there rather than
			// holding the thread for the whole connect.
			socketCreated := false
			dialer := net.Dialer{ControlContext: func(context.Context, string, string, syscall.RawConn) error {
				if socketCreated {
					return errors.New("sandbox dial cannot recreate its socket outside the namespace")
				}
				socketCreated = true
				return restore()
			}}
			var err error
			conn, err = dialer.DialContext(ctx, network, addr)
			return err
		})
		if dialErr != nil || ctx.Err() != nil {
			if conn != nil {
				_ = conn.Close()
			}
			if dialErr != nil {
				return nil, dialErr
			}
			return nil, ctx.Err()
		}
		return conn, nil
	}
}

func validateDialTarget(network, addr string) error {
	switch network {
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
	default:
		return net.UnknownNetworkError(network)
	}
	hostname, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if _, err := netip.ParseAddr(hostname); err != nil {
		return fmt.Errorf("netns.Dialer supports only IP literals (got %q): %w", hostname, err)
	}
	return nil
}

// with switches to targetNS, calls run, then restores the original namespace.
// run can call restore to switch back and unlock the OS thread before returning.
// Calling restore again after it succeeds has no effect.
//
// A separate goroutine lets us leave the thread locked if restoration fails.
// Go then discards that thread when the goroutine exits.
func with(targetNS Handle, run func(restore func() error) error) error {
	var resultErr error
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		runtime.LockOSThread()
		originalNS, err := vishnetns.Get()
		if err != nil {
			runtime.UnlockOSThread()
			resultErr = fmt.Errorf("while reading the current netns: %w", err)
			return
		}
		defer originalNS.Close()
		if err := vishnetns.Set(targetNS); err != nil {
			runtime.UnlockOSThread()
			resultErr = fmt.Errorf("while entering the actor netns: %w", err)
			return
		}

		restored := false
		restore := func() error {
			if restored {
				return nil
			}
			if err := vishnetns.Set(originalNS); err != nil {
				return fmt.Errorf("while restoring the worker netns: %w", err)
			}
			runtime.UnlockOSThread()
			restored = true
			return nil
		}

		resultErr = run(restore)
		if err := restore(); err != nil {
			resultErr = err
		}
	}()
	done.Wait()
	return resultErr
}
