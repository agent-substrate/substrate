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
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/child"
)

// errVMMGone marks a failed Cloud Hypervisor API call whose VMM can never
// answer again: its process has exited, or nothing listens on its api-socket.
var errVMMGone = errors.New("the actor's cloud-hypervisor is gone")

// vmmProbe asks an actor's Cloud Hypervisor, for guestQuiesce.begin, whether
// its API is ready and whether its VM is paused. A failed call wraps
// errVMMGone when the VMM is gone (see vmmGone), so a failed freeze then fails
// the checkpoint instead of aborting it (see guestQuiesce.freeze).
type vmmProbe struct {
	client *ch.Client
	// proc is the VMM's process. It may be nil.
	proc *child.Process
	// apiSocket is the VMM's api-socket, the one client calls.
	apiSocket string
	// readyTimeout bounds ready's wait for the API.
	readyTimeout time.Duration
}

// ready waits for the VMM's API to answer.
func (p vmmProbe) ready(ctx context.Context) error {
	if _, err := p.client.WaitReady(ctx, p.readyTimeout); err != nil {
		return p.withGone(ctx, fmt.Errorf("while waiting for CH api-socket: %w", err))
	}
	return nil
}

// paused reports whether the VM is paused.
func (p vmmProbe) paused(ctx context.Context) (bool, error) {
	state, err := p.client.State(ctx)
	if err != nil {
		return false, p.withGone(ctx, err)
	}
	return state == "Paused", nil
}

// withGone adds to err, a failed API call, whether the VMM is gone.
func (p vmmProbe) withGone(ctx context.Context, err error) error {
	if gone := vmmGone(ctx, p.proc, p.apiSocket); gone != nil {
		return fmt.Errorf("%w (%w)", err, gone)
	}
	return err
}

// vmmDialTimeout bounds vmmGone's connection to the api-socket.
const vmmDialTimeout = time.Second

// vmmGone returns an error wrapping errVMMGone when the VMM that proc runs, or
// that serves apiSocket, is gone: proc has exited, or connecting to apiSocket
// finds no socket or is refused. It returns nil when neither is known, as when
// the VMM is only slow to answer. proc may be nil.
func vmmGone(ctx context.Context, proc *child.Process, apiSocket string) error {
	if proc != nil {
		select {
		case <-proc.Done():
			return fmt.Errorf("%w: its process has exited: %s", errVMMGone, proc.Exit())
		default:
		}
	}
	d := net.Dialer{Timeout: vmmDialTimeout}
	conn, err := d.DialContext(ctx, "unix", apiSocket)
	if err == nil {
		_ = conn.Close()
		return nil
	}
	if errors.Is(err, unix.ECONNREFUSED) || errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("%w: its api-socket %q has no listener: %w", errVMMGone, apiSocket, err)
	}
	return nil
}
