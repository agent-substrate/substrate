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
	"log/slog"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/checkpointabort"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// guestFreezeTimeout bounds freezing or thawing all of an actor's containers.
const guestFreezeTimeout = 5 * time.Second

// containerFreezer is the kata-agent calls that freeze and thaw guest
// containers. The agent freezes a container's cgroup, so its processes stop
// in the guest kernel while the agent and the rest of the guest keep running.
type containerFreezer interface {
	PauseContainer(ctx context.Context, containerID string) error
	ResumeContainer(ctx context.Context, containerID string) error
}

// restoredGuestAgent is the kata-agent calls activateRestoredGuest makes.
type restoredGuestAgent interface {
	containerFreezer
	guestClockSetter
	crngReseeder
}

// freezesForSnapshot reports whether a MEMORY snapshot on h is taken with the
// actor's containers frozen. Cloud Hypervisor does not advance an MSHV guest's
// clock across a restore, so on MSHV the containers stay frozen in the
// snapshot until the restore has reseeded the CRNG and set the clock. KVM
// guests keep a correct clock across a restore and are not frozen.
func freezesForSnapshot(h hypervisor) bool {
	return h == hypervisorMSHV
}

// errThawFailed marks a failed freeze whose rollback could not thaw a
// container it had frozen, so that container may still be frozen.
var errThawFailed = errors.New("could not thaw every guest container it froze")

// freezeGuestContainers freezes the containers ids in the guest. If one fails
// it thaws, best effort, the ones it froze and the failing one, whose freeze
// may have applied with only its reply lost, and returns the error. When a
// container may still be frozen after that, the error also wraps
// errThawFailed: an earlier one did not thaw, or the failing one did not thaw
// for any reason other than the agent's refusal to thaw a container that is
// not frozen. timeout bounds the freeze, and the rollback gets as long again.
func freezeGuestContainers(ctx context.Context, ac containerFreezer, ids []string, timeout time.Duration) error {
	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for i, id := range ids {
		err := ac.PauseContainer(fctx, id)
		if err == nil {
			continue
		}
		freezeErr := fmt.Errorf("while freezing guest container %q: %w", id, err)
		// The freeze context may be what expired; the rollback gets its own.
		tctx, tcancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer tcancel()
		var thawErrs []error
		if terr := ac.ResumeContainer(tctx, id); terr != nil && !agentRefusedNotFrozen(terr) {
			thawErrs = append(thawErrs, fmt.Errorf("while thawing guest container %q: %w", id, terr))
		}
		if terr := thawEach(tctx, ac, ids[:i]); terr != nil {
			thawErrs = append(thawErrs, terr)
		}
		if len(thawErrs) > 0 {
			return fmt.Errorf("%w, and %w: %w", freezeErr, errThawFailed, errors.Join(thawErrs...))
		}
		return freezeErr
	}
	return nil
}

// agentRefusedNotFrozen reports whether err is the kata-agent's refusal to
// thaw a container that is not frozen: rustjail's LinuxContainer.resume fails
// with "container status is: <state>, not paused", which the agent sends as
// INTERNAL. Only that proves the container runs; any other error, a timeout or
// a lost connection among them, leaves it possibly frozen.
func agentRefusedNotFrozen(err error) bool {
	return status.Code(err) == codes.Internal && strings.Contains(err.Error(), "not paused")
}

// thawEach thaws every container in ids, continuing past failures, and
// returns them joined.
func thawEach(ctx context.Context, ac containerFreezer, ids []string) error {
	var errs []error
	for _, id := range ids {
		if err := ac.ResumeContainer(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("while thawing guest container %q: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// thawGuestContainers thaws the containers ids of a restored guest, stopping
// at the first failure. The restore then fails and its VM is stopped. To
// limit what runs until then, a failure re-freezes, best effort, the ones
// already thawed and the failing one, whose thaw may have applied with only
// its reply lost; a container whose re-freeze fails runs until the VM stops.
// timeout bounds the thaw, and the re-freeze gets as long again.
func thawGuestContainers(ctx context.Context, ac containerFreezer, ids []string, timeout time.Duration) error {
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for i, id := range ids {
		err := ac.ResumeContainer(tctx, id)
		if err == nil {
			continue
		}
		fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer fcancel()
		for _, thawed := range ids[:i+1] {
			if ferr := ac.PauseContainer(fctx, thawed); ferr != nil && thawed != id {
				slog.WarnContext(ctx, "Failed to re-freeze a guest container after a failed thaw; it may run until the restore tears the VM down",
					slog.String("container", thawed), slog.Any("err", ferr))
			}
		}
		return fmt.Errorf("while thawing guest container %q: %w", id, err)
	}
	return nil
}

// activateRestoredGuest resumes a restored guest's vCPUs, dials its
// kata-agent, and readies the guest for its containers: it reseeds the CRNG
// and, on MSHV, sets the clock and then thaws the containers the snapshot
// holds frozen (see freezesForSnapshot). On MSHV no container code runs
// before the clock and CRNG are fresh. On KVM only the CRNG is reseeded, and
// the containers run from the resume on, so a workload can read the snapshot's
// entropy until the reseed lands.
//
// A failure fails the restore, and the caller stops the VM. A step before the
// thaw fails with every container still frozen; a failed thaw re-freezes, best
// effort, the ones already thawed, so a failed re-freeze can leave some
// running until the VM stops. The agent is returned whenever the dial
// succeeded, so the caller can close it.
func activateRestoredGuest[A restoredGuestAgent](ctx context.Context, h hypervisor, resumeVM func(context.Context) error, dial func(context.Context) (A, error), ids []string, now func() time.Time) (A, error) {
	var ac A
	if err := resumeVM(ctx); err != nil {
		return ac, fmt.Errorf("while resuming restored guest: %w", err)
	}
	ac, err := dial(ctx)
	if err != nil {
		return ac, fmt.Errorf("while dialing kata-agent after resume: %w", err)
	}
	if err := reseedGuestCRNG(ctx, ac); err != nil {
		return ac, fmt.Errorf("while reseeding guest CRNG: %w", err)
	}
	if err := syncGuestClock(ctx, h, ac, now, guestClockTimeout); err != nil {
		return ac, err
	}
	if freezesForSnapshot(h) {
		if err := thawGuestContainers(ctx, ac, ids, guestFreezeTimeout); err != nil {
			return ac, err
		}
	}
	return ac, nil
}

// guestQuiesce stops a guest for a checkpoint at scope. On a backend that
// freezes for a MEMORY snapshot (freezesForSnapshot) it freezes the
// containers ids through ac before the checkpoint touches anything else, so
// the snapshot holds them frozen, and then pauses the VM. Elsewhere it only
// pauses the VM.
//
// The checkpoint calls begin, to freeze and take the actor off its tunnel,
// and pause last. Once the freeze succeeds nothing in the checkpoint thaws
// the containers, whatever fails after it: a VM pause that returns an error
// may still apply, and a thaw racing it could leave the VM paused with its
// containers runnable. Only a restore, in a new VM, thaws them, so the VM's
// record that they are frozen stays true for a retry.
type guestQuiesce struct {
	h     hypervisor
	scope ateworkerpb.SnapshotFidelity
	ac    containerFreezer
	ids   []string
	// tunnelDeactivated is the VM's record that a checkpoint has deactivated
	// the actor's tunnel (runningActor.checkpointDeactivatedTunnel). It
	// outlives a failed checkpoint, since only a new VM reactivates the tunnel.
	// Must not be nil.
	tunnelDeactivated *bool
	// containersFrozen is the VM's record that a checkpoint has frozen every
	// container in ids (runningActor.checkpointFrozeContainers). It outlives a
	// failed checkpoint, since nothing thaws them until a new VM restores.
	// Must not be nil.
	containersFrozen *bool
}

// begin readies the guest for the checkpoint's pause: it freezes the
// containers, if this checkpoint does that (freeze), takes the actor off its
// tunnel through deactivate, and waits for the VM's Cloud Hypervisor API
// through vmReady. The freeze needs the API, so it waits first; otherwise the
// wait follows the deactivation, as every other step of the checkpoint does.
//
// A freeze that fails without changing anything, on a VMM that is not gone,
// returns a checkpointabort error, with the actor still on its tunnel. Any
// other failed freeze fails the checkpoint, and the control plane then
// crashes the actor and releases its worker, so begin takes the actor off its
// tunnel before returning, as every later failure of the checkpoint leaves it.
func (q *guestQuiesce) begin(ctx context.Context, vmReady func(context.Context) error, vmPaused func(context.Context) (bool, error), deactivate func(context.Context) error) error {
	if !q.freezes() {
		if err := q.deactivateTunnel(ctx, deactivate); err != nil {
			return err
		}
		return vmReady(ctx)
	}
	if err := q.freeze(ctx, vmReady, vmPaused); err != nil {
		if checkpointabort.Is(err) {
			return err
		}
		if derr := q.deactivateTunnel(ctx, deactivate); derr != nil {
			return errors.Join(err, fmt.Errorf("while deactivating the actor's tunnel: %w", derr))
		}
		return err
	}
	return q.deactivateTunnel(ctx, deactivate)
}

// freezes reports whether this checkpoint freezes the guest's containers: a
// MEMORY snapshot on a backend that freezes for one, unless an earlier attempt
// of this checkpoint froze them. Those are still frozen, whether or not that
// attempt's VM pause applied.
func (q *guestQuiesce) freezes() bool {
	return q.scope == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY && freezesForSnapshot(q.h) && !*q.containersFrozen
}

// freeze waits for the VM's API, checks that the VM runs, and freezes the
// guest's containers. It runs while the actor's tunnel still serves it, so a
// failure that changes nothing returns a checkpointabort error and the actor
// stays RUNNING: the API not answering in time, a failed VM state read, or a
// failed freeze that thawed every container it froze. Those fail the
// checkpoint instead when the actor could not run on: an earlier attempt of
// this checkpoint deactivated the tunnel, or the API failure wraps
// errVMMGone, since a VMM that is gone runs no guest and no retry can
// checkpoint it.
//
// It also fails the checkpoint when the actor has no kata-agent connection,
// which only a torn-down VM lacks; when it finds the VM paused without a
// recorded freeze, since its containers may be runnable and its agent could
// not freeze them; and when a rollback may have left a container frozen.
func (q *guestQuiesce) freeze(ctx context.Context, vmReady func(context.Context) error, vmPaused func(context.Context) (bool, error)) error {
	if q.ac == nil {
		return errors.New("while freezing guest containers: the actor has no kata-agent connection")
	}
	if err := vmReady(ctx); err != nil {
		return q.abortIfRunnable(err)
	}
	paused, err := vmPaused(ctx)
	if err != nil {
		return q.abortIfRunnable(fmt.Errorf("while reading the guest's VM state: %w", err))
	}
	if paused {
		return errors.New("while freezing guest containers: the guest's VM is paused, but no checkpoint froze its containers")
	}
	if err := freezeGuestContainers(ctx, q.ac, q.ids, guestFreezeTimeout); err != nil {
		if errors.Is(err, errThawFailed) {
			return err
		}
		return q.abortIfRunnable(err)
	}
	*q.containersFrozen = true
	return nil
}

// abortIfRunnable returns err, the error of a freeze step that left the
// actor as it was, as a checkpointabort error, unless the actor could not run
// on: err wraps errVMMGone, or an earlier attempt of this checkpoint
// deactivated the actor's tunnel.
func (q *guestQuiesce) abortIfRunnable(err error) error {
	if errors.Is(err, errVMMGone) {
		return err
	}
	if *q.tunnelDeactivated {
		return fmt.Errorf("an earlier attempt of this checkpoint deactivated the actor's tunnel: %w", err)
	}
	return checkpointabort.Errorf("checkpoint aborted with the actor still running: %w", err)
}

// deactivateTunnel takes the actor off its tunnel through deactivate, and
// records that first, since a deactivation that fails may have applied in
// part.
func (q *guestQuiesce) deactivateTunnel(ctx context.Context, deactivate func(context.Context) error) error {
	*q.tunnelDeactivated = true
	return deactivate(ctx)
}

// pause pauses the VM. A pause that fails may still apply, so the containers
// stay frozen either way.
func (q *guestQuiesce) pause(ctx context.Context, pauseVM func(context.Context) error) error {
	if err := pauseVM(ctx); err != nil {
		return fmt.Errorf("while pausing guest: %w", err)
	}
	return nil
}
