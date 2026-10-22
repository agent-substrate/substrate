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
	"slices"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/checkpointabort"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// orderedAgent is a fake kata-agent that records every call, together with the
// VM calls the test routes through record, in one ordered log. Like kata it
// tracks which containers are frozen, refuses to freeze a frozen container or
// thaw a running one with kata's errors, and cannot answer while the VM is
// paused.
type orderedAgent struct {
	log *[]string
	// fail names the call ("pause b", "resume a", "reseed", "clock") that
	// fails without taking effect, with failErr or else errInjected.
	fail    string
	failErr error
	// lost names a freeze or thaw call that takes effect but whose reply is
	// lost, so the caller sees an error.
	lost string
	// frozen is the set of frozen containers.
	frozen map[string]bool
	// vm, when set, is the VM the agent runs in.
	vm *fakeCHVM
}

var errInjected = errors.New("injected")

func (a *orderedAgent) record(call string) error {
	*a.log = append(*a.log, call)
	if a.vm != nil && a.vm.paused {
		return context.DeadlineExceeded
	}
	if call == a.fail {
		if a.failErr != nil {
			return a.failErr
		}
		return errInjected
	}
	return nil
}

func (a *orderedAgent) setFrozen(call, id string, frozen bool) error {
	if err := a.record(call); err != nil {
		return err
	}
	if a.frozen == nil {
		a.frozen = map[string]bool{}
	}
	// kata's refusals, as AgentClient returns them (see agentRefusedNotFrozen).
	switch {
	case frozen && a.frozen[id]:
		return fmt.Errorf("agent PauseContainer: %w", status.Error(codes.Internal, "failed to pause container: current status is: Paused"))
	case !frozen && !a.frozen[id]:
		return fmt.Errorf("agent ResumeContainer: %w", status.Error(codes.Internal, "container status is: Running, not paused"))
	}
	a.frozen[id] = frozen
	if call == a.lost {
		return context.DeadlineExceeded
	}
	return nil
}

func (a *orderedAgent) PauseContainer(_ context.Context, id string) error {
	return a.setFrozen("pause "+id, id, true)
}

func (a *orderedAgent) ResumeContainer(_ context.Context, id string) error {
	return a.setFrozen("resume "+id, id, false)
}

func (a *orderedAgent) ReseedRandomDev(context.Context, []byte) error {
	return a.record("reseed")
}

func (a *orderedAgent) SetGuestDateTime(context.Context, time.Time) error {
	return a.record("clock")
}

// frozenIDs returns the frozen containers, sorted.
func (a *orderedAgent) frozenIDs() []string {
	var ids []string
	for id, frozen := range a.frozen {
		if frozen {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func frozenSet(ids ...string) map[string]bool {
	m := map[string]bool{}
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// fakeCHVM is Cloud Hypervisor's VM: pausing a paused VM succeeds, as with
// ch.Client.Pause.
type fakeCHVM struct {
	log  *[]string
	fail string
	// failErr, when set, is the error of the failing "vm ready" or
	// "vm state", else errInjected.
	failErr error
	paused  bool
}

func (v *fakeCHVM) failure() error {
	if v.failErr != nil {
		return v.failErr
	}
	return errInjected
}

func (v *fakeCHVM) ready(context.Context) error {
	*v.log = append(*v.log, "vm ready")
	if v.fail == "vm ready" {
		return v.failure()
	}
	return nil
}

func (v *fakeCHVM) state(context.Context) (bool, error) {
	*v.log = append(*v.log, "vm state")
	if v.fail == "vm state" {
		return false, v.failure()
	}
	return v.paused, nil
}

func (v *fakeCHVM) pause(context.Context) error {
	*v.log = append(*v.log, "vm pause")
	if v.fail == "vm pause" {
		return errInjected
	}
	v.paused = true
	return nil
}

func vmCall(log *[]string, call, fail string) func(context.Context) error {
	return func(context.Context) error {
		*log = append(*log, call)
		if call == fail {
			return errInjected
		}
		return nil
	}
}

func TestFreezeGuestContainers(t *testing.T) {
	ids := []string{"a", "b"}
	tests := []struct {
		name           string
		fail, lost     string
		failErr        error
		want           []string
		wantFrozen     []string
		wantErr        bool
		wantThawFailed bool
	}{
		{
			name:       "freezes every container",
			want:       []string{"pause a", "pause b"},
			wantFrozen: []string{"a", "b"},
		},
		{
			name:    "a failed freeze thaws what it froze",
			fail:    "pause b",
			want:    []string{"pause a", "pause b", "resume b", "resume a"},
			wantErr: true,
		},
		{
			// The freeze of b applied; only its reply was lost.
			name:    "a freeze applied before its error is thawed too",
			lost:    "pause b",
			want:    []string{"pause a", "pause b", "resume b", "resume a"},
			wantErr: true,
		},
		{
			name:    "a failed first freeze thaws nothing else",
			fail:    "pause a",
			want:    []string{"pause a", "resume a"},
			wantErr: true,
		},
		{
			name:       "a rollback that leaves a container frozen says so",
			lost:       "pause b",
			fail:       "resume a",
			want:       []string{"pause a", "pause b", "resume b", "resume a"},
			wantFrozen: []string{"a"},
			wantErr:    true, wantThawFailed: true,
		},
		{
			// The agent that lost the freeze reply times out the thaw too.
			name:       "a freeze applied before its error whose thaw times out says so",
			lost:       "pause b",
			fail:       "resume b",
			failErr:    context.DeadlineExceeded,
			want:       []string{"pause a", "pause b", "resume b", "resume a"},
			wantFrozen: []string{"b"},
			wantErr:    true, wantThawFailed: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			ac := &orderedAgent{log: &log, fail: tc.fail, failErr: tc.failErr, lost: tc.lost}
			err := freezeGuestContainers(t.Context(), ac, ids, guestFreezeTimeout)
			if (err != nil) != tc.wantErr {
				t.Errorf("freezeGuestContainers() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := errors.Is(err, errThawFailed); got != tc.wantThawFailed {
				t.Errorf("errors.Is(%v, errThawFailed) = %v, want %v", err, got, tc.wantThawFailed)
			}
			if !slices.Equal(log, tc.want) {
				t.Errorf("calls = %q, want %q", log, tc.want)
			}
			if got := ac.frozenIDs(); !slices.Equal(got, tc.wantFrozen) {
				t.Errorf("frozen = %q, want %q", got, tc.wantFrozen)
			}
		})
	}
}

func TestAgentRefusedNotFrozen(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"kata refusal", fmt.Errorf("agent ResumeContainer: %w", status.Error(codes.Internal, "container status is: Running, not paused")), true},
		{"kata cgroup thaw failure", fmt.Errorf("agent ResumeContainer: %w", status.Error(codes.Internal, "failed to write freezer state")), false},
		{"timeout", fmt.Errorf("agent ResumeContainer: %w", context.DeadlineExceeded), false},
		{"timeout status", status.Error(codes.DeadlineExceeded, "not paused"), false},
		{"closed connection", fmt.Errorf("agent ResumeContainer: %w", ttrpc.ErrClosed), false},
		{"unavailable", status.Error(codes.Unavailable, "not paused"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := agentRefusedNotFrozen(tc.err); got != tc.want {
				t.Errorf("agentRefusedNotFrozen(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestThawGuestContainers(t *testing.T) {
	ids := []string{"a", "b", "c"}
	tests := []struct {
		name       string
		fail, lost string
		want       []string
		wantFrozen []string
		wantErr    bool
	}{
		{
			name: "thaws every container",
			want: []string{"resume a", "resume b", "resume c"},
		},
		{
			// The re-freezes succeed, so every container ends frozen.
			name:       "a failed thaw re-freezes the ones already thawed",
			fail:       "resume b",
			want:       []string{"resume a", "resume b", "pause a", "pause b"},
			wantFrozen: []string{"a", "b", "c"},
			wantErr:    true,
		},
		{
			// The thaw of b applied; only its reply was lost.
			name:       "a thaw applied before its error is re-frozen too",
			lost:       "resume b",
			want:       []string{"resume a", "resume b", "pause a", "pause b"},
			wantFrozen: []string{"a", "b", "c"},
			wantErr:    true,
		},
		{
			// Re-freezing is best effort: a stays running until the VM stops.
			name:       "a failed re-freeze leaves that container running",
			lost:       "resume b",
			fail:       "pause a",
			want:       []string{"resume a", "resume b", "pause a", "pause b"},
			wantFrozen: []string{"b", "c"},
			wantErr:    true,
		},
		{
			name:       "a failed last thaw re-freezes all of them",
			fail:       "resume c",
			want:       []string{"resume a", "resume b", "resume c", "pause a", "pause b", "pause c"},
			wantFrozen: []string{"a", "b", "c"},
			wantErr:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			ac := &orderedAgent{log: &log, fail: tc.fail, lost: tc.lost, frozen: frozenSet(ids...)}
			err := thawGuestContainers(t.Context(), ac, ids, guestFreezeTimeout)
			if (err != nil) != tc.wantErr {
				t.Errorf("thawGuestContainers() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !slices.Equal(log, tc.want) {
				t.Errorf("calls = %q, want %q", log, tc.want)
			}
			if got := ac.frozenIDs(); !slices.Equal(got, tc.wantFrozen) {
				t.Errorf("frozen = %q, want %q", got, tc.wantFrozen)
			}
		})
	}
}

// TestGuestQuiesce runs a checkpoint's quiesce the way CheckpointWorkload
// does: begin (wait for the VM's API, freeze, deactivate the tunnel), then
// the other steps before the pause ("prepare"), then the VM pause, then the
// capture ("capture").
func TestGuestQuiesce(t *testing.T) {
	memory := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	rootfs := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	ids := []string{"a", "b"}
	tests := []struct {
		name       string
		h          hypervisor
		scope      ateworkerpb.SnapshotFidelity
		fail, lost string
		// vmmGone is set when the failing VM call's VMM is gone.
		vmmGone bool
		noAgent bool
		// tunnelDown is set when an earlier attempt deactivated the tunnel.
		tunnelDown bool
		// vmPaused is set when the VM is paused before the checkpoint.
		vmPaused   bool
		want       []string
		wantFrozen []string
		// wantRecorded is whether the VM records the containers frozen.
		wantRecorded bool
		wantErr      bool
		wantAbort    bool
	}{
		{
			name:         "mshv memory freezes the containers before anything else",
			h:            hypervisorMSHV,
			scope:        memory,
			want:         []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause", "capture"},
			wantFrozen:   []string{"a", "b"},
			wantRecorded: true,
		},
		{
			name:  "kvm memory only pauses the VM",
			h:     hypervisorKVM,
			scope: memory,
			want:  []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
		{
			// The guest is discarded, so there is nothing to keep frozen.
			name:  "mshv rootfs only pauses the VM",
			h:     hypervisorMSHV,
			scope: rootfs,
			want:  []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
		{
			name:      "a failed wait for the VM's API aborts with the actor untouched",
			h:         hypervisorMSHV,
			scope:     memory,
			fail:      "vm ready",
			want:      []string{"vm ready"},
			wantErr:   true,
			wantAbort: true,
		},
		{
			name:      "a failed VM state read aborts with the actor untouched",
			h:         hypervisorMSHV,
			scope:     memory,
			fail:      "vm state",
			want:      []string{"vm ready", "vm state"},
			wantErr:   true,
			wantAbort: true,
		},
		{
			name:      "a failed freeze aborts with the actor untouched",
			h:         hypervisorMSHV,
			scope:     memory,
			fail:      "pause b",
			want:      []string{"vm ready", "vm state", "pause a", "pause b", "resume b", "resume a"},
			wantErr:   true,
			wantAbort: true,
		},
		{
			name:      "a freeze applied before its error aborts with every container thawed",
			h:         hypervisorMSHV,
			scope:     memory,
			lost:      "pause b",
			want:      []string{"vm ready", "vm state", "pause a", "pause b", "resume b", "resume a"},
			wantErr:   true,
			wantAbort: true,
		},
		{
			// A container may still be frozen, so the actor cannot run on.
			name:       "a failed freeze that cannot thaw deactivates the tunnel and is no abort",
			h:          hypervisorMSHV,
			scope:      memory,
			lost:       "pause b",
			fail:       "resume a",
			want:       []string{"vm ready", "vm state", "pause a", "pause b", "resume b", "resume a", "deactivate"},
			wantFrozen: []string{"a"},
			wantErr:    true,
		},
		{
			// The tunnel stays down, so the actor cannot run on.
			name:       "a failed freeze after an earlier attempt deactivated the tunnel is no abort",
			h:          hypervisorMSHV,
			scope:      memory,
			fail:       "pause b",
			tunnelDown: true,
			want:       []string{"vm ready", "vm state", "pause a", "pause b", "resume b", "resume a", "deactivate"},
			wantErr:    true,
		},
		{
			// No guest runs, and no retry could checkpoint it.
			name:    "a failed wait for the VM's API of a VMM that is gone deactivates the tunnel and is no abort",
			h:       hypervisorMSHV,
			scope:   memory,
			fail:    "vm ready",
			vmmGone: true,
			want:    []string{"vm ready", "deactivate"},
			wantErr: true,
		},
		{
			name:    "a failed VM state read of a VMM that is gone deactivates the tunnel and is no abort",
			h:       hypervisorMSHV,
			scope:   memory,
			fail:    "vm state",
			vmmGone: true,
			want:    []string{"vm ready", "vm state", "deactivate"},
			wantErr: true,
		},
		{
			name:       "a failed wait for the VM's API after an earlier attempt deactivated the tunnel is no abort",
			h:          hypervisorMSHV,
			scope:      memory,
			fail:       "vm ready",
			tunnelDown: true,
			want:       []string{"vm ready", "deactivate"},
			wantErr:    true,
		},
		{
			name:    "no agent connection deactivates the tunnel and is no abort",
			h:       hypervisorMSHV,
			scope:   memory,
			noAgent: true,
			want:    []string{"deactivate"},
			wantErr: true,
		},
		{
			// The containers may be runnable, and the agent of a paused guest
			// cannot freeze them.
			name:     "a VM paused without a recorded freeze deactivates the tunnel and is no abort",
			h:        hypervisorMSHV,
			scope:    memory,
			vmPaused: true,
			want:     []string{"vm ready", "vm state", "deactivate"},
			wantErr:  true,
		},
		{
			// Left for a retry, which TestGuestQuiesce_Retry covers.
			name:         "a failed tunnel deactivation leaves the containers frozen",
			h:            hypervisorMSHV,
			scope:        memory,
			fail:         "deactivate",
			want:         []string{"vm ready", "vm state", "pause a", "pause b", "deactivate"},
			wantFrozen:   []string{"a", "b"},
			wantRecorded: true,
			wantErr:      true,
		},
		{
			// Left for a retry, which TestGuestQuiesce_Retry covers.
			name:         "a failure before the VM pause leaves the containers frozen",
			h:            hypervisorMSHV,
			scope:        memory,
			fail:         "prepare",
			want:         []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare"},
			wantFrozen:   []string{"a", "b"},
			wantRecorded: true,
			wantErr:      true,
		},
		{
			// The pause may still apply, so a thaw could leave the VM paused
			// with its containers runnable.
			name:         "a failed VM pause leaves the containers frozen",
			h:            hypervisorMSHV,
			scope:        memory,
			fail:         "vm pause",
			want:         []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause"},
			wantFrozen:   []string{"a", "b"},
			wantRecorded: true,
			wantErr:      true,
		},
		{
			// Left for a retry, which TestGuestQuiesce_Retry covers.
			name:         "a failure after the VM pause leaves the containers frozen",
			h:            hypervisorMSHV,
			scope:        memory,
			fail:         "capture",
			want:         []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause", "capture"},
			wantFrozen:   []string{"a", "b"},
			wantRecorded: true,
			wantErr:      true,
		},
		{
			// KVM waits after the deactivation, so the actor is already
			// unreachable.
			name:    "kvm with a failed wait for the VM's API is no abort",
			h:       hypervisorKVM,
			scope:   memory,
			fail:    "vm ready",
			want:    []string{"deactivate", "vm ready"},
			wantErr: true,
		},
		{
			name:    "kvm with a failed VM pause is no abort",
			h:       hypervisorKVM,
			scope:   memory,
			fail:    "vm pause",
			want:    []string{"deactivate", "vm ready", "prepare", "vm pause"},
			wantErr: true,
		},
		{
			name:    "kvm with a failure before the VM pause is no abort",
			h:       hypervisorKVM,
			scope:   memory,
			fail:    "prepare",
			want:    []string{"deactivate", "vm ready", "prepare"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			vm := &fakeCHVM{log: &log, fail: tc.fail, paused: tc.vmPaused}
			if tc.vmmGone {
				vm.failErr = fmt.Errorf("%w (%w)", errInjected, errVMMGone)
			}
			agent := &orderedAgent{log: &log, fail: tc.fail, lost: tc.lost, vm: vm}
			tunnelDown := tc.tunnelDown
			var froze bool
			err := runQuiesce(t.Context(), tc.h, tc.scope, agent, tc.noAgent, ids, vm, &tunnelDown, &froze, tc.fail)
			if (err != nil) != tc.wantErr {
				t.Errorf("checkpoint error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := checkpointabort.Is(err); got != tc.wantAbort {
				t.Errorf("checkpointabort.Is(%v) = %v, want %v", err, got, tc.wantAbort)
			}
			if !slices.Equal(log, tc.want) {
				t.Errorf("calls = %q, want %q", log, tc.want)
			}
			if got := agent.frozenIDs(); !slices.Equal(got, tc.wantFrozen) {
				t.Errorf("frozen = %q, want %q", got, tc.wantFrozen)
			}
			if froze != tc.wantRecorded {
				t.Errorf("recorded freeze = %v, want %v", froze, tc.wantRecorded)
			}
			// Only an abort leaves the actor on its tunnel.
			if wantDown := !tc.wantAbort || tc.tunnelDown; tunnelDown != wantDown {
				t.Errorf("recorded tunnel deactivation = %v, want %v", tunnelDown, wantDown)
			}
		})
	}
}

// TestGuestQuiesce_Retry fails a checkpoint after its freeze and retries it,
// as ateapi does when its call is canceled. No step of either attempt thaws a
// container, so the retry captures them frozen without freezing them again,
// whether or not the first attempt's VM pause applied.
func TestGuestQuiesce_Retry(t *testing.T) {
	tests := []struct {
		name string
		// fail is the first attempt's failing step.
		fail string
		// pauseLands applies the failed VM pause after the first attempt
		// returned, as a CH pause whose request timed out on the client may.
		pauseLands bool
		wantFirst  []string
		wantRetry  []string
	}{
		{
			name:      "after the VM pause",
			fail:      "capture",
			wantFirst: []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause", "capture"},
			wantRetry: []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
		{
			name:      "after a failure before the VM pause",
			fail:      "prepare",
			wantFirst: []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare"},
			wantRetry: []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
		{
			name:      "after a failed tunnel deactivation",
			fail:      "deactivate",
			wantFirst: []string{"vm ready", "vm state", "pause a", "pause b", "deactivate"},
			wantRetry: []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
		{
			name:      "after a failed VM pause that did not apply",
			fail:      "vm pause",
			wantFirst: []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause"},
			wantRetry: []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
		{
			name:       "after a failed VM pause that applies late",
			fail:       "vm pause",
			pauseLands: true,
			wantFirst:  []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause"},
			wantRetry:  []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
	}
	ids := []string{"a", "b"}
	memory := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			vm := &fakeCHVM{log: &log, fail: tc.fail}
			agent := &orderedAgent{log: &log, vm: vm}
			var tunnelDown, froze bool
			if err := runQuiesce(t.Context(), hypervisorMSHV, memory, agent, false, ids, vm, &tunnelDown, &froze, tc.fail); err == nil {
				t.Fatalf("first attempt: want the injected %s failure", tc.fail)
			}
			if !slices.Equal(log, tc.wantFirst) {
				t.Errorf("first attempt calls = %q, want %q", log, tc.wantFirst)
			}
			if !froze {
				t.Fatal("first attempt did not record the freeze")
			}
			if tc.pauseLands {
				vm.paused = true
			}
			log = nil
			vm.fail = ""
			if err := runQuiesce(t.Context(), hypervisorMSHV, memory, agent, false, ids, vm, &tunnelDown, &froze, ""); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if !slices.Equal(log, tc.wantRetry) {
				t.Errorf("retry calls = %q, want %q", log, tc.wantRetry)
			}
			// Neither attempt thawed, so the capture held every container frozen.
			if got := agent.frozenIDs(); !slices.Equal(got, ids) {
				t.Errorf("frozen after the retry = %q, want %q", got, ids)
			}
		})
	}
}

// TestGuestQuiesce_PreflightRetry fails an MSHV memory checkpoint's wait for
// the VM's API or its VM state read, either first or after an earlier attempt
// changed the actor, and then retries it with nothing failing. Only a failure
// before any change aborts, leaving the actor on its tunnel; a later one takes
// the actor off its tunnel. Either way the retry captures every container
// frozen.
func TestGuestQuiesce_PreflightRetry(t *testing.T) {
	memory := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	rootfs := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	tests := []struct {
		name string
		// earlierScope and earlierFail, when earlierFail is set, are an
		// earlier attempt's scope and failing step.
		earlierScope ateworkerpb.SnapshotFidelity
		earlierFail  string
		// fail is the failing preflight step.
		fail      string
		want      []string
		wantAbort bool
		wantRetry []string
	}{
		{
			name:      "a failed wait for the VM's API on a first attempt",
			fail:      "vm ready",
			want:      []string{"vm ready"},
			wantAbort: true,
			wantRetry: []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause", "capture"},
		},
		{
			name:      "a failed VM state read on a first attempt",
			fail:      "vm state",
			want:      []string{"vm ready", "vm state"},
			wantAbort: true,
			wantRetry: []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause", "capture"},
		},
		{
			// The freeze is not repeated, so the wait follows the deactivation.
			name:         "a failed wait for the VM's API after an attempt froze the containers",
			earlierScope: memory,
			earlierFail:  "prepare",
			fail:         "vm ready",
			want:         []string{"deactivate", "vm ready"},
			wantRetry:    []string{"deactivate", "vm ready", "prepare", "vm pause", "capture"},
		},
		{
			name:         "a failed VM state read after an attempt deactivated the tunnel",
			earlierScope: rootfs,
			earlierFail:  "prepare",
			fail:         "vm state",
			want:         []string{"vm ready", "vm state", "deactivate"},
			wantRetry:    []string{"vm ready", "vm state", "pause a", "pause b", "deactivate", "prepare", "vm pause", "capture"},
		},
	}
	ids := []string{"a", "b"}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			vm := &fakeCHVM{log: &log}
			agent := &orderedAgent{log: &log, vm: vm}
			var tunnelDown, froze bool
			if tc.earlierFail != "" {
				vm.fail = tc.earlierFail
				if err := runQuiesce(t.Context(), hypervisorMSHV, tc.earlierScope, agent, false, ids, vm, &tunnelDown, &froze, tc.earlierFail); err == nil {
					t.Fatalf("earlier attempt: want the injected %s failure", tc.earlierFail)
				}
				if !tunnelDown {
					t.Fatal("earlier attempt did not record the tunnel deactivation")
				}
			}

			log = nil
			vm.fail = tc.fail
			err := runQuiesce(t.Context(), hypervisorMSHV, memory, agent, false, ids, vm, &tunnelDown, &froze, tc.fail)
			if err == nil {
				t.Fatalf("want the injected %s failure", tc.fail)
			}
			if got := checkpointabort.Is(err); got != tc.wantAbort {
				t.Errorf("checkpointabort.Is(%v) = %v, want %v", err, got, tc.wantAbort)
			}
			if !slices.Equal(log, tc.want) {
				t.Errorf("calls = %q, want %q", log, tc.want)
			}
			if tunnelDown == tc.wantAbort {
				t.Errorf("recorded tunnel deactivation = %v, want %v", tunnelDown, !tc.wantAbort)
			}

			log = nil
			vm.fail = ""
			if err := runQuiesce(t.Context(), hypervisorMSHV, memory, agent, false, ids, vm, &tunnelDown, &froze, ""); err != nil {
				t.Fatalf("retry: %v", err)
			}
			if !slices.Equal(log, tc.wantRetry) {
				t.Errorf("retry calls = %q, want %q", log, tc.wantRetry)
			}
			if got := agent.frozenIDs(); !slices.Equal(got, ids) {
				t.Errorf("frozen after the retry = %q, want %q", got, ids)
			}
		})
	}
}

// runQuiesce is CheckpointWorkload's use of guestQuiesce, with the steps
// between begin and the pause as "prepare" and the ones after the pause as
// "capture". tunnelDown and froze are the VM's records that a checkpoint
// deactivated the tunnel and froze the containers.
func runQuiesce(ctx context.Context, h hypervisor, scope ateworkerpb.SnapshotFidelity, agent *orderedAgent, noAgent bool, ids []string, vm *fakeCHVM, tunnelDown, froze *bool, fail string) error {
	q := &guestQuiesce{h: h, scope: scope, ac: agent, ids: ids, tunnelDeactivated: tunnelDown, containersFrozen: froze}
	if noAgent {
		q.ac = nil
	}
	if err := q.begin(ctx, vm.ready, vm.state, vmCall(vm.log, "deactivate", fail)); err != nil {
		return err
	}
	if err := vmCall(vm.log, "prepare", fail)(ctx); err != nil {
		return err
	}
	if err := q.pause(ctx, vm.pause); err != nil {
		return err
	}
	return vmCall(vm.log, "capture", fail)(ctx)
}

func TestActivateRestoredGuest(t *testing.T) {
	ids := []string{"a", "b"}
	tests := []struct {
		name    string
		h       hypervisor
		fail    string
		dialErr bool
		want    []string
		// wantCode is the restore's code, OK when it succeeds.
		wantCode codes.Code
	}{
		{
			name: "mshv refreshes the clock and CRNG before thawing the containers",
			h:    hypervisorMSHV,
			want: []string{"vm resume", "dial", "reseed", "clock", "resume a", "resume b"},
		},
		{
			name: "kvm only reseeds",
			h:    hypervisorKVM,
			want: []string{"vm resume", "dial", "reseed"},
		},
		{
			name:     "a failed VM resume stops before the agent",
			h:        hypervisorMSHV,
			fail:     "vm resume",
			want:     []string{"vm resume"},
			wantCode: codes.Unavailable,
		},
		{
			name:     "a failed dial stops before the reseed",
			h:        hypervisorMSHV,
			dialErr:  true,
			want:     []string{"vm resume", "dial"},
			wantCode: codes.Unavailable,
		},
		{
			name:     "a failed reseed leaves the containers frozen",
			h:        hypervisorMSHV,
			fail:     "reseed",
			want:     []string{"vm resume", "dial", "reseed"},
			wantCode: codes.Unavailable,
		},
		{
			name:     "a failed clock set leaves the containers frozen",
			h:        hypervisorMSHV,
			fail:     "clock",
			want:     []string{"vm resume", "dial", "reseed", "clock"},
			wantCode: codes.Internal,
		},
		{
			name:     "a failed thaw fails the restore",
			h:        hypervisorMSHV,
			fail:     "resume a",
			want:     []string{"vm resume", "dial", "reseed", "clock", "resume a", "pause a"},
			wantCode: codes.Internal,
		},
		{
			name:     "a failed later thaw re-freezes the earlier ones",
			h:        hypervisorMSHV,
			fail:     "resume b",
			want:     []string{"vm resume", "dial", "reseed", "clock", "resume a", "resume b", "pause a", "pause b"},
			wantCode: codes.Internal,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var log []string
			agent := &orderedAgent{log: &log, fail: tc.fail, frozen: frozenSet(ids...)}
			dial := func(context.Context) (*orderedAgent, error) {
				log = append(log, "dial")
				if tc.dialErr {
					return nil, errInjected
				}
				return agent, nil
			}
			got, reseeded, err := activateRestoredGuest(t.Context(), tc.h, vmCall(&log, "vm resume", tc.fail), dial, ids, time.Now)
			if got := apierror.Code(err); got != tc.wantCode {
				t.Errorf("activateRestoredGuest() error = %v, code %v, want %v", err, got, tc.wantCode)
			}
			// The restore phases log times the reseed from this.
			if wantReseeded := slices.Contains(tc.want, "reseed") && tc.fail != "reseed"; reseeded.IsZero() == wantReseeded {
				t.Errorf("reseeded = %v, want it set exactly when the reseed succeeded (%v)", reseeded, wantReseeded)
			}
			if !slices.Equal(log, tc.want) {
				t.Errorf("calls = %q, want %q", log, tc.want)
			}
			// The re-freezes in these cases succeed, so on MSHV the failed
			// restore ends with every container frozen.
			if tc.h == hypervisorMSHV && tc.wantCode != codes.OK {
				if got := agent.frozenIDs(); !slices.Equal(got, ids) {
					t.Errorf("frozen after the failed restore = %q, want %q", got, ids)
				}
			}
			// The caller closes the agent on a failed restore, so it must get
			// it back whenever the dial succeeded.
			if dialed := slices.Contains(log, "dial") && !tc.dialErr; dialed != (got != nil) {
				t.Errorf("returned agent = %v, want one exactly when the dial succeeded", got)
			}
		})
	}
}
