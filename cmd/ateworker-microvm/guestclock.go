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
	"time"
)

// guestClockTimeout bounds the kata-agent SetGuestDateTime call, so a wedged
// agent fails the boot or restore instead of holding the actor's lock.
const guestClockTimeout = 5 * time.Second

// guestClockSetter is the kata-agent call syncGuestClock needs.
type guestClockSetter interface {
	SetGuestDateTime(ctx context.Context, now time.Time) error
}

// syncGuestClock sets an MSHV guest's wall clock to the host's before the actor
// is reported ready.
//
// Cloud Hypervisor advances a KVM guest's clock across a restore (v53+), but
// not an MSHV guest's: a restored MSHV guest resumes at the instant it was
// snapshotted, and chronyd in the guest image did not converge during hardware
// testing. A cold-booted MSHV guest is set too, so its clock never depends on
// the guest's RTC or chronyd. A failure fails the boot or restore rather than
// running the actor on a wrong clock.
//
// It is a no-op on KVM. Guest code can still observe the stale clock between
// vCPU resume and this call. timeout bounds the agent call.
func syncGuestClock(ctx context.Context, h hypervisor, ac guestClockSetter, now func() time.Time, timeout time.Duration) error {
	if h != hypervisorMSHV {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ac.SetGuestDateTime(ctx, now()); err != nil {
		return fmt.Errorf("while setting the guest clock: %w", err)
	}
	return nil
}
