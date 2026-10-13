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
	"time"
)

// guestClockTimeout bounds the whole clock set, retries included, so a wedged
// agent fails the boot or restore instead of holding the actor's lock.
const guestClockTimeout = 5 * time.Second

// guestClockMaxLag is how far behind the host a clock set aims to leave the
// guest. The agent applies the timestamp it is sent, read from the host clock
// just before the call, so the guest ends behind by at most the call's round
// trip. A reply that arrives later than this is discarded and the clock set
// again with a fresh timestamp, up to guestClockAttempts sets. Prompt calls on
// AKS took a few milliseconds.
const guestClockMaxLag = 100 * time.Millisecond

// guestClockAttempts bounds the sets syncGuestClock makes while replies stay
// later than guestClockMaxLag.
const guestClockAttempts = 3

// guestClockMaxLateLag is the most the last set may lag and still be accepted
// once guestClockAttempts sets have all replied later than guestClockMaxLag,
// so an agent that answers slowly on a loaded node leaves the guest at most
// this far behind rather than failing the restore. A larger lag fails it.
const guestClockMaxLateLag = time.Second

// guestClockSetter is the kata-agent call syncGuestClock needs.
type guestClockSetter interface {
	SetGuestDateTime(ctx context.Context, now time.Time) error
}

// syncGuestClock sets an MSHV guest's wall clock to the host's before the
// actor is reported ready, leaving it off by at most guestClockMaxLag, or by
// guestClockMaxLateLag when every attempt replies late.
//
// Cloud Hypervisor advances a KVM guest's clock across a restore (v53+), but
// not an MSHV guest's: a restored MSHV guest resumes at the instant it was
// snapshotted, and chronyd in the guest image did not converge during hardware
// testing. A cold-booted MSHV guest is set too, so its clock never depends on
// the guest's RTC or chronyd. A failure fails the boot or restore rather than
// running the actor on a wrong clock.
//
// It is a no-op on KVM. Guest code can still observe the stale clock between
// vCPU resume and this call. timeout bounds all attempts together.
func syncGuestClock(ctx context.Context, h hypervisor, ac guestClockSetter, now func() time.Time, timeout time.Duration) error {
	if h != hypervisorMSHV {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for attempt := 1; ; attempt++ {
		// Wall-clock readings only (Round(0) drops the monotonic one): the
		// guest is set to host wall time, so a host clock step during the call
		// moves the guest off it, and must show in the lag. A step back shows
		// as a negative lag, the guest ahead of the host.
		sent := now().Round(0)
		if err := ac.SetGuestDateTime(ctx, sent); err != nil {
			return fmt.Errorf("while setting the guest clock: %w", err)
		}
		// Each set overwrites the last, so only this one's lag counts.
		lag := now().Round(0).Sub(sent)
		if lag.Abs() <= guestClockMaxLag {
			return nil
		}
		if attempt == guestClockAttempts {
			if lag.Abs() > guestClockMaxLateLag {
				return fmt.Errorf("while setting the guest clock: the last of %d sets left the guest off by %v, more than %v", attempt, lag.Abs(), guestClockMaxLateLag)
			}
			slog.WarnContext(ctx, "Guest clock sets kept replying late; accepting the last one",
				slog.Duration("lag", lag), slog.Duration("max_late_lag", guestClockMaxLateLag), slog.Int("attempts", attempt))
			return nil
		}
		slog.WarnContext(ctx, "Guest clock set replied late; setting it again",
			slog.Duration("lag", lag), slog.Duration("max_lag", guestClockMaxLag))
	}
}
