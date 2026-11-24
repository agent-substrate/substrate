// Copyright 2026 The Agent Substrate Authors
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
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/clock"

	"go.ate.dev/substrate/api/ateapi/v1"
)

const (
	// drainSuspendConcurrency is how many actors atelet suspends at once off
	// a draining image provider plugin. Each suspend checkpoints the actor
	// and uploads its snapshot.
	drainSuspendConcurrency = 4
	// drainRetryMin and drainRetryMax bound the backoff before atelet asks
	// again to suspend an actor the control plane refused.
	drainRetryMin = 2 * time.Second
	drainRetryMax = time.Minute
)

// drainCandidate is an actor running on the node that holds a provider lease.
type drainCandidate struct {
	podUID   string
	atespace string
	name     string
	uid      string
}

// drainSuspend is what drainSuspender knows about one actor.
type drainSuspend struct {
	inFlight bool
	// asked is set once the control plane accepted the suspend, or answered
	// that the actor is not hosted here.
	asked    bool
	failures int
	retryAt  time.Time
}

// drainSuspender asks the control plane to suspend the actors that hold image
// provider leases while the plugin drains, so the plugin can exit. A suspend
// checkpoints the actor and releases its leases, and the actor resumes later
// from its snapshot.
type drainSuspender struct {
	workers ateapi.WorkerServiceClient
	clock   clock.PassiveClock
	slots   chan struct{}

	mu     sync.Mutex
	actors map[string]*drainSuspend
}

func newDrainSuspender(workers ateapi.WorkerServiceClient, clk clock.PassiveClock) *drainSuspender {
	return &drainSuspender{
		workers: workers,
		clock:   clk,
		slots:   make(chan struct{}, drainSuspendConcurrency),
		actors:  map[string]*drainSuspend{},
	}
}

// start asks the control plane to suspend each candidate that is not asked
// yet, not in flight and not backing off, while one of the
// drainSuspendConcurrency slots is free. A later call tries the candidates
// left over. holders is every actor on the node that holds a provider lease;
// start forgets the others. It returns how many suspends it started, and a
// func that waits for them.
func (d *drainSuspender) start(ctx context.Context, holders map[string]bool, candidates []drainCandidate) (int, func()) {
	var wg sync.WaitGroup
	d.mu.Lock()
	defer d.mu.Unlock()
	for uid, a := range d.actors {
		if !holders[uid] && !a.inFlight {
			delete(d.actors, uid)
		}
	}
	now := d.clock.Now()
	started := 0
	for _, c := range candidates {
		a := d.actors[c.uid]
		if a == nil {
			a = &drainSuspend{}
			d.actors[c.uid] = a
		}
		if a.inFlight || a.asked || now.Before(a.retryAt) {
			continue
		}
		select {
		case d.slots <- struct{}{}:
		default:
			// Every slot is busy. A later call starts the rest.
			return started, wg.Wait
		}
		a.inFlight = true
		started++
		wg.Go(func() {
			defer func() { <-d.slots }()
			d.suspend(ctx, c, a)
		})
	}
	return started, wg.Wait
}

// suspend asks the control plane to suspend one actor and records the answer.
func (d *drainSuspender) suspend(ctx context.Context, c drainCandidate, a *drainSuspend) {
	suspendCtx, cancel := context.WithTimeout(ctx, drainSuspendTimeout)
	defer cancel()
	// The control plane runs the suspend back through atelet, so no actor
	// lock may be held here.
	_, err := d.workers.RequestActorSuspend(suspendCtx, &ateapi.RequestActorSuspendRequest{
		Worker:   &ateapi.ObjectRef{Name: c.podUID},
		Actor:    &ateapi.ObjectRef{Atespace: c.atespace, Name: c.name},
		ActorUid: c.uid,
	})

	d.mu.Lock()
	defer d.mu.Unlock()
	a.inFlight = false
	attrs := []any{slog.String("actor_uid", c.uid), slog.String("pod_uid", c.podUID)}
	switch {
	case err == nil:
		a.asked = true
		slog.InfoContext(ctx, "Suspended an actor off the draining image provider plugin", attrs...)
	case status.Code(err) == codes.NotFound:
		// The actor is not hosted on this node anymore, so asking again
		// cannot help.
		a.asked = true
		slog.InfoContext(ctx, "Not suspending an actor the control plane says is not hosted here",
			append(attrs, slog.Any("err", err))...)
	default:
		a.failures++
		delay := drainRetryDelay(a.failures)
		a.retryAt = d.clock.Now().Add(delay)
		// Warn on the first failure and when the backoff reaches its cap.
		level := slog.LevelDebug
		if a.failures == 1 || (delay == drainRetryMax && drainRetryDelay(a.failures-1) < drainRetryMax) {
			level = slog.LevelWarn
		}
		slog.Log(ctx, level, "Failed to suspend an actor off the draining image provider plugin",
			append(attrs, slog.Int("failures", a.failures), slog.Duration("retry_in", delay), slog.Any("err", err))...)
	}
}

// forget forgets the actors that are not in flight, so a later plugin
// instance that drains suspends the actors that resumed since.
func (d *drainSuspender) forget() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for uid, a := range d.actors {
		if !a.inFlight {
			delete(d.actors, uid)
		}
	}
}

// drainRetryDelay returns the backoff after the given number of failures,
// doubling from drainRetryMin up to drainRetryMax.
func drainRetryDelay(failures int) time.Duration {
	delay := drainRetryMin
	for i := 1; i < failures && delay < drainRetryMax; i++ {
		delay *= 2
	}
	return min(delay, drainRetryMax)
}
