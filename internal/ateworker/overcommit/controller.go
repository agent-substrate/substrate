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

package overcommit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/overcommit/predictor"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

// Defaults for the ateworker's overcommit flags.
const (
	DefaultMaturationWindow = 30 * time.Second
	DefaultHistoryWindow    = 5 * time.Minute
	DefaultPollInterval     = 5 * time.Second
)

// Sampler reads the usage of every workload the ateworker hosts.
type Sampler func(ctx context.Context) (*ateworkerpb.GetActiveWorkloadStatsResponse, error)

// trackedActor is an actor the Controller books resources for.
type trackedActor struct {
	// startedAt is when the actor was first sampled.
	startedAt time.Time
	// limits are the actor's declared limits as first sampled, which is what
	// the Controller booked for it.
	limits *ateapi.Resources
	// lastCPU is the actor's previous measured sample, which the next one
	// computes a CPU rate against. nil until the first one arrives.
	lastCPU *cpuReading
}

type cpuReading struct {
	usageUsec  uint64
	observedAt int64 // Unix nanoseconds
}

// Controller moves the Worker's allocation toward the predicted usage of the
// actors it hosts.
//
// It keeps trackedAllocated, what it believes this worker has booked against
// its Worker: each actor's limits from the first sample that shows it,
// adjusted by every change the control plane has accepted. An actor missing
// from a later sample has left; its booking stays in trackedAllocated and the
// same step hands it back, since no remaining actor is predicted to need it. A
// failed change leaves trackedAllocated alone, so the next step retries it.
//
// Because it only sees what is sampled, an actor that comes and goes between
// two polls is never booked, and one that leaves and returns under the same
// UID between two polls looks like it never left. The control plane booked the
// limits of both at bind, so their resources stay allocated to the Worker.
// TODO: close this gap if polling proves too coarse in practice.
type Controller struct {
	sampler      Sampler
	predictor    predictor.Strategy
	adjuster     Adjuster
	pollInterval time.Duration

	// mu guards actors and trackedAllocated, which Step writes and the
	// accessors read. It is not held across Adjuster calls or the Sampler.
	mu               sync.Mutex
	actors           map[string]*trackedActor
	trackedAllocated *ateapi.Resources
}

// NewController returns a Controller that steps every pollInterval once Run
// is called.
func NewController(sampler Sampler, strategy predictor.Strategy, adjuster Adjuster, pollInterval time.Duration) *Controller {
	return &Controller{
		sampler:      sampler,
		predictor:    strategy,
		adjuster:     adjuster,
		pollInterval: pollInterval,
		actors:       map[string]*trackedActor{},
	}
}

// Tracking reports whether the Controller is tracking actorUID.
func (c *Controller) Tracking(actorUID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.actors[actorUID]
	return ok
}

// TrackedAllocated returns a copy of what the Controller believes this worker
// has booked against its Worker.
func (c *Controller) TrackedAllocated() *ateapi.Resources {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneResources(c.trackedAllocated)
}

// Run steps every poll interval until ctx ends. A failed step is logged and
// retried on the next tick.
func (c *Controller) Run(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := c.Step(ctx, now); err != nil {
				slog.WarnContext(ctx, "Overcommit step failed", slog.Any("err", err))
			}
		}
	}
}

// Step samples usage, predicts what the tracked actors need, and moves the
// Worker's allocation to match: handing back what is allocated beyond the
// prediction, and claiming what the prediction exceeds.
func (c *Controller) Step(ctx context.Context, now time.Time) error {
	stats, err := c.sampler(ctx)
	if err != nil {
		return fmt.Errorf("sampling workload stats: %w", err)
	}
	snapshot := c.observe(now, stats.GetSamples())

	predictedAllocated, err := c.predictor.Predict(now, snapshot)
	if err != nil {
		return fmt.Errorf("predicting with %s: %w", c.predictor.Name(), err)
	}

	c.mu.Lock()
	reclaim, grow := splitDelta(c.trackedAllocated, predictedAllocated)
	c.mu.Unlock()

	var errs []error
	if reclaim != nil {
		if err := c.adjuster.SubtractWorkerAllocated(ctx, reclaim); err != nil {
			errs = append(errs, fmt.Errorf("handing back %v: %w", reclaim, err))
		} else {
			c.mu.Lock()
			c.trackedAllocated = subResources(c.trackedAllocated, reclaim)
			c.mu.Unlock()
			slog.DebugContext(ctx, "Handed back overcommitted resources", slog.Any("resources", reclaim))
		}
	}
	if grow != nil {
		if err := c.adjuster.AddWorkerAllocated(ctx, grow); err != nil {
			errs = append(errs, fmt.Errorf("claiming %v: %w", grow, err))
		} else {
			c.mu.Lock()
			c.trackedAllocated = addResources(c.trackedAllocated, grow)
			c.mu.Unlock()
			slog.DebugContext(ctx, "Claimed resources for predicted usage", slog.Any("resources", grow))
		}
	}
	return errors.Join(errs...)
}

// observe reconciles the tracked actors with samples and returns the snapshot
// the predictor sees.
func (c *Controller) observe(now time.Time, samples []*ateworkerpb.WorkloadStatsSample) predictor.GroupUsageSnapshot {
	byUID := make(map[string]*ateworkerpb.WorkloadStatsSample, len(samples))
	for _, sample := range samples {
		if sample.GetActorUid() != "" {
			byUID[sample.GetActorUid()] = sample
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.observeActors(now, byUID)
	return c.buildSnapshot(now, byUID)
}

// observeActors brings the tracked actors in line with the sampled ones. An
// actor sampled for the first time is booked at its declared limits; one no
// longer sampled stops being tracked. The caller must hold c.mu.
func (c *Controller) observeActors(now time.Time, byUID map[string]*ateworkerpb.WorkloadStatsSample) {
	for uid := range c.actors {
		if _, ok := byUID[uid]; !ok {
			delete(c.actors, uid)
		}
	}
	for uid, sample := range byUID {
		if _, ok := c.actors[uid]; ok {
			continue
		}
		startedAt := now
		if at := sample.GetObservedAtUnixNano(); at > 0 {
			startedAt = time.Unix(0, at)
		}
		limits := declaredLimits(sample)
		c.actors[uid] = &trackedActor{startedAt: startedAt, limits: limits}
		c.trackedAllocated = addResources(c.trackedAllocated, limits)
	}
}

// buildSnapshot folds the samples into each tracked actor's CPU history and
// returns the usage of every tracked actor. One with no measurements yet has
// no usage and no CPU rate. The caller must hold c.mu.
func (c *Controller) buildSnapshot(now time.Time, byUID map[string]*ateworkerpb.WorkloadStatsSample) predictor.GroupUsageSnapshot {
	snapshot := predictor.GroupUsageSnapshot{
		Timestamp: now,
		Actors:    make(map[string]predictor.ActorUsageSample, len(c.actors)),
	}
	for uid, actor := range c.actors {
		usage := predictor.ActorUsageSample{
			ActorUID:  uid,
			StartedAt: actor.startedAt,
			Limits:    cloneResources(actor.limits),
		}
		sample, ok := byUID[uid]
		// A pending sample has no measurements to give.
		if ok && sample.GetSource() != ateworkerpb.StatsSource_STATS_SOURCE_UNSPECIFIED {
			var cpuMilli int64
			current := &cpuReading{usageUsec: sample.GetCpuUsageUsec(), observedAt: sample.GetObservedAtUnixNano()}
			if rate, ok := cpuRate(actor.lastCPU, current); ok {
				cpuMilli = rate
				usage.HasCPURate = true
			}
			usage.Usage = resources.CPUMemory(cpuMilli, clampInt64(sample.GetMemoryWorkingSetBytes()))
			actor.lastCPU = current
		}
		snapshot.Actors[uid] = usage
	}
	return snapshot
}

// declaredLimits is what the control plane booked for the actor at bind: its
// declared limits. A negative limit, which the ateworker never reports, is read
// as none.
func declaredLimits(sample *ateworkerpb.WorkloadStatsSample) *ateapi.Resources {
	return resources.CPUMemory(max(sample.GetCpuLimitMilli(), 0), max(sample.GetMemoryLimitBytes(), 0))
}

// cpuRate converts the CPU time used between two samples into millicores:
// Δcpu_usec × 10⁶ / Δt_ns. It reports false when there is no earlier sample,
// time did not advance, or the counter went backwards, which starts a new
// accounting epoch.
func cpuRate(previous, current *cpuReading) (int64, bool) {
	if previous == nil || current.observedAt <= previous.observedAt || current.usageUsec < previous.usageUsec {
		return 0, false
	}
	elapsedNanos := uint64(current.observedAt - previous.observedAt)
	return clampInt64((current.usageUsec - previous.usageUsec) * 1_000_000 / elapsedNanos), true
}

// splitDelta splits trackedAllocated − predictedAllocated into what to hand
// back (dimensions allocated beyond the prediction) and what to claim
// (dimensions predicted beyond the allocation). Both hold only positive
// quantities, and either is nil when it holds none.
func splitDelta(trackedAllocated, predictedAllocated *ateapi.Resources) (reclaim, grow *ateapi.Resources) {
	cpu := trackedAllocated.GetCpuMillicores() - predictedAllocated.GetCpuMillicores()
	memory := trackedAllocated.GetMemoryBytes() - predictedAllocated.GetMemoryBytes()
	return resources.CPUMemory(max(cpu, 0), max(memory, 0)), resources.CPUMemory(max(-cpu, 0), max(-memory, 0))
}

func addResources(a, b *ateapi.Resources) *ateapi.Resources {
	return resources.CPUMemory(a.GetCpuMillicores()+b.GetCpuMillicores(), a.GetMemoryBytes()+b.GetMemoryBytes())
}

func subResources(a, b *ateapi.Resources) *ateapi.Resources {
	return resources.CPUMemory(a.GetCpuMillicores()-b.GetCpuMillicores(), a.GetMemoryBytes()-b.GetMemoryBytes())
}

func cloneResources(r *ateapi.Resources) *ateapi.Resources {
	return resources.CPUMemory(r.GetCpuMillicores(), r.GetMemoryBytes())
}

func clampInt64(v uint64) int64 {
	const maxInt64 = 1<<63 - 1
	if v > maxInt64 {
		return maxInt64
	}
	return int64(v)
}
