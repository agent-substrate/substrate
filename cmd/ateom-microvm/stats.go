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
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/agentstats"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// statsCallTimeout bounds one container's guest-agent call. The RPC is polled
// on a timer, so it must fail fast rather than pile pollers up behind a guest
// that has stopped answering: a missed sample is cheap, a stuck handler is not.
// Generous next to a healthy read, which is a vsock round trip and four small
// file reads inside the guest, and far short of the lifecycle calls' 20-30s.
const statsCallTimeout = 2 * time.Second

// Sweeps and initial readings ask up to statsFanOut guests at once. A sweep
// gives up on the rest after statsSweepBudget, so it ends inside one sample
// interval. A guest not reached in time reports as pending.
const (
	statsFanOut      = 32
	statsSweepBudget = 45 * time.Second
)

// containerStatsReader is the one guest-agent call GetWorkloadStats makes.
// *kata.AgentClient satisfies it; the narrow interface is what lets the handler
// be tested without a live micro-VM, which is otherwise the only way to get an
// agent to talk to.
type containerStatsReader interface {
	StatsContainer(ctx context.Context, containerID string) (*agentpb.CgroupStats, error)
}

// guestStatsTarget is everything GetWorkloadStats needs to sample a live guest.
//
// It exists so the handler never reads AteomService.running, which lock guards:
// the handler must not take lock (see below), and a map read racing a lifecycle
// RPC's write is a data race whatever the read is for. The fields it holds are
// the ones RunWorkload and RestoreWorkload already produce.
type guestStatsTarget struct {
	// actorUID is the actor these containers belong to. Checked against the
	// attribution before anything is reported, so a target left behind by a
	// transition cannot file one actor's numbers under another's name.
	actorUID string

	// agent is the kata-agent client the actor's log forwarding already keeps
	// open for its lifetime, borrowed rather than dialed again. ttrpc
	// multiplexes, so a poll and the forwarding reads share it safely.
	agent containerStatsReader

	// workloadIDs are the guest containers to sum, one per actor container.
	// Each actor container exists in the guest as TWO kata containers (see
	// the retired guest-overlay design): a "carrier", whose only job is to make the agent
	// bind the read-only image rootfs at a fixed guest path, and the overlay
	// WORKLOAD, which lays a writable upper over it and runs the container's
	// actual process. Only workload ids belong here: a carrier is created but
	// never started, so no process ever runs in it and its cgroup has nothing
	// to add to the sum.
	workloadIDs []string
}

// GetWorkloadStats implements ateompb.Ateom/GetWorkloadStats.
//
// The sample comes from inside the guest, not from the host cgroup. On this
// runtime the host cgroup holds cloud-hypervisor, whose memory is the guest RAM
// allocation it took at boot: near-constant, and near-identical for an idle
// actor and a saturated one. The guest kernel is what accounts for the
// workload, and the kata-agent is what can read it out.
//
// It must not take the actor's lifecycle lock, which is held across a whole
// cold boot, snapshot, or restore; blocking there would silence the poller
// through the phases whose usage matters most.
func (s *AteomService) GetWorkloadStats(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) (*ateompb.GetWorkloadStatsResponse, error) {
	if req.GetActorUid() == "" {
		return nil, apierror.InvalidArgument("actor_uid is required")
	}

	// NOT_FOUND rather than FAILED_PRECONDITION: the requested actor is not
	// here, which no amount of retrying on the same timer will change. Its
	// worker-to-actor mapping wants re-resolving.
	hosted := s.lookupActor(req.GetActorUid())
	if hosted == nil {
		return nil, apierror.NotFound("ateom is not executing actor %q", req.GetActorUid())
	}
	sample, err := s.measureGuest(ctx, hosted)
	if err != nil {
		if errors.Is(err, errStaleGuestTarget) {
			return nil, apierror.Internal("%v", err)
		}
		// "No numbers right now", never NOT_FOUND: the requested actor IS the
		// one here. The reasons are all routine -- a poll landing in the boot
		// or the restore before the target is published, a teardown that
		// cleared the target ahead of closing the connection, a restore whose
		// post-restore agent dial failed, or a guest that has stopped
		// answering. The caller should take the next sample; after a teardown
		// the next sample is the NOT_FOUND above.
		return nil, apierror.FailedPrecondition("%v", err)
	}

	// The calls above hold no lock, so a checkpoint plus a fresh run can land
	// underneath them. Pointer identity catches that: hostActor stores a new
	// record every time.
	if s.lookupActor(req.GetActorUid()) != hosted {
		return nil, apierror.NotFound("ateom stopped executing actor %q while the sample was being taken", req.GetActorUid())
	}
	// The reading advanced the activation's CPU, so the cache keeps up with it.
	hosted.usage.Store(sample)

	return &ateompb.GetWorkloadStatsResponse{Sample: sample}, nil
}

// GetActiveWorkloadStats implements ateompb.Ateom/GetActiveWorkloadStats: the
// discovery read. It serves each actor's latest sample from the sampler, so a
// poll puts no load of its own on the guests; an actor not sampled yet is
// pending.
func (s *AteomService) GetActiveWorkloadStats(ctx context.Context, req *ateompb.GetActiveWorkloadStatsRequest) (*ateompb.GetActiveWorkloadStatsResponse, error) {
	hosted := s.hostedActors()
	samples := make([]*ateompb.WorkloadStatsSample, 0, len(hosted))
	for _, h := range hosted {
		sample := h.usage.Latest()
		if sample == nil {
			sample = h.usage.WithEpoch(pendingSample(&h.attribution))
		}
		samples = append(samples, sample)
	}

	// An empty list is "available", per the proto: a normal answer for a
	// scraper to get, not an error.
	return &ateompb.GetActiveWorkloadStatsResponse{Samples: samples}, nil
}

// sweepUsage samples every hosted actor between its initial reading and its
// final record, stores each sample and writes its periodic record, and returns
// the samples. Same lock discipline as GetWorkloadStats, for the same reasons.
func (s *AteomService) sweepUsage(ctx context.Context) []*ateompb.WorkloadStatsSample {
	hosted := s.hostedActors()
	sweepCtx, cancel := context.WithTimeout(ctx, statsSweepBudget)
	defer cancel()

	// A workload with no numbers yet is a pending entry, so it stays
	// attributable even if it dies during boot, and one of several booting
	// does not stop the rest being reported.
	samples := make([]*ateompb.WorkloadStatsSample, len(hosted))
	var wg sync.WaitGroup
	for i, h := range hosted {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The reads run here, outside the sampler's own recover.
			defer func() {
				if r := recover(); r != nil {
					slog.ErrorContext(ctx, "Usage sample panicked", slog.String(string(ateattr.ActorUIDKey), h.attribution.UID),
						slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				}
			}()
			samples[i] = s.sampleHostedGuest(sweepCtx, h)
		}()
	}
	wg.Wait()
	return slices.DeleteFunc(samples, func(s *ateompb.WorkloadStatsSample) bool { return s == nil })
}

// sampleHostedGuest measures one actor for the sweep, stores the sample and
// writes its periodic record, or returns nil when the actor is outside its
// sampling window or no longer hosted. A guest not reached before ctx is done,
// or that does not answer, is pending.
func (s *AteomService) sampleHostedGuest(ctx context.Context, h *hostedActor) *ateompb.WorkloadStatsSample {
	if !h.usage.Sampling() {
		return nil
	}
	sample := h.usage.WithEpoch(pendingSample(&h.attribution))
	measured, err := h.usage.Measure(ctx, func() (*ateompb.WorkloadStatsSample, map[string]uint64, error) {
		return s.sampleGuestInSlot(ctx, h)
	})
	if errors.Is(err, errStaleGuestTarget) {
		slog.ErrorContext(ctx, "Guest stats target belongs to another actor", slog.String(string(ateattr.ActorUIDKey), h.attribution.UID), slog.Any("err", err))
	}
	// Otherwise an error is boot, restore, teardown in progress, a guest that
	// has stopped answering, or the sweep's budget: all routine.
	if err == nil {
		sample = measured
	}
	// The guest is found by UID alone. If the actor was re-hosted meanwhile,
	// perhaps on another template, the numbers are the new activation's: drop
	// them, and let the new activation start with its own initial reading.
	if s.lookupActor(h.attribution.UID) != h {
		return nil
	}
	h.usage.Periodic(sample, func() { s.usage.Emit(ctx, ateattr.StatsKindPeriodic, sample) })
	return sample
}

// measureGuest reads h's guest as a reading of its activation.
func (s *AteomService) measureGuest(ctx context.Context, h *hostedActor) (*ateompb.WorkloadStatsSample, error) {
	return h.usage.Measure(ctx, func() (*ateompb.WorkloadStatsSample, map[string]uint64, error) {
		return s.sampleGuest(ctx, h)
	})
}

// sampleGuestInSlot reads h's guest while holding one of s.guestSlots. The slot
// is taken inside the reading, so a sweep waiting for another reading of the
// same actor holds no slot.
func (s *AteomService) sampleGuestInSlot(ctx context.Context, h *hostedActor) (*ateompb.WorkloadStatsSample, map[string]uint64, error) {
	select {
	case s.guestSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	defer func() { <-s.guestSlots }()
	return s.sampleGuest(ctx, h)
}

// recordInitial samples a new activation and writes its initial record. It
// runs off the resume path, since a guest read can take many seconds, and
// bounds that read like one sweep's. It recovers from panics, as a sweep does:
// it runs on its own goroutine, where a panic would take the ateom down. The
// activation is then sampled without an initial record.
func (s *AteomService) recordInitial(ctx context.Context, h *hostedActor) {
	actorUID := h.attribution.UID
	defer func() {
		if r := recover(); r != nil {
			slog.ErrorContext(ctx, "Initial usage sample panicked", slog.String(string(ateattr.ActorUIDKey), actorUID),
				slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			h.usage.Initial(nil, nil)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, statsSweepBudget)
	defer cancel()
	sample, err := h.usage.Measure(ctx, func() (*ateompb.WorkloadStatsSample, map[string]uint64, error) {
		return s.sampleGuestInSlot(ctx, h)
	})
	if err != nil {
		slog.WarnContext(ctx, "No initial usage sample", slog.String(string(ateattr.ActorUIDKey), actorUID), slog.Any("err", err))
		sample = nil
	}
	h.usage.Initial(sample, func() { s.usage.Emit(ctx, ateattr.StatsKindInitial, sample) })
}

// recordFinalIfEnded writes h's final record if a checkpoint or terminate tore
// its activation down, which unhosts the actor even when a later step fails.
// The caller holds the actor's lock.
func (s *AteomService) recordFinalIfEnded(ctx context.Context, h *hostedActor) {
	if h != nil && s.lookupActor(h.attribution.UID) != h {
		s.recordFinal(ctx, h)
	}
}

// recordFinal writes the final record of an activation that a checkpoint or a
// terminate ended, from its newest measured sample. A guest read here would add
// its latency to the checkpoint, so the record marks where the epoch ends
// rather than adding a fresher reading.
func (s *AteomService) recordFinal(ctx context.Context, h *hostedActor) {
	pending := h.usage.WithEpoch(pendingSample(&h.attribution))
	h.usage.Final(func(measured *ateompb.WorkloadStatsSample) {
		if measured == nil {
			measured = pending
		}
		s.usage.Emit(ctx, ateattr.StatsKindFinal, measured)
	})
}

// pendingSample is a workload with no numbers to give yet, as the discovery
// read reports it: attribution and the runtime family, measurements absent --
// source stays STATS_SOURCE_UNSPECIFIED, which the sample's contract defines
// as "not measured" rather than "measured as zero".
func pendingSample(active *resources.ActorAttribution) *ateompb.WorkloadStatsSample {
	return &ateompb.WorkloadStatsSample{
		Atespace:              active.Ref.Atespace,
		ActorName:             active.Ref.Name,
		ActorUid:              active.UID,
		ActorTemplateAtespace: active.TemplateAtespace,
		ActorTemplateName:     active.TemplateName,

		SandboxClass: ateompb.SandboxClass_SANDBOX_CLASS_MICROVM,

		ObservedAtUnixNano: time.Now().UnixNano(),
	}
}

// errStaleGuestTarget is the one bug-shaped failure sampleGuest can return:
// the published guest target and the attribution disagree, which the lifecycle
// RPCs write together under lock and so should never happen. GetWorkloadStats
// maps it to Internal and the sweep logs it as an error; everything else
// sampleGuest returns is routine.
var errStaleGuestTarget = errors.New("guest agent connection belongs to a different actor")

// sampleGuest reads the guest's container cgroups through the agent and builds
// the sample attributed to active. With one exception its errors mean "no
// numbers right now" rather than a bug -- a guest that has stopped answering
// is routine here, and unlike the gVisor runtime's local file reads, a vsock
// call offers no error type that separates "gone" from "broken". The
// exception is errStaleGuestTarget, above. Errors come back raw because the
// callers express the routine ones differently: an error code for the keyed
// read, a pending sample for the sweep. The read holds no lifecycle lock, so
// the keyed caller re-checks the actor record it loaded after this returns.
func (s *AteomService) sampleGuest(ctx context.Context, h *hostedActor) (*ateompb.WorkloadStatsSample, map[string]uint64, error) {
	active := &h.attribution
	// The actor is the one here, but there is no guest to ask yet. Usually that
	// is a poll landing in the boot or the restore: the ateom retains the
	// attribution from the moment it accepts the actor, and the target is only
	// published once the containers are up. It is also what a teardown looks
	// like from here, since teardownActor clears the target before it closes
	// the connection, and what a restore whose post-restore agent dial failed
	// looks like for the rest of that activation.
	// The target is read from h, not looked up by UID, so a re-host during
	// this read cannot hand over the next activation's guest.
	target := s.guestOf(h)
	if target == nil {
		return nil, nil, errors.New("no guest agent connection to measure yet")
	}
	// Belt and braces against the one thing that must never happen. The target
	// is published and cleared under lock alongside the attribution, so this
	// should be unreachable; if the two ever disagree, decline rather than
	// report a stale guest's numbers under the requested actor's name.
	if target.actorUID != active.UID {
		return nil, nil, fmt.Errorf("%w: %q, not %q", errStaleGuestTarget, target.actorUID, active.UID)
	}

	observedAt := time.Now()
	sample, cpuByContainer, err := sumContainerStats(ctx, target)
	if err != nil {
		return nil, nil, fmt.Errorf("no container stats from the guest agent: %w", err)
	}

	return &ateompb.WorkloadStatsSample{
		Atespace:              active.Ref.Atespace,
		ActorName:             active.Ref.Name,
		ActorUid:              active.UID,
		ActorTemplateAtespace: active.TemplateAtespace,
		ActorTemplateName:     active.TemplateName,

		SandboxClass: ateompb.SandboxClass_SANDBOX_CLASS_MICROVM,
		Source:       ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT,

		MemoryCurrentBytes:    sample.MemoryCurrentBytes,
		MemoryPeakBytes:       sample.MemoryPeakBytes,
		MemoryWorkingSetBytes: sample.MemoryWorkingSetBytes,
		CpuUsageUsec:          sample.CPUUsageUsec,

		ObservedAtUnixNano: observedAt.UnixNano(),
	}, cpuByContainer, nil
}

// sumContainerStats reads every container of the actor and adds them up. It
// also returns each container's CPU by id, so the activation can tell a
// container it could not read from one whose usage fell.
//
// A container the agent cannot report contributes nothing instead of failing
// the sample. That is not only the "a partial reading beats none" trade the
// gVisor side makes for a missing cgroup file — for the common way this
// happens, zero is the correct contribution rather than a fallback: a container
// that has exited took its guest cgroup with it and consumes nothing from here
// on. Failing the actor's telemetry because one sidecar is gone would be the
// wrong answer.
//
// It fails only when no container could be read at all, which is the guest as a
// whole not answering rather than one container being gone, and returns the
// last error so the caller can say why.
//
// The loop as a whole is bounded by ctx — the caller's RPC deadline — with
// maxActorContainers * statsCallTimeout as the ceiling when the caller set
// none. statsCallTimeout is per call so that one hung container read cannot
// eat the budget the remaining containers still need.
func sumContainerStats(ctx context.Context, target *guestStatsTarget) (agentstats.Sample, map[string]uint64, error) {
	var (
		total   agentstats.Sample
		cpu     = make(map[string]uint64, len(target.workloadIDs))
		lastErr error
	)
	for _, id := range target.workloadIDs {
		if err := ctx.Err(); err != nil {
			// The caller is gone; the per-call ctx below would fail instantly
			// anyway, so stop burning through the remaining containers.
			return agentstats.Sample{}, nil, err
		}
		callCtx, cancel := context.WithTimeout(ctx, statsCallTimeout)
		cs, err := target.agent.StatsContainer(callCtx, id)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		one := agentstats.FromCgroupStats(cs)
		cpu[id] = one.CPUUsageUsec
		total = total.Plus(one)
	}

	if len(cpu) == 0 {
		if lastErr == nil {
			// No containers to read. The actor is up with an empty container
			// list, which the boot path does not produce, so treat it as "no
			// numbers" rather than reporting a confident zero.
			lastErr = errors.New("no containers to measure")
		}
		return agentstats.Sample{}, nil, lastErr
	}
	return total, cpu, nil
}
