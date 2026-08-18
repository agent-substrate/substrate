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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/agentstats"
	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// statsCallTimeout bounds one container's guest-agent call. The RPC is polled
// on a timer, so it must fail fast rather than pile pollers up behind a guest
// that has stopped answering: a missed sample is cheap, a stuck handler is not.
// Generous next to a healthy read, which is a vsock round trip and four small
// file reads inside the guest, and far short of the lifecycle calls' 20-30s.
const statsCallTimeout = 2 * time.Second

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
// Unlike the three lifecycle RPCs this does not take s.lock, and must not
// start: it is polled on a timer for the whole life of a workload, while lock
// is held across an entire cold boot with its retry, across a snapshot write,
// and across a restore. Blocking on it would silence the poller through exactly
// the phases whose usage is most interesting. Both pieces of state it reads —
// the attribution and the guest target — are atomics for that reason.
func (s *AteomService) GetWorkloadStats(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) (*ateompb.GetWorkloadStatsResponse, error) {
	if req.GetActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_uid is required")
	}

	// Both of these are NOT_FOUND rather than FAILED_PRECONDITION: they tell the
	// caller the requested actor is not here, which no amount of retrying on the
	// same timer will change. Its worker-to-actor mapping wants re-resolving.
	active := s.activeActor.Load()
	if active == nil {
		return nil, status.Errorf(codes.NotFound, "ateom is available; it is not executing actor %q", req.GetActorUid())
	}
	if active.UID != req.GetActorUid() {
		return nil, status.Errorf(codes.NotFound, "ateom is executing actor %q, not the requested %q", active.UID, req.GetActorUid())
	}

	sample, err := s.sampleGuest(ctx, active)
	if err != nil {
		if errors.Is(err, errStaleGuestTarget) {
			return nil, status.Error(codes.Internal, err.Error())
		}
		// "No numbers right now", never NOT_FOUND: the requested actor IS the
		// one here. The reasons are all routine -- a poll landing in the boot
		// or the restore before the target is published, a teardown that
		// cleared the target ahead of closing the connection, a restore whose
		// post-restore agent dial failed, or a guest that has stopped
		// answering. The caller should take the next sample; after a teardown
		// the next sample is the NOT_FOUND above.
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	// Re-check that the same workload is still the active one. The calls above
	// hold no lock, so a checkpoint plus a fresh run can complete underneath
	// them, and the numbers would then belong to an actor other than the one
	// being reported. Pointer identity is enough: activeActor is stored as a new
	// pointer on every Run and Restore and never mutated in place, so an
	// unchanged pointer means no transition happened across the read.
	//
	// NOT_FOUND, like the two checks above and for the same reason: the
	// requested actor is no longer the one here, so a retry lands on one of them
	// and gets that answer anyway. The same state should not report two
	// different codes depending on where in the handler it was noticed.
	if s.activeActor.Load() != active {
		return nil, status.Errorf(codes.NotFound, "ateom stopped executing actor %q while the sample was being taken", req.GetActorUid())
	}

	return &ateompb.GetWorkloadStatsResponse{Sample: sample}, nil
}

// GetActiveWorkloadStats implements
// ateompb.Ateom/GetActiveWorkloadStats: the discovery read, sampling
// whatever is executing with no identity asserted. Same lock discipline as
// GetWorkloadStats above, for the same reasons.
func (s *AteomService) GetActiveWorkloadStats(ctx context.Context, req *ateompb.GetActiveWorkloadStatsRequest) (*ateompb.GetActiveWorkloadStatsResponse, error) {
	active := s.activeActor.Load()
	if active == nil {
		return noSample(ateompb.NoSampleReason_NO_SAMPLE_REASON_NO_WORKLOAD), nil
	}

	sample, err := s.sampleGuest(ctx, active)
	if err != nil {
		if errors.Is(err, errStaleGuestTarget) {
			return nil, status.Error(codes.Internal, err.Error())
		}
		// Every routine way sampleGuest declines is a workload with no numbers
		// yet -- boot, restore, teardown in progress, a guest that has stopped
		// answering -- and for a caller with no prior knowledge each is as
		// normal a finding as an available ateom. A reason, not an error.
		return noSample(ateompb.NoSampleReason_NO_SAMPLE_REASON_NOT_MEASURABLE_YET), nil
	}

	// Same re-check as GetWorkloadStats, different answer: with no uid asserted
	// there is no "requested actor" for NOT_FOUND to disown, and a transition
	// underneath the read just means these numbers cannot be attributed to any
	// single actor. Report the reason as of now -- the next tick resolves it
	// either way.
	if latest := s.activeActor.Load(); latest != active {
		reason := ateompb.NoSampleReason_NO_SAMPLE_REASON_NOT_MEASURABLE_YET
		if latest == nil {
			reason = ateompb.NoSampleReason_NO_SAMPLE_REASON_NO_WORKLOAD
		}
		return noSample(reason), nil
	}

	return &ateompb.GetActiveWorkloadStatsResponse{
		Result: &ateompb.GetActiveWorkloadStatsResponse_Sample{Sample: sample},
	}, nil
}

// noSample is the discovery read's "nothing to give, and that is normal"
// answer.
func noSample(reason ateompb.NoSampleReason) *ateompb.GetActiveWorkloadStatsResponse {
	return &ateompb.GetActiveWorkloadStatsResponse{
		Result: &ateompb.GetActiveWorkloadStatsResponse_NoSampleReason{NoSampleReason: reason},
	}
}

// errStaleGuestTarget is the one bug-shaped failure sampleGuest can return:
// the published guest target and the attribution disagree, which the lifecycle
// RPCs write together under lock and so should never happen. Both stats reads
// map it to Internal; everything else sampleGuest returns is routine.
var errStaleGuestTarget = errors.New("guest agent connection belongs to a different actor")

// sampleGuest reads the guest's container cgroups through the agent and builds
// the sample attributed to active. With one exception its errors mean "no
// numbers right now" rather than a bug -- a guest that has stopped answering
// is routine here, and unlike the gVisor runtime's local file reads, a vsock
// call offers no error type that separates "gone" from "broken". The
// exception is errStaleGuestTarget, above. Errors come back raw because the
// two RPCs express the routine ones differently: an error code for the keyed
// read, a NoSampleReason for the discovery read. Callers re-check
// s.activeActor against the pointer they loaded after this returns; the read
// holds no lock.
func (s *AteomService) sampleGuest(ctx context.Context, active *ateomstats.ActorAttribution) (*ateompb.WorkloadStatsSample, error) {
	// The actor is the one here, but there is no guest to ask yet. Usually that
	// is a poll landing in the boot or the restore: the ateom retains the
	// attribution from the moment it accepts the actor, and the target is only
	// published once the containers are up. It is also what a teardown looks
	// like from here, since teardownActor clears the target before it closes
	// the connection, and what a restore whose post-restore agent dial failed
	// looks like for the rest of that activation.
	target := s.guestStats.Load()
	if target == nil {
		return nil, errors.New("no guest agent connection to measure yet")
	}
	// Belt and braces against the one thing that must never happen. The target
	// is published and cleared under lock alongside the attribution, so this
	// should be unreachable; if the two ever disagree, decline rather than
	// report a stale guest's numbers under the requested actor's name.
	if target.actorUID != active.UID {
		return nil, fmt.Errorf("%w: %q, not %q", errStaleGuestTarget, target.actorUID, active.UID)
	}

	observedAt := time.Now()
	sample, err := sumContainerStats(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("no container stats from the guest agent: %w", err)
	}

	return &ateompb.WorkloadStatsSample{
		Atespace:               active.Ref.Atespace,
		ActorName:              active.Ref.Name,
		ActorUid:               active.UID,
		ActorTemplateNamespace: active.TemplateNamespace,
		ActorTemplateName:      active.TemplateName,

		SandboxClass: ateompb.SandboxClass_SANDBOX_CLASS_MICROVM,
		Source:       ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT,

		MemoryCurrentBytes:    sample.MemoryCurrentBytes,
		MemoryPeakBytes:       sample.MemoryPeakBytes,
		MemoryWorkingSetBytes: sample.MemoryWorkingSetBytes,
		CpuUsageUsec:          sample.CPUUsageUsec,

		ObservedAtUnixNano: observedAt.UnixNano(),
	}, nil
}

// sumContainerStats reads every container of the actor and adds them up.
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
func sumContainerStats(ctx context.Context, target *guestStatsTarget) (agentstats.Sample, error) {
	var (
		total   agentstats.Sample
		read    int
		lastErr error
	)
	for _, id := range target.workloadIDs {
		if err := ctx.Err(); err != nil {
			// The caller is gone; the per-call ctx below would fail instantly
			// anyway, so stop burning through the remaining containers.
			return agentstats.Sample{}, err
		}
		callCtx, cancel := context.WithTimeout(ctx, statsCallTimeout)
		cs, err := target.agent.StatsContainer(callCtx, id)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		read++
		total = total.Plus(agentstats.FromCgroupStats(cs))
	}

	if read == 0 {
		if lastErr == nil {
			// No containers to read. The actor is up with an empty container
			// list, which the boot path does not produce, so treat it as "no
			// numbers" rather than reporting a confident zero.
			lastErr = errors.New("no containers to measure")
		}
		return agentstats.Sample{}, lastErr
	}
	return total, nil
}
