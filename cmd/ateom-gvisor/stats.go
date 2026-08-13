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
	"io/fs"
	"path/filepath"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// defaultCgroupRoot is the worker pod's own cgroup scope. The worker runs in a
// private cgroup namespace, so this path is the pod's cgroup rather than the
// host root, and runsc's per-container leaves are its direct children (see
// setupCgroupDelegation).
const defaultCgroupRoot = "/sys/fs/cgroup"

// sandboxCgroupContainer is the container whose cgroup leaf accounts for the
// sandbox as a whole.
//
// gVisor runs every container of a sandbox as threads inside one host process,
// the sentry. runsc starts that process from the root container's create and
// from inside that container's cgroup — container.createRoot wraps the sandbox
// and gofer spawn in cgroup.RunInCgroup — so the sentry lands in the leaf of
// "pause", the first container RunWorkload and RestoreWorkload create.
//
// The leaf is a direct child of the delegated scope rather than of ateom's own
// cgroup, because runsc resolves cgroupsPath against the parent of the cgroup
// it is running in: cgroup v2 forbids a cgroup from holding processes and
// delegating controllers to children at once, so runsc walks up one level to
// find a directory it is allowed to create in. setupCgroupDelegation moves
// ateom into /sys/fs/cgroup/ateom precisely so that one level up is the
// delegated scope.
//
// The actor's own containers get leaves too, since cmdCreate calls
// ensureContainerCgroupsPath for each of them, but those leaves stay empty by
// design. gVisor's setupCgroupForSubcontainer creates them with empty resources
// and explains why: "Since subcontainers run exclusively inside the sandbox,
// subcontainer cgroups on the host have no effect on them. However, some tools
// (e.g. cAdvisor) uses cgroups paths to discover new containers and report
// stats for them." They are discovery markers, not accounting.
//
// So the actor's memory and CPU are the sentry's and are charged here. That is
// why a sample is attributed to the actor rather than to a container, and why
// this reads one leaf instead of summing them — the others have nothing in them
// to sum.
//
// What the leaf holds besides the actor's own work: the sentry's own overhead
// (its Go heap, page tables, netstack) and the gofers. Process listings taken
// on a live node in #161 put runsc-sandbox and both gofers — the pause
// container's and the actor container's — in the "pause" cgroup. Those runs
// predate #496, so they establish the leaf name and the fact that everything
// lands in one leaf, not the absolute path, which #496's delegation moved under
// the pod scope.
//
// So this measures the sandbox, not the actor's processes in isolation, which
// is what the proto means by "the unit of measurement is the SANDBOX". Sizing
// and chargeback want that number, since the sentry's overhead is a cost the
// node pays for running this actor, but it is not comparable to a container
// figure from a runc-based runtime, and a mostly idle actor reads as a nonzero
// floor. Splitting the actor's share out would need the sentry's own
// accounting, which the proto already says is not reported here.
//
// The name has to agree with the cgroupsPath convention in
// runsc.ensureContainerCgroupsPath, which is "/" + containerName relative to
// the same scope.
const sandboxCgroupContainer = "pause"

// GetWorkloadStats implements ateompb.Ateom/GetWorkloadStats.
//
// Unlike the three lifecycle RPCs this does not take s.lock, and must not start:
// it is polled on a timer for the whole life of a workload, while lock is held
// across entire boots and checkpoints. Blocking on it would park every poll
// behind a multi-second runsc call, and — worse in the other direction — holding
// it across the cgroup read would put a CheckpointWorkload behind telemetry.
// The attribution is read from an atomic instead, and the cgroup files are read
// with no lock held at all.
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

	sample, err := s.sampleSandbox(active)
	if err != nil {
		// The requested actor is the active one but its cgroup is not there.
		// Most often that is a poll landing in the boot: the ateom retains the
		// attribution from the moment it accepts the actor, before runsc has
		// created the leaf. The other way in is a sandbox that went away
		// underneath the read, which the next CheckpointWorkload turns into the
		// NOT_FOUND above. Either way it is "no numbers right now" and the
		// caller should take the next sample, so FAILED_PRECONDITION. Anything
		// else is a real read failure.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, status.Error(codes.FailedPrecondition, "no sandbox cgroup to measure yet")
		}
		return nil, status.Errorf(codes.Internal, "reading sandbox cgroup: %v", err)
	}

	// Re-check that the same workload is still the active one. The read above
	// holds no lock, so a checkpoint plus a fresh run can complete underneath it,
	// and the numbers would then belong to an actor other than the one being
	// reported. Pointer identity is enough: activeActor is stored as a new
	// pointer on every Run and Restore and never mutated in place, so an
	// unchanged pointer means no transition happened across the read.
	//
	// NOT_FOUND, like the two checks above and for the same reason: the requested
	// actor is no longer the one here, so a retry lands on one of them and gets
	// that answer anyway. The same state should not report two different codes
	// depending on where in the handler it was noticed.
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

	sample, err := s.sampleSandbox(active)
	if err != nil {
		// A missing cgroup is a workload with no numbers yet -- a poll landing
		// in the boot -- which for a caller with no prior knowledge is as
		// normal a finding as an available ateom, so it is a reason, not an
		// error. Anything else is a real read failure.
		if errors.Is(err, fs.ErrNotExist) {
			return noSample(ateompb.NoSampleReason_NO_SAMPLE_REASON_NOT_MEASURABLE_YET), nil
		}
		return nil, status.Errorf(codes.Internal, "reading sandbox cgroup: %v", err)
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

// sampleSandbox reads the sandbox cgroup and builds the sample attributed to
// active. Errors come back raw -- notably fs.ErrNotExist for a cgroup that is
// not there yet -- because the two RPCs disagree on what that means: an error
// code for the keyed read, a normal EXECUTING answer for the discovery read.
// Callers re-check s.activeActor against the pointer they loaded after this
// returns; the read holds no lock.
func (s *AteomService) sampleSandbox(active *ateomstats.ActorAttribution) (*ateompb.WorkloadStatsSample, error) {
	read := s.readSandboxCgroup
	if read == nil {
		read = cgroupstats.Read
	}
	observedAt := time.Now()
	sample, err := read(filepath.Join(s.cgroupRoot, sandboxCgroupContainer))
	if err != nil {
		return nil, err
	}

	return &ateompb.WorkloadStatsSample{
		Atespace:               active.Ref.Atespace,
		ActorName:              active.Ref.Name,
		ActorUid:               active.UID,
		ActorTemplateNamespace: active.TemplateNamespace,
		ActorTemplateName:      active.TemplateName,

		SandboxClass: ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Source:       ateompb.StatsSource_STATS_SOURCE_CGROUP,

		MemoryCurrentBytes:    sample.MemoryCurrentBytes,
		MemoryPeakBytes:       sample.MemoryPeakBytes,
		MemoryWorkingSetBytes: sample.MemoryWorkingSetBytes,
		CpuUsageUsec:          sample.CPUUsageUsec,

		ObservedAtUnixNano: observedAt.UnixNano(),
	}, nil
}
