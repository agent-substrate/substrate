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
	"log/slog"
	"path/filepath"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/cgroupstats"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// defaultCgroupRoot is the worker pod's own cgroup scope. The worker runs in a
// private cgroup namespace, so this path is the pod's cgroup rather than the
// host root, and runsc's per-container leaves are its direct children (see
// ateomcgroup.Delegate).
const defaultCgroupRoot = "/sys/fs/cgroup"

// sandboxCgroupContainer is the container whose cgroup leaf accounts for the
// sandbox as a whole.
//
// gVisor runs every container of a sandbox as threads inside one host process,
// the sentry. runsc starts that process from the root container's create and
// from inside that container's cgroup — container.createRoot wraps the sandbox
// and gofer spawn in cgroup.RunInCgroup — so the sentry lands in the leaf of
// the pause container, the first one RunWorkload and RestoreWorkload create.
//
// The leaf is a direct child of the delegated scope rather than of ateom's own
// cgroup, because runsc resolves cgroupsPath against the parent of the cgroup
// it is running in: cgroup v2 forbids a cgroup from holding processes and
// delegating controllers to children at once, so runsc walks up one level to
// find a directory it is allowed to create in. ateomcgroup.Delegate moves
// ateom into /sys/fs/cgroup/ateom precisely so that one level up is the
// delegated scope.
//
// The actor's own containers get leaves too, since cmdCreate shapes a
// cgroupsPath into each of their specs, but those leaves stay empty by
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
// container's and the actor container's — in the pause cgroup. Those runs
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
// ocispec.GVisorCgroupLeaf supplies the leaf name for both shaping and stats.
const sandboxCgroupContainer = ocispec.PauseContainer

// GetWorkloadStats implements ateompb.Ateom/GetWorkloadStats.
//
// It must not take the actor's lifecycle lock, which is held across whole
// boots and checkpoints: polls would stall behind runsc, and a checkpoint
// would wait on telemetry. The cgroup files are read with no lock held.
func (s *AteomService) GetWorkloadStats(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) (*ateompb.GetWorkloadStatsResponse, error) {
	if req.GetActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_uid is required")
	}

	// NOT_FOUND rather than FAILED_PRECONDITION: the requested actor is not
	// here, which no amount of retrying on the same timer will change. Its
	// worker-to-actor mapping wants re-resolving.
	hosted := s.lookupActor(req.GetActorUid())
	if hosted == nil {
		return nil, status.Errorf(codes.NotFound, "ateom is not executing actor %q", req.GetActorUid())
	}
	active := &hosted.attribution

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

	// The read above holds no lock, so a checkpoint plus a fresh run can land
	// underneath it. Pointer identity catches that: hostActor stores a new
	// record every time.
	if s.lookupActor(req.GetActorUid()) != hosted {
		return nil, status.Errorf(codes.NotFound, "ateom stopped executing actor %q while the sample was being taken", req.GetActorUid())
	}

	return &ateompb.GetWorkloadStatsResponse{Sample: sample}, nil
}

// GetActiveWorkloadStats implements
// ateompb.Ateom/GetActiveWorkloadStats: the discovery read, sampling
// whatever is executing with no identity asserted. Same lock discipline as
// GetWorkloadStats above, for the same reasons.
func (s *AteomService) GetActiveWorkloadStats(ctx context.Context, req *ateompb.GetActiveWorkloadStatsRequest) (*ateompb.GetActiveWorkloadStatsResponse, error) {
	hosted := s.hostedActors()
	samples := make([]*ateompb.WorkloadStatsSample, 0, len(hosted))
	for _, h := range hosted {
		// An actor with no numbers is reported as pending: most often it is
		// booting, but a failed read for one must not lose the others either.
		sample, err := s.sampleSandbox(&h.attribution)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				slog.WarnContext(ctx, "Failed to read sandbox cgroup", slog.String("actorUID", h.attribution.UID), slog.Any("err", err))
			}
			sample = pendingSample(&h.attribution)
		}
		// The read holds no lock, and the cgroup is found by UID alone. If the
		// actor was re-hosted meanwhile, perhaps on another template, the
		// numbers are the new activation's: report it as pending instead.
		switch latest := s.lookupActor(h.attribution.UID); {
		case latest == nil:
			continue
		case latest != h:
			sample = pendingSample(&latest.attribution)
		}
		samples = append(samples, sample)
	}

	// An empty list is "available", per the proto: a normal answer for a
	// scraper to get, not an error.
	return &ateompb.GetActiveWorkloadStatsResponse{Samples: samples}, nil
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

		SandboxClass: ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,

		ObservedAtUnixNano: time.Now().UnixNano(),
	}
}

// sampleSandbox reads the sandbox cgroup and builds the sample attributed to
// active. Errors come back raw -- notably fs.ErrNotExist for a cgroup that is
// not there yet -- because the two RPCs disagree on what that means: an error
// code for the keyed read, a normal EXECUTING answer for the discovery read.
// The read holds no lock, so the keyed caller re-checks the actor record it
// loaded after this returns.
func (s *AteomService) sampleSandbox(active *resources.ActorAttribution) (*ateompb.WorkloadStatsSample, error) {
	read := s.readSandboxCgroup
	if read == nil {
		read = cgroupstats.Read
	}
	observedAt := time.Now()
	sample, err := read(filepath.Join(s.cgroupRoot, ocispec.GVisorCgroupLeaf(active.UID, sandboxCgroupContainer)))
	if err != nil {
		return nil, err
	}

	return &ateompb.WorkloadStatsSample{
		Atespace:              active.Ref.Atespace,
		ActorName:             active.Ref.Name,
		ActorUid:              active.UID,
		ActorTemplateAtespace: active.TemplateAtespace,
		ActorTemplateName:     active.TemplateName,

		SandboxClass: ateompb.SandboxClass_SANDBOX_CLASS_GVISOR,
		Source:       ateompb.StatsSource_STATS_SOURCE_CGROUP,

		MemoryCurrentBytes:    sample.MemoryCurrentBytes,
		MemoryPeakBytes:       sample.MemoryPeakBytes,
		MemoryWorkingSetBytes: sample.MemoryWorkingSetBytes,
		CpuUsageUsec:          sample.CPUUsageUsec,

		ObservedAtUnixNano: observedAt.UnixNano(),
	}, nil
}
