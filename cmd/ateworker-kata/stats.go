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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworkervalidation"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/sandboxclass"
)

// A VM's host processes all run in the actor's cgroup leaf: the shim, the
// hypervisor and the helpers. The leaf's usage is the VM's usage, the guest's
// memory included, and the guest kernel and the VMM are part of it.

// GetWorkloadStats reads the actor's cgroup leaf. It takes no lifecycle lock,
// which a boot holds for its whole length.
func (s *AteWorkerService) GetWorkloadStats(ctx context.Context, req *ateworkerpb.GetWorkloadStatsRequest) (*ateworkerpb.GetWorkloadStatsResponse, error) {
	if err := ateworkervalidation.ValidateGetWorkloadStatsRequest(ctx, req); err != nil {
		return nil, err
	}
	hosted := s.lookupActor(req.GetActorUid())
	if hosted == nil {
		return nil, apierror.NotFound("worker is not executing actor %q", req.GetActorUid())
	}
	sample, err := s.measure(ctx, hosted)
	if err != nil {
		// The VM has no cgroup yet while it boots, or has gone away under the
		// read: no numbers now, and the caller takes the next sample.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, apierror.FailedPrecondition("no VM cgroup to measure yet")
		}
		return nil, apierror.Internal("reading the VM cgroup: %v", err)
	}
	// The read holds no lock, so the actor can have been replaced meanwhile.
	if s.lookupActor(req.GetActorUid()) != hosted {
		return nil, apierror.NotFound("worker stopped executing actor %q while the sample was being taken", req.GetActorUid())
	}
	return &ateworkerpb.GetWorkloadStatsResponse{Sample: sample}, nil
}

// GetActiveWorkloadStats reports every hosted actor. An actor that cannot be
// read is pending: its attribution, and no measurements.
func (s *AteWorkerService) GetActiveWorkloadStats(ctx context.Context, _ *ateworkerpb.GetActiveWorkloadStatsRequest) (*ateworkerpb.GetActiveWorkloadStatsResponse, error) {
	hosted := s.hostedActors()
	samples := make([]*ateworkerpb.WorkloadStatsSample, 0, len(hosted))
	for _, h := range hosted {
		sample, err := s.measure(ctx, h)
		if err != nil {
			sample = h.usage.WithEpoch(pendingSample(h.attribution))
		}
		samples = append(samples, sample)
	}
	return &ateworkerpb.GetActiveWorkloadStatsResponse{Samples: samples}, nil
}

// pendingSample is an actor with no numbers to give yet: attribution and the
// class, with the source left unspecified, which means "not measured".
func pendingSample(a resources.ActorAttribution) *ateworkerpb.WorkloadStatsSample {
	return &ateworkerpb.WorkloadStatsSample{
		Atespace:              a.Ref.Atespace,
		ActorName:             a.Ref.Name,
		ActorUid:              a.UID,
		ActorTemplateAtespace: a.TemplateAtespace,
		ActorTemplateName:     a.TemplateName,
		SandboxClass:          sandboxclass.Kata,
		ObservedAtUnixNano:    time.Now().UnixNano(),
	}
}

// measure reads the actor's cgroup leaf as a reading of its activation.
func (s *AteWorkerService) measure(ctx context.Context, h *hostedActor) (*ateworkerpb.WorkloadStatsSample, error) {
	return h.usage.Measure(ctx, func() (*ateworkerpb.WorkloadStatsSample, map[string]uint64, error) {
		observedAt := time.Now()
		c, err := readCgroup(filepath.Join(s.cgroupRoot, h.attribution.UID))
		if err != nil {
			return nil, nil, err
		}
		sample := pendingSample(h.attribution)
		sample.Source = ateworkerpb.StatsSource_STATS_SOURCE_CGROUP
		sample.MemoryCurrentBytes = c.memoryCurrent
		sample.MemoryPeakBytes = c.memoryPeak
		sample.MemoryWorkingSetBytes = c.workingSet()
		sample.CpuUsageUsec = c.cpuUsageUsec
		sample.ObservedAtUnixNano = observedAt.UnixNano()
		return sample, nil, nil
	})
}

// cgroupUsage is a reading of one cgroup v2 directory.
type cgroupUsage struct {
	memoryCurrent uint64
	// memoryPeak is zero on a kernel without memory.peak (before 5.19).
	memoryPeak uint64
	// inactiveFile is the reclaimable page cache in memoryCurrent.
	inactiveFile uint64
	// cpuUsageUsec is zero when the cpu controller is not delegated.
	cpuUsageUsec uint64
}

// workingSet is the memory that would have to be paged in again if reclaimed.
func (c cgroupUsage) workingSet() uint64 {
	// The files are read a moment apart, so the cache can exceed the total.
	if c.inactiveFile >= c.memoryCurrent {
		return 0
	}
	return c.memoryCurrent - c.inactiveFile
}

// readCgroup reads the usage of the cgroup directory dir. A missing directory,
// or no memory.current, is an error wrapping fs.ErrNotExist. Files a kernel can
// lack read as zero.
func readCgroup(dir string) (cgroupUsage, error) {
	var c cgroupUsage
	var err error
	if c.memoryCurrent, err = readUint(filepath.Join(dir, "memory.current")); err != nil {
		return cgroupUsage{}, err
	}
	// Best effort from here: a number the kernel does not give is zero.
	c.memoryPeak, _ = readUint(filepath.Join(dir, "memory.peak"))
	c.inactiveFile, _ = readKeyed(filepath.Join(dir, "memory.stat"), "inactive_file")
	c.cpuUsageUsec, _ = readKeyed(filepath.Join(dir, "cpu.stat"), "usage_usec")
	return c, nil
}

func readUint(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s: %w", path, err)
	}
	return v, nil
}

// readKeyed reads the value of key in a "key value" file such as memory.stat.
func readKeyed(path, key string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), " ")
		if !ok || k != key {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing %s in %s: %w", key, path, err)
		}
		return n, nil
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("%s has no %s: %w", path, key, fs.ErrNotExist)
}
