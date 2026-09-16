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

//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sandboxd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type activeKataWorkload struct {
	key         string
	attribution resources.ActorAttribution
	sandbox     *sandboxd.Sandbox
	client      runtimeClient
}

type activeKataState struct {
	value atomic.Pointer[activeKataWorkload]
}

type kataDrainState struct {
	value atomic.Bool
}

type runtimeStatsClient interface {
	Stats(context.Context, *sandboxd.Sandbox) (*sandboxd.TaskStats, error)
}

type runtimeDrainClient interface {
	Drain(context.Context, *sandboxd.Sandbox) error
}

func (s *service) setActive(atespace, actorName, uid, templateAtespace, templateName, key string, sb *sandboxd.Sandbox, client runtimeClient) {
	s.active.value.Store(&activeKataWorkload{
		key: key,
		attribution: resources.ActorAttribution{
			Ref:              resources.ActorRef{Atespace: atespace, Name: actorName},
			UID:              uid,
			TemplateAtespace: templateAtespace,
			TemplateName:     templateName,
		},
		sandbox: sb,
		client:  client,
	})
}

func (s *service) clearActive(key string) {
	active := s.active.value.Load()
	if active != nil && active.key == key {
		s.active.value.CompareAndSwap(active, nil)
	}
}

func (s *service) rejectIfDraining() error {
	if s.draining.value.Load() {
		return status.Error(codes.Unavailable, "Kata worker is draining")
	}
	return nil
}

func (s *service) GetWorkloadStats(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) (*ateompb.GetWorkloadStatsResponse, error) {
	if req.GetActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor_uid is required")
	}
	active := s.active.value.Load()
	if active == nil || active.attribution.UID != req.GetActorUid() {
		return nil, status.Errorf(codes.NotFound, "Kata worker is not executing actor %q", req.GetActorUid())
	}
	sample, err := sampleKataWorkload(ctx, active)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "Kata workload is not measurable yet: %v", err)
	}
	if s.active.value.Load() != active {
		return nil, status.Errorf(codes.NotFound, "Kata worker stopped executing actor %q while sampling", req.GetActorUid())
	}
	return &ateompb.GetWorkloadStatsResponse{Sample: sample}, nil
}

func (s *service) GetActiveWorkloadStats(ctx context.Context, _ *ateompb.GetActiveWorkloadStatsRequest) (*ateompb.GetActiveWorkloadStatsResponse, error) {
	active := s.active.value.Load()
	if active == nil {
		return &ateompb.GetActiveWorkloadStatsResponse{}, nil
	}
	sample, err := sampleKataWorkload(ctx, active)
	if err != nil {
		return &ateompb.GetActiveWorkloadStatsResponse{
			Samples: []*ateompb.WorkloadStatsSample{pendingKataSample(active)},
		}, nil
	}
	if latest := s.active.value.Load(); latest != active {
		if latest == nil {
			return &ateompb.GetActiveWorkloadStatsResponse{}, nil
		}
		return &ateompb.GetActiveWorkloadStatsResponse{
			Samples: []*ateompb.WorkloadStatsSample{pendingKataSample(latest)},
		}, nil
	}
	return &ateompb.GetActiveWorkloadStatsResponse{
		Samples: []*ateompb.WorkloadStatsSample{sample},
	}, nil
}

func sampleKataWorkload(ctx context.Context, active *activeKataWorkload) (*ateompb.WorkloadStatsSample, error) {
	client, ok := active.client.(runtimeStatsClient)
	if !ok {
		return nil, errors.New("runtime does not expose Task Stats")
	}
	stats, err := client.Stats(ctx, active.sandbox)
	if err != nil {
		return nil, err
	}
	a := active.attribution
	return &ateompb.WorkloadStatsSample{
		Atespace:              a.Ref.Atespace,
		ActorName:             a.Ref.Name,
		ActorUid:              a.UID,
		ActorTemplateAtespace: a.TemplateAtespace,
		ActorTemplateName:     a.TemplateName,
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_KATA,
		Source:                ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT,
		MemoryCurrentBytes:    stats.MemoryCurrentBytes,
		MemoryPeakBytes:       stats.MemoryPeakBytes,
		MemoryWorkingSetBytes: stats.MemoryWorkingSetBytes,
		CpuUsageUsec:          stats.CPUUsageUsec,
		ObservedAtUnixNano:    time.Now().UnixNano(),
	}, nil
}

func pendingKataSample(active *activeKataWorkload) *ateompb.WorkloadStatsSample {
	a := active.attribution
	return &ateompb.WorkloadStatsSample{
		Atespace:              a.Ref.Atespace,
		ActorName:             a.Ref.Name,
		ActorUid:              a.UID,
		ActorTemplateAtespace: a.TemplateAtespace,
		ActorTemplateName:     a.TemplateName,
		SandboxClass:          ateompb.SandboxClass_SANDBOX_CLASS_KATA,
		ObservedAtUnixNano:    time.Now().UnixNano(),
	}
}

func (s *service) setActiveRPC(name string, cancel context.CancelFunc) {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	s.activeRPC = &activeRPCInfo{name: name, cancel: cancel}
}

func (s *service) clearActiveRPC() {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	s.activeRPC = nil
}

func (s *service) cancelActiveActivationRPC() {
	s.activeRPCMu.Lock()
	defer s.activeRPCMu.Unlock()
	if s.activeRPC != nil && (s.activeRPC.name == rpcRunWorkload || s.activeRPC.name == rpcRestoreWorkload) {
		s.activeRPC.cancel()
	}
}

func (s *service) gracefulShutdown(ctx context.Context) error {
	s.draining.value.Store(true)
	s.cancelActiveActivationRPC()
	if !s.mu.LockContext(ctx) {
		return ctx.Err()
	}
	defer s.mu.Unlock()
	active := s.active.value.Load()
	if active == nil {
		return nil
	}
	var errs []error
	if client, ok := active.client.(runtimeDrainClient); ok {
		if err := client.Drain(ctx, active.sandbox); err != nil {
			errs = append(errs, err)
		}
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.deleteRuntime(cleanupCtx, active.key, active.sandbox, active.client); err != nil {
		errs = append(errs, err)
	}
	if s.ingress != nil {
		if err := s.ingress.Deactivate(cleanupCtx); err != nil {
			errs = append(errs, err)
		}
	}
	if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(active.attribution.UID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, err)
	}
	if err := s.network.Cleanup(cleanupCtx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
