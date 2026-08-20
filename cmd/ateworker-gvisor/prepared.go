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

	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sandboxprep"
	"go.ate.dev/substrate/internal/sizing"
	"go.ate.dev/substrate/internal/workerstats"
)

// preparedSandbox is guarded by its actor's lifecycle lock.
type preparedSandbox struct {
	request     *ateworkerpb.PrepareSandboxRequest
	rcmd        *runsc
	rootCreated bool
	discarding  bool
}

func (s *AteWorkerService) preparedSandbox(actorUID string) *preparedSandbox {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	if h := s.actors[actorUID]; h != nil {
		return h.prepared
	}
	return nil
}

func (s *AteWorkerService) setPreparedSandbox(actorUID string, p *preparedSandbox) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if h := s.actors[actorUID]; h != nil {
		h.prepared = p
	}
}

func (s *AteWorkerService) preparedActorUIDs() []string {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	var uids []string
	for uid, h := range s.actors {
		if h.prepared != nil {
			uids = append(uids, uid)
		}
	}
	return uids
}

func (p *preparedSandbox) validate(req *ateworkerpb.PrepareSandboxRequest) error {
	if p.discarding {
		return apierror.FailedPrecondition("prepared sandbox cleanup is incomplete")
	}
	if !sandboxprep.Matches(p.request, req) {
		return apierror.FailedPrecondition("prepared sandbox does not match workload request")
	}
	return nil
}

// PrepareSandbox starts the pause container independently of application images.
func (s *AteWorkerService) PrepareSandbox(ctx context.Context, req *ateworkerpb.PrepareSandboxRequest) (_ *ateworkerpb.PrepareSandboxResponse, retErr error) {
	w := req.GetWorkload()
	if w.GetActorUid() == "" {
		return nil, apierror.InvalidArgument("actor_uid is required")
	}
	if err := validateActorDirs(w.GetActorDirs()); err != nil {
		return nil, err
	}
	if err := validateRunscPath(w.GetRunscPath()); err != nil {
		return nil, err
	}
	if !s.locks.Lock(ctx, w.GetActorUid()) {
		return nil, ctx.Err()
	}
	defer s.locks.Unlock(w.GetActorUid())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	release, err := s.beginRPC(w.GetActorUid(), "PrepareSandbox", cancel)
	if err != nil {
		return nil, err
	}
	defer release()
	if p := s.preparedSandbox(w.GetActorUid()); p != nil {
		return &ateworkerpb.PrepareSandboxResponse{}, p.validate(req)
	}
	if s.lookupActor(w.GetActorUid()) != nil {
		return nil, apierror.FailedPrecondition("actor already has a running workload")
	}
	if err := resetRunscStateAndPidFileDirs(w.GetActorDirs()); err != nil {
		return nil, err
	}
	attribution := workerstats.ActorAttributionFromRequest(w)
	if err := s.tunnel.Deactivate(ctx, attribution); err != nil {
		return nil, err
	}
	_, err = s.hostActor(ctx, attribution, w.GetActorDirs())
	if err != nil {
		return nil, err
	}
	p := &preparedSandbox{
		request: proto.Clone(req).(*ateworkerpb.PrepareSandboxRequest),
		rcmd: &runsc{
			path: w.GetRunscPath(), actorUID: w.GetActorUid(), actorDirs: w.GetActorDirs(),
			size:           sizing.FromLimits(w.GetCpuMilli(), w.GetMemoryBytes()),
			durableVolumes: durableVolumeNames(w.GetSpec()), containers: w.GetSpec().GetContainers(), flags: s.flags,
		},
	}
	s.setPreparedSandbox(w.GetActorUid(), p)
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			retErr = errors.Join(retErr, s.discardPreparedSandbox(cleanupCtx, p))
		}
	}()
	if err := imagecache.SetupBundleRootfs(ociBundlePath(w.GetActorDirs(), ocispec.PauseContainer)); err != nil {
		return nil, fmt.Errorf("while composing pause rootfs: %w", err)
	}
	if !req.GetFromCheckpoint() {
		pw, err := s.actorLogger.StartJSONLogPipe(attribution, ocispec.PauseContainer)
		if err != nil {
			return nil, err
		}
		defer pw.Close()
		p.rootCreated = true
		if err := p.rcmd.cmdCreate(ctx, pw, ocispec.PauseContainer, nil); err != nil {
			return nil, err
		}
		if err := p.rcmd.cmdStart(ctx, pw, ocispec.PauseContainer); err != nil {
			return nil, err
		}
	}
	return &ateworkerpb.PrepareSandboxResponse{}, nil
}

func (s *AteWorkerService) DiscardPreparedSandbox(ctx context.Context, req *ateworkerpb.DiscardPreparedSandboxRequest) (*ateworkerpb.DiscardPreparedSandboxResponse, error) {
	if !s.locks.Lock(ctx, req.GetActorUid()) {
		return nil, ctx.Err()
	}
	defer s.locks.Unlock(req.GetActorUid())
	if p := s.preparedSandbox(req.GetActorUid()); p != nil {
		if err := s.discardPreparedSandbox(ctx, p); err != nil {
			return nil, err
		}
	}
	return &ateworkerpb.DiscardPreparedSandboxResponse{}, nil
}

func (s *AteWorkerService) discardPreparedSandbox(ctx context.Context, p *preparedSandbox) error {
	p.discarding = true
	if p.rootCreated {
		if err := p.rcmd.cmdDelete(ctx, ocispec.PauseContainer); err != nil {
			gone, checkErr := isContainerAlreadyGone(ctx, p.rcmd, ocispec.PauseContainer)
			if !gone {
				return errors.Join(err, checkErr)
			}
		}
		p.rootCreated = false
	}
	if err := imagecache.UnmountAllUnder(p.rcmd.actorDirs.GetOciBundleDir()); err != nil {
		return err
	}
	return s.unhostActor(ctx, p.rcmd.actorUID)
}
