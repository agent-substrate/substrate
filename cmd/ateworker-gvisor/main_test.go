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
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateworkerpb"
)

// Every RPC rejects a request without ActorDirs before touching any state.
func TestRPCsRejectMissingActorDirs(t *testing.T) {
	s := &AteWorkerService{}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateworkerpb.RunWorkloadRequest{})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateworkerpb.RestoreWorkloadRequest{})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateworkerpb.CheckpointWorkloadRequest{})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateworkerpb.TerminateWorkloadRequest{})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}

func TestRPCsRejectUntrustedRunscPath(t *testing.T) {
	s := &AteWorkerService{}
	ctx := context.Background()
	dirs := &ateworkerpb.ActorDirs{
		RootDir:                   "/node/actors/actor-a",
		OciBundleDir:              "/node/actors/actor-a/bundle",
		CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
		RestoreDir:                "/node/actors/actor-a/restore",
		DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
		SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
		VolumesDir:                "/node/actors/actor-a/volumes",
	}
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateworkerpb.RunWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateworkerpb.RestoreWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateworkerpb.CheckpointWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateworkerpb.TerminateWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}
