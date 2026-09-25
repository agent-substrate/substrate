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
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
)

// actorResolvConf writes the resolver bind source outside the actor's rootfs.
func actorResolvConf(actorUID string) (string, error) {
	pod, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return "", fmt.Errorf("reading the worker pod resolv.conf: %w", err)
	}
	path := ateompath.ActorResolvConfPath(actorUID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("creating the actor directory: %w", err)
	}
	if err := os.WriteFile(path, ateomnet.SandboxResolvConf(pod), 0o644); err != nil {
		return "", fmt.Errorf("writing the actor resolv.conf: %w", err)
	}
	return path, nil
}

// removeActorResolvConf drops the file with the actor's network; atelet's
// per-activation reset clears directories under the actor's path, not files.
func removeActorResolvConf(ctx context.Context, actorUID string) {
	if err := os.Remove(ateompath.ActorResolvConfPath(actorUID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.WarnContext(ctx, "Failed to remove the actor resolv.conf", slog.Any("err", err))
	}
}

// attachAtunnel completes setup after atunnel receives the service's dialer.
func (s *AteomService) attachAtunnel(ingress *atunnel.Server, egress *atunnel.Egress, egressPort uint16) {
	s.atunnelIngress = ingress
	s.atunnelEgress = egress
	s.atunnelEgressPort = egressPort
}
