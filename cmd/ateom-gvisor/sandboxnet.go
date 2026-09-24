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

// prepareSandboxNetwork builds the actor's network and starts serving it.
func (s *AteomService) prepareSandboxNetwork(ctx context.Context, actorUID string) error {
	if err := s.releaseSandboxNetwork(ctx); err != nil {
		return err
	}
	session, err := ateomnet.ServeSandbox(ctx, ateomnet.SandboxNetworkConfig{
		ActorUID:   actorUID,
		Veth:       true,
		EgressPort: s.atunnelEgressPort,
		DNSPort:    atunnel.DNSPort,
	}, s.atunnelEgress, s.dnsRelay)
	if err != nil {
		return fmt.Errorf("while setting up the sandbox network: %w", err)
	}

	// Point the sandbox resolver at its gateway.
	if _, err := actorResolvConf(actorUID); err != nil {
		_ = session.Close(ctx)
		return err
	}

	return s.sandbox.Replace(ctx, session)
}

// releaseSandboxNetwork stops serving the actor and takes its network down,
// along with the resolv.conf atelet's per-activation reset leaves behind.
func (s *AteomService) releaseSandboxNetwork(ctx context.Context) error {
	if session := s.sandbox.Session(); session != nil {
		if err := os.Remove(ateompath.ActorResolvConfPath(session.Network.ActorUID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			slog.WarnContext(ctx, "Failed to remove the actor resolv.conf", slog.Any("err", err))
		}
	}
	return s.sandbox.Close(ctx)
}

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

// attachAtunnel completes setup after atunnel receives the service's dialer.
func (s *AteomService) attachAtunnel(ingress *atunnel.Server, egress *atunnel.Egress, egressPort uint16) {
	s.atunnelIngress = ingress
	s.atunnelEgress = egress
	s.atunnelEgressPort = egressPort
}
