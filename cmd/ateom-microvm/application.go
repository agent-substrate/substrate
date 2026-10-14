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
	"fmt"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/wakeupprobe"
)

// guestPhase distinguishes a booted kernel from a guest with live applications.
// Existing ordinary activations use the zero value.
type guestPhase uint8

const (
	applicationRunning guestPhase = iota
	guestBooted
)

func (s *AteomService) waitActorReady(ctx context.Context, p actorBootParams) error {
	if err := wakeupprobe.WaitAll(ctx, p.containers, ateomnet.ActorVethIP, wakeupprobe.DialFunc(s.sandboxDialer(p.actorUID))); err != nil {
		return fmt.Errorf("while waiting for container wakeup probe: %w", err)
	}
	return nil
}

// publishActorApplication runs only after readiness and the last fallible step.
// Until then the caller owns cleanup of the guest connection and VM processes.
func (s *AteomService) publishActorApplication(ctx context.Context, p actorBootParams, ra *runningActor, ctrs []actorContainer) {
	ra.phase = applicationRunning
	ra.workloadIDs = workloadIDs(ctrs)
	s.setRunningVM(p.actorUID, ra)
	attribution := p.actorAttribution()
	for _, id := range ra.workloadIDs {
		s.startActorLogForwarding(ra.guestAgent, attribution, id, id)
	}
	s.setGuestStats(p.actorUID, &guestStatsTarget{actorUID: p.actorUID, agent: ra.guestAgent, workloadIDs: ra.workloadIDs})
	if hosted := s.lookupActor(p.actorUID); hosted != nil {
		go s.recordInitial(context.WithoutCancel(ctx), hosted)
	}
}

func (s *AteomService) startActorContainers(ctx context.Context, ac *kata.AgentClient, id, vsockPath string, ctrs []actorContainer) error {
	// Establish the agent sandbox + the kataShared virtio-fs mount (every
	// container's merged rootfs, durable volumes, CSI volumes, and system-info
	// volumes). All containers share it, so use the first container's hostname.
	tStart := time.Now()
	sbCtx, sbCancel := context.WithTimeout(ctx, 20*time.Second)
	err := ac.CreateSandboxForActor(sbCtx, kata.CreateSandboxOpts{
		SandboxID: id,
		Hostname:  ctrs[0].spec.Hostname,
	})
	sbCancel()
	if err != nil {
		return fmt.Errorf("while creating agent sandbox: %w", err)
	}
	tSandbox := time.Now()

	// Configure guest networking (the shim's job): eth0 IP/MAC/MTU, routes, ARP.
	mtu := uint64(actorTapMTUOf(ctx, s.sandboxNetNS(id), "tap0_kata"))
	netCtx, netCancel := context.WithTimeout(ctx, 20*time.Second)
	err = s.configureGuestNetwork(netCtx, ac, mtu)
	netCancel()
	if err != nil {
		if s.guestDebug {
			dump := kata.DebugConsoleDump(ctx, vsockPath, "ip addr 2>&1; echo '== route =='; ip route 2>&1; echo '== neigh =='; ip neigh 2>&1")
			slog.ErrorContext(ctx, "guest network config failed; dump", slog.String("dump", dump))
		}
		return fmt.Errorf("while configuring guest network: %w", err)
	}

	tNetwork := time.Now()

	for _, c := range ctrs {
		if err := s.startRootfsContainer(ctx, ac, vsockPath, c); err != nil {
			return err
		}
	}
	slog.InfoContext(ctx, "Agent setup phases", slog.String("id", id),
		slog.Duration("sandbox", tSandbox.Sub(tStart)),
		slog.Duration("network", tNetwork.Sub(tSandbox)),
		slog.Duration("containers", time.Since(tNetwork)),
		slog.Int("container_count", len(ctrs)))
	return nil
}

// startRootfsContainer brings up one container on its host-merged rootfs (the
// stock kata flow: create + start against shared/<name>/rootfs). On failure it
// dumps the guest's view of the shared tree.
//
// Its spec binds every declared volume at its mount path.
func (s *AteomService) startRootfsContainer(ctx context.Context, ac *kata.AgentClient, vsockPath string, c actorContainer) error {
	cCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := ac.StartRootfsContainer(cCtx, c.name, c.spec)
	cancel()
	if err != nil {
		if s.guestDebug {
			dump := kata.DebugConsoleDump(ctx, vsockPath,
				"echo '== shared/containers =='; ls -la /run/kata-containers/shared/containers/ 2>&1 | head -40; "+
					"echo '== rootfs =='; ls /run/kata-containers/shared/containers/"+c.name+"/rootfs/ 2>&1 | head; "+
					"echo '== mounts =='; grep -E 'kata|virtiofs' /proc/mounts 2>&1")
			slog.ErrorContext(ctx, "rootfs container failed; dump", slog.String("container", c.name), slog.String("dump", dump))
		}
		return fmt.Errorf("while starting rootfs container %q: %w", c.name, err)
	}
	return nil
}

// startActorLogForwarding spawns two goroutines that pump the actor container's
// stdout and stderr (read over the kata-agent ttrpc client ac via repeated
// ReadStdout/ReadStderr) through the shared actorlog forwarder, which annotates
// each line with the actor's ate.dev/* labels and writes it to the pod's stdout.
//
// The streams are keyed by streamID == the kata containerID==execID (the overlay
// workload id); lines are tagged with actorName + containerName
// (ate.actor.container.name) so a multi-container actor demultiplexes.
// The reader contexts are context.Background() — the goroutines are NOT bound to the
// RPC that started them; they terminate when ac is closed (by teardownActor), which
// makes the in-flight ReadStdout/ReadStderr fail and the StreamReader return io.EOF,
// ending WrapContainerLogs. This keeps the agent connection (which ttrpc allows
// concurrent Calls on) alive for forwarding while guaranteeing no goroutine outlives
// the connection.
func (s *AteomService) startActorLogForwarding(ac *kata.AgentClient, a resources.ActorAttribution, streamID, containerName string) {
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, streamID, streamID, false), a, containerName)
	go s.actorLogger.WrapContainerLogs(kata.NewStdioReader(context.Background(), ac, streamID, streamID, true), a, containerName)
}

func (s *AteomService) configureGuestNetwork(ctx context.Context, ac *kata.AgentClient, mtu uint64) error {
	if err := ac.UpdateInterface(ctx, &agentpb.Interface{
		Device: ateomnet.ActorVethName,
		Name:   ateomnet.ActorVethName,
		HwAddr: actorGuestMAC,
		Mtu:    mtu,
		IPAddresses: []*agentpb.IPAddress{
			{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethIP, Mask: "30"},
		},
	}); err != nil {
		return err
	}
	if err := ac.UpdateRoutes(ctx, []*agentpb.Route{
		{Dest: ateomnet.ActorVethSubnet, Device: ateomnet.ActorVethName, Scope: uint32(unix.RT_SCOPE_LINK), Family: agentpb.IPFamily_v4},
		{Dest: "", Gateway: ateomnet.ActorVethGateway, Device: ateomnet.ActorVethName, Family: agentpb.IPFamily_v4},
	}); err != nil {
		return err
	}
	return ac.AddARPNeighbors(ctx, []*agentpb.ARPNeighbor{{
		ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethGateway},
		Device:      ateomnet.ActorVethName,
		Lladdr:      gatewayMAC,
		State:       0x80, // NUD_PERMANENT
	}})
}
