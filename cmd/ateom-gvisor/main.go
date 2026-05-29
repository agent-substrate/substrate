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
	"net"
	"os"
	"runtime"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/cmd/ateom-gvisor/internal/ateom"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/hashicorp/go-reap"
	"github.com/spf13/pflag"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	podUID = pflag.String("pod-uid", "", "The UID of the current pod")

	showVersion = pflag.Bool("version", false, "Print version and exit.")

	reapLock sync.RWMutex
)

const (
	hostVethName      = "ateom0"
	actorVethName     = "eth0"
	actorVethTempName = "ateom1"
	hostVethCIDR      = "10.200.0.1/30"
	actorVethCIDR     = "10.200.0.2/30"
	actorVethGateway  = "10.200.0.1"
	actorVethIP       = "10.200.0.2"
	defaultActorPort  = "80"
	actorNftTableName = "ateom_actor"
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}

	ctx := context.Background()

	if err := do(ctx); err != nil {
		slog.ErrorContext(ctx, "Error while executing", slog.Any("err", err))
		os.Exit(1)
	}
}

func do(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	syncedWriter := ateom.NewSyncedWriter(os.Stdout)
	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(syncedWriter, nil)))
	slog.SetDefault(logger)

	slog.InfoContext(ctx, "ateom booting")

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "ateom-gvisor",
		Sampler:     sdktrace.ParentBased(sdktrace.NeverSample()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	// Create ateom dir
	ateomDir := ateompath.AteomPath(*podUID)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}

	// TODO: Consider whether we want to fork, so that we have an "init" process
	// as PID 1 that does nothing but reap processes that get reparented to it.
	// Then we won't have to mess about with locking the reaper while we do our
	// own exec.Cmd calls.
	go reap.ReapChildren(nil, nil, nil, &reapLock)
	slog.InfoContext(ctx, "Child process reaper launched")

	// Clean up any old socket.
	sockPath := ateompath.AteomSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	// Create a new network namespace that we will pass to gVisor.  gVisor will
	// read the addresses and routes off of every link in the namespace, then
	// remove all the addresses and handle injecting packets into the interfaces
	// using AF_PACKET.
	interiorNetNS, err := createNetNSWithoutSwitching(ctx, ateompath.AteomNetNSName(*podUID))
	if err != nil {
		return fmt.Errorf("while creating ateom-interior netns: %w", err)
	}

	actorLogger := ateom.NewActorLogger(syncedWriter, metadata.OnGCE())
	ateomService := NewService(interiorNetNS, actorLogger)

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, ateomService)
	reflection.Register(svr)

	if err := svr.Serve(lis); err != nil {
		slog.ErrorContext(ctx, "Failed to serve", slog.Any("err", err))
		os.Exit(1)
	}

	return nil
}

// AteomService is a service for shepherding single microvm.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// Let's go ahead and assume that Ateom RPCs that are running `runsc`
	// subcommands are probably not safe to call concurrently.
	lock sync.Mutex

	interiorNetNS netns.NsHandle
	actorLogger   *ateom.ActorLogger
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(interiorNetNS netns.NsHandle, actorLogger *ateom.ActorLogger) *AteomService {
	svc := &AteomService{
		interiorNetNS: interiorNetNS,
		actorLogger:   actorLogger,
	}
	return svc
}

func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.actorLogger.EmitLifecycleLog("Actor starting", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.

	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Run failure", "err", cleanupErr)
			}
		}
	}()

	rcmd := &runsc{
		path:                   req.GetRunscPath(),
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	// Create and start pause container
	if err := rcmd.cmdCreate(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while starting pause container: %w", err)
	}

	pw, err := s.actorLogger.StartJSONLogPipe(req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())
	if err != nil {
		return nil, fmt.Errorf("while starting json log pipe: %w", err)
	}
	defer pw.Close()

	// Create and start each application container
	for _, ac := range req.GetSpec().GetContainers() {
		if err := rcmd.cmdCreate(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
		}
	}

	s.actorLogger.EmitLifecycleLog("Actor started", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	return &ateompb.RunWorkloadResponse{}, nil
}

func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.actorLogger.EmitLifecycleLog("Actor checkpointing", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	// Contract with atelet:
	//
	//   * After we exit, atelet will upload checkpoint to GCS
	//   * After we exit, atelet will tear down OCI bundles and reset the actor directory.

	rcmd := &runsc{
		path:                   req.GetRunscPath(),
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	checkpointPath := ateompath.CheckpointStateDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
	if err := os.MkdirAll(checkpointPath, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint directory: %w", err)
	}

	// Checkpoint pause container (root of the sandbox)
	if err := rcmd.cmdCheckpoint(ctx, "pause", checkpointPath); err != nil {
		return nil, fmt.Errorf("while checkpointing pause: %w", err)
	}

	// Check state of all containers to mimic containerd.
	//
	// Without this, `runsc delete` occasionally throws an error.
	if err := rcmd.cmdState(ctx, "pause"); err != nil {
		return nil, fmt.Errorf("while checking state of pause container: %w", err)
	}
	for _, ctr := range req.GetSpec().GetContainers() {
		if err := rcmd.cmdState(ctx, ctr.GetName()); err != nil {
			return nil, fmt.Errorf("while deleting %q application container: %w", ctr.GetName(), err)
		}
	}

	// Delete all application containers
	for _, ctr := range req.GetSpec().GetContainers() {
		if err := rcmd.cmdDelete(ctx, ctr.GetName()); err != nil {
			return nil, fmt.Errorf("while deleting %q application container: %w", ctr.GetName(), err)
		}
	}

	// Delete pause container
	if err := rcmd.cmdDelete(ctx, "pause"); err != nil {
		return nil, fmt.Errorf("while deleting pause container: %w", err)
	}

	if err := s.cleanupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while cleaning up actor network: %w", err)
	}

	s.actorLogger.EmitLifecycleLog("Actor checkpointed", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	return nil, nil
}

func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.actorLogger.EmitLifecycleLog("Actor restoring", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.
	//   * Checkpoint downloaded and placed on disk

	if err := s.setupActorNetwork(ctx); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up actor network after Restore failure", "err", cleanupErr)
			}
		}
	}()

	rcmd := &runsc{
		path:                   req.GetRunscPath(),
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	checkpointDir := ateompath.RestoreStateDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())

	// Create and restore pause container
	if err := rcmd.cmdCreate(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdRestore(ctx, os.Stdout, "pause", checkpointDir); err != nil {
		return nil, fmt.Errorf("while starting pause container: %w", err)
	}

	pw, err := s.actorLogger.StartJSONLogPipe(req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())
	if err != nil {
		return nil, fmt.Errorf("while starting json log pipe: %w", err)
	}
	defer pw.Close()

	// Create and restore each application container
	for _, ac := range req.GetSpec().GetContainers() {
		if err := rcmd.cmdCreate(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdRestore(ctx, pw, ac.GetName(), checkpointDir); err != nil {
			return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
		}
	}

	s.actorLogger.EmitLifecycleLog("Actor restored", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	return &ateompb.RestoreWorkloadResponse{}, nil
}

func (s *AteomService) setupActorNetwork(ctx context.Context) (retErr error) {
	// Build a fresh point-to-point network between the worker pod netns and the
	// gVisor interior netns. The worker side keeps the pod's real eth0, creates
	// ateom0 as the gateway, and moves only the veth peer into the actor netns.
	// The actor side renames that peer to eth0 and installs a default route via
	// the worker-side veth address. This replaces the old behavior of moving the
	// Kubernetes-provided eth0 out of the worker pod.
	//
	// The nftables rules installed here are a compatibility bridge for the
	// current router assumptions: actor egress is masqueraded behind the worker
	// pod IP, and inbound traffic to the worker pod's HTTP port is DNAT'd to the
	// actor veth IP. Later transparent egress capture will replace the broad
	// egress NAT with AgentGateway-bound capture rules.
	//
	// Clean up stale state from a failed prior activation before creating the
	// next actor-side network. The worker currently runs one actor at a time.
	if err := s.cleanupActorNetwork(ctx); err != nil {
		return fmt.Errorf("while removing stale actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			if cleanupErr := s.cleanupActorNetwork(ctx); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to clean up partially configured actor network", "err", cleanupErr)
			}
		}
	}()

	podIP, err := podIPv4()
	if err != nil {
		return fmt.Errorf("while resolving pod IPv4 address: %w", err)
	}

	hostAddr, err := parseAddr(hostVethCIDR)
	if err != nil {
		return err
	}

	veth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{
			Name: hostVethName,
		},
		PeerName: actorVethTempName,
	}
	if err := netlink.LinkAdd(veth); err != nil {
		return fmt.Errorf("while creating actor veth pair: %w", err)
	}

	hostLink, err := netlink.LinkByName(hostVethName)
	if err != nil {
		return fmt.Errorf("while getting host veth: %w", err)
	}
	if err := netlink.AddrReplace(hostLink, hostAddr); err != nil {
		return fmt.Errorf("while assigning host veth address: %w", err)
	}
	if err := netlink.LinkSetUp(hostLink); err != nil {
		return fmt.Errorf("while bringing up host veth: %w", err)
	}

	actorLink, err := netlink.LinkByName(actorVethTempName)
	if err != nil {
		return fmt.Errorf("while getting actor veth peer: %w", err)
	}
	if err := netlink.LinkSetNsFd(actorLink, int(s.interiorNetNS)); err != nil {
		return fmt.Errorf("while moving actor veth peer into interior netns: %w", err)
	}

	if err := netNSDo(ctx, s.interiorNetNS, configureActorVeth); err != nil {
		return fmt.Errorf("while configuring actor veth in interior netns: %w", err)
	}

	if err := enableIPv4Forwarding(); err != nil {
		return err
	}
	if err := installActorNftablesRules(podIP); err != nil {
		return err
	}

	if err := dumpNetInfo(ctx, "Pod NetNS "); err != nil {
		return fmt.Errorf("while dumping pod netns links: %w", err)
	}
	if err := netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		return dumpNetInfo(ctx, "Interior NetNS ")
	}); err != nil {
		return fmt.Errorf("while dumping interior netns links: %w", err)
	}

	return nil
}

func configureActorVeth(ctx context.Context) error {
	// Run inside the gVisor interior netns after setupActorNetwork moves the
	// veth peer there. gVisor reads link names, addresses, and routes from this
	// namespace when the workload starts, so the peer is deliberately renamed to
	// eth0 and configured like a normal container interface:
	//
	//   * lo is brought up for localhost behavior.
	//   * the temporary veth peer is renamed to eth0.
	//   * eth0 receives the actor-side /30 address.
	//   * the default route points to the worker-side veth gateway.
	loLink, err := netlink.LinkByName("lo")
	if err != nil {
		return fmt.Errorf("while acquiring lo in interior netns: %w", err)
	}
	if err := netlink.LinkSetUp(loLink); err != nil {
		return fmt.Errorf("while bringing up lo in interior netns: %w", err)
	}

	actorLink, err := netlink.LinkByName(actorVethTempName)
	if err != nil {
		return fmt.Errorf("while acquiring actor veth in interior netns: %w", err)
	}
	if err := netlink.LinkSetName(actorLink, actorVethName); err != nil {
		return fmt.Errorf("while renaming actor veth to %q: %w", actorVethName, err)
	}
	actorLink, err = netlink.LinkByName(actorVethName)
	if err != nil {
		return fmt.Errorf("while reacquiring actor veth in interior netns: %w", err)
	}

	actorAddr, err := parseAddr(actorVethCIDR)
	if err != nil {
		return err
	}
	if err := netlink.AddrReplace(actorLink, actorAddr); err != nil {
		return fmt.Errorf("while assigning actor veth address: %w", err)
	}
	if err := netlink.LinkSetUp(actorLink); err != nil {
		return fmt.Errorf("while bringing up actor veth: %w", err)
	}

	gw := net.ParseIP(actorVethGateway).To4()
	if gw == nil {
		return fmt.Errorf("invalid actor veth gateway %q", actorVethGateway)
	}
	if err := netlink.RouteReplace(&netlink.Route{
		LinkIndex: actorLink.Attrs().Index,
		Gw:        gw,
	}); err != nil {
		return fmt.Errorf("while installing actor default route: %w", err)
	}

	return nil
}

func (s *AteomService) cleanupActorNetwork(ctx context.Context) error {
	// Remove all per-activation network state owned by ateom. Deleting the
	// worker-side veth also deletes its peer when the pair is still connected,
	// but failed setup can leave the peer already moved into the actor netns.
	// For that reason cleanup also enters the interior netns and deletes either
	// the final actor interface name or the temporary peer name if present.
	//
	// This function is intentionally idempotent so it can run before setup, after
	// checkpoint, and from setup failure cleanup without requiring the caller to
	// know how far network initialization progressed.
	if err := removeActorNftablesRules(); err != nil {
		return err
	}

	if link, err := netlink.LinkByName(hostVethName); err == nil {
		if err := netlink.LinkDel(link); err != nil {
			return fmt.Errorf("while deleting host veth: %w", err)
		}
	} else if _, ok := err.(netlink.LinkNotFoundError); !ok {
		return fmt.Errorf("while looking up host veth: %w", err)
	}

	if err := netNSDo(ctx, s.interiorNetNS, func(_ context.Context) error {
		for _, name := range []string{actorVethName, actorVethTempName} {
			link, err := netlink.LinkByName(name)
			if err == nil {
				if err := netlink.LinkDel(link); err != nil {
					return fmt.Errorf("while deleting interior veth %q: %w", name, err)
				}
				continue
			}
			if _, ok := err.(netlink.LinkNotFoundError); !ok {
				return fmt.Errorf("while looking up interior veth %q: %w", name, err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("while cleaning interior netns links: %w", err)
	}

	return nil
}

func podIPv4() (net.IP, error) {
	// Resolve the worker pod IPv4 address from the pod namespace's real eth0.
	// Because eth0 now stays in the pod namespace, this IP remains available for
	// both normal worker connectivity and the temporary inbound DNAT rule.
	eth0Link, err := netlink.LinkByName("eth0")
	if err != nil {
		return nil, fmt.Errorf("while getting pod eth0: %w", err)
	}
	addrs, err := netlink.AddrList(eth0Link, netlink.FAMILY_V4)
	if err != nil {
		return nil, fmt.Errorf("while listing pod eth0 addresses: %w", err)
	}
	for _, addr := range addrs {
		if addr.IP == nil {
			continue
		}
		if ip := addr.IP.To4(); ip != nil {
			return ip, nil
		}
	}
	return nil, fmt.Errorf("pod eth0 has no IPv4 address")
}

func parseAddr(cidr string) (*netlink.Addr, error) {
	addr, err := netlink.ParseAddr(cidr)
	if err != nil {
		return nil, fmt.Errorf("while parsing address %q: %w", cidr, err)
	}
	return addr, nil
}

func enableIPv4Forwarding() error {
	// Forwarding is required because actor packets now enter the worker pod via
	// the host-side veth and then leave through the pod's eth0. Without this, the
	// kernel would not route traffic between those interfaces even though both
	// live in the worker pod network namespace.
	if err := os.WriteFile("/proc/sys/net/ipv4/ip_forward", []byte("1\n"), 0o644); err != nil {
		return fmt.Errorf("while enabling IPv4 forwarding in worker pod netns: %w", err)
	}
	return nil
}

func installActorNftablesRules(podIP net.IP) error {
	// Install a dedicated nftables table for the active actor. Keeping all
	// rules in an ateom-owned table makes cleanup simple and avoids mutating
	// Kubernetes or CNI-managed chains directly.
	//
	// The temporary compatibility rules do three things:
	//
	//   * postrouting: masquerade actor egress from 10.200.0.2 behind the worker
	//     pod IP so replies route back to the pod.
	//   * prerouting: DNAT traffic sent to the worker pod IP on TCP/80 to the
	//     actor veth IP on TCP/80, preserving existing inbound behavior.
	//   * forward: accept forwarded packets between the actor veth and pod eth0.
	//
	// This is not the final egress policy path. The later AgentGateway phase
	// should replace the broad masquerade path with transparent TCP capture and
	// default-deny rules.
	if err := removeActorNftablesRules(); err != nil {
		return err
	}

	c := &nftables.Conn{}
	table := &nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   actorNftTableName,
	}
	c.AddTable(table)

	prerouting := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	preroutingExprs := append(ipDestinationEqual(podIP.String()), tcpDestinationPortEqual(80)...)
	preroutingExprs = append(preroutingExprs,
		&expr.Immediate{
			Register: 1,
			Data:     net.ParseIP(actorVethIP).To4(),
		},
		&expr.Immediate{
			Register: 2,
			Data:     binaryutil.BigEndian.PutUint16(80),
		},
		&expr.NAT{
			Type:        expr.NATTypeDestNAT,
			Family:      unix.NFPROTO_IPV4,
			RegAddrMin:  1,
			RegProtoMin: 2,
		},
	)
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: prerouting,
		Exprs: preroutingExprs,
	})

	postrouting := c.AddChain(&nftables.Chain{
		Name:     "postrouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPostrouting,
		Priority: nftables.ChainPriorityNATSource,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: postrouting,
		Exprs: append(ipSourceEqual(actorVethIP), &expr.Masq{}),
	})

	acceptPolicy := nftables.ChainPolicyAccept
	forward := c.AddChain(&nftables.Chain{
		Name:     "forward",
		Table:    table,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookForward,
		Priority: nftables.ChainPriorityFilter,
		Policy:   &acceptPolicy,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: forward,
		Exprs: []expr.Any{
			&expr.Verdict{Kind: expr.VerdictAccept},
		},
	})

	if err := c.Flush(); err != nil {
		return fmt.Errorf("while installing actor nftables rules: %w", err)
	}
	return nil
}

func removeActorNftablesRules() error {
	// Delete the whole ateom nftables table if it exists. The table is
	// per-worker and currently per-active-actor because this worker path runs at
	// most one actor at a time. Missing tables are treated as already clean.
	c := &nftables.Conn{}
	tables, err := c.ListTablesOfFamily(nftables.TableFamilyIPv4)
	if err != nil {
		return fmt.Errorf("while listing nftables tables: %w", err)
	}
	for _, table := range tables {
		if table.Name != actorNftTableName {
			continue
		}
		c.DelTable(table)
		if err := c.Flush(); err != nil {
			return fmt.Errorf("while deleting actor nftables table: %w", err)
		}
		return nil
	}
	return nil
}

func ipSourceEqual(ip string) []expr.Any {
	return ipPayloadEqual(12, ip)
}

func ipDestinationEqual(ip string) []expr.Any {
	return ipPayloadEqual(16, ip)
}

func ipPayloadEqual(offset uint32, ip string) []expr.Any {
	return []expr.Any{
		&expr.Payload{
			DestRegister: 1,
			Base:         expr.PayloadBaseNetworkHeader,
			Offset:       offset,
			Len:          4,
		},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     net.ParseIP(ip).To4(),
		},
	}
}

func tcpDestinationPortEqual(port uint16) []expr.Any {
	return []expr.Any{
		&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     []byte{unix.IPPROTO_TCP},
		},
		&expr.Payload{
			DestRegister: 1,
			Base:         expr.PayloadBaseTransportHeader,
			Offset:       2,
			Len:          2,
		},
		&expr.Cmp{
			Op:       expr.CmpOpEq,
			Register: 1,
			Data:     binaryutil.BigEndian.PutUint16(port),
		},
	}
}

func createNetNSWithoutSwitching(ctx context.Context, name string) (netns.NsHandle, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return -1, fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	interiorNetNS, err := netns.NewNamed(name)
	if err != nil {
		return -1, fmt.Errorf("while creating interior network namespace for gVisor: %w", err)
	}

	return interiorNetNS, nil
}

func netNSDo(ctx context.Context, targetNS netns.NsHandle, do func(context.Context) error) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// We need to create the new NS, then switch back to the current netns.
	curNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting current netns: %w", err)
	}
	defer func() {
		if err := netns.Set(curNetNS); err != nil {
			// Better to blow up the program than continue execution with
			// one OS thread randomly in a different netns.
			panic(fmt.Sprintf("Failed to restore original netns: %v", err))
		}
	}()

	if err := netns.Set(targetNS); err != nil {
		return fmt.Errorf("setting target netns: %w", err)
	}

	if err := do(ctx); err != nil {
		return fmt.Errorf("while executing function in target netns: %w", err)
	}

	return nil
}

func dumpNetInfo(ctx context.Context, prefix string) error {
	links, err := netlink.LinkList()
	if err != nil {
		return fmt.Errorf("in netlink.LinkList(): %w", err)
	}

	for _, link := range links {
		slog.InfoContext(ctx, prefix+"Link", slog.String("name", link.Attrs().Name), slog.String("type", link.Type()), slog.Any("attrs", link.Attrs()))

		addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
		if err != nil {
			return fmt.Errorf("while getting pod eth0 addresses: %w", err)
		}
		slog.InfoContext(ctx, prefix+"Link Addresses", slog.String("link", link.Attrs().Name), slog.Any("addrs", addrs))

		rts, err := netlink.RouteList(link, netlink.FAMILY_V4)
		if err != nil {
			return fmt.Errorf("while getting routes off eth0: %w", err)
		}
		for _, rt := range rts {
			slog.InfoContext(ctx, prefix+"Link Routes", slog.Any("link", link.Attrs().Name), slog.Any("route", rt), slog.Any("route-string", rt.String()))
		}
	}

	return nil
}
