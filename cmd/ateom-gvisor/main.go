//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"

	"cloud.google.com/go/compute/metadata"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/hashicorp/go-reap"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	podNamespace = flag.String("pod-namespace", "", "The namespace of the current pod")
	podName      = flag.String("pod-name", "", "The name of the current pod")

	reapLock sync.RWMutex
)

func main() {
	flag.Parse()
	ctx := context.Background()

	if err := do(ctx); err != nil {
		slog.ErrorContext(ctx, "Error while executing", slog.Any("err", err))
		os.Exit(1)
	}
}

func do(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(os.Stdout, nil)))
	slog.SetDefault(logger)

	slog.InfoContext(ctx, "ateom booting")

	tp, err := initTracing(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize tracing", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			slog.Error("Failed to shutdown TracerProvider", slog.Any("err", err))
		}
	}()

	// Create ateom dir
	ateomDir := ateompath.AteomPath(*podNamespace, *podName)
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
	sockPath := ateompath.AteomSocketPath(*podNamespace, *podName)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	// On first start, scrape from eth0 interface.
	//
	// TODO(ateom): Save to boltdb database or file under ateom folder, so that
	// if ateom process restarts, we still have it.
	eth0Link, err := netlink.LinkByName("eth0")
	if err != nil {
		return fmt.Errorf("while getting netlink link for eth0: %w", err)
	}

	eth0LinkInfo, err := scrapeLink(eth0Link)
	if err != nil {
		return fmt.Errorf("while scraping info from eth0: %w", err)
	}
	slog.InfoContext(ctx, "Eth0 link info", slog.Any("eth0", eth0LinkInfo))

	// Create a new network namespace that we will pass to gVisor.  gVisor will
	// read the addresses and routes off of every link in the namespace, then
	// remove all the addresses and handle injecting packets into the interfaces
	// using AF_PACKET.
	interiorNetNS, err := createNetNSWithoutSwitching(ctx, ateompath.AteomNetNSName(*podNamespace, *podName))
	if err != nil {
		return fmt.Errorf("while creating ateom-interior netns: %w", err)
	}

	actorLogger := NewActorLogger(logger, metadata.OnGCE())
	ateomService := NewService(interiorNetNS, eth0LinkInfo, actorLogger)

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

func initTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("ateom-gvisor"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// No exporter, since ateom has no network connectivity once eth0 is sent
	// into the gvisor netns.  Maybe we can eventually figure out export via
	// UDS.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		// Only trace on-demand when signaled by the client (e.g. via --trace flag)
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

// AteomService is a service for shepherding single microvm.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// Let's go ahead and assume that Ateom RPCs that are running `runsc`
	// subcommands are probably not safe to call concurrently.
	lock sync.Mutex

	interiorNetNS netns.NsHandle
	eth0LinkInfo  *SaveLinkInfo
	actorLogger   *ActorLogger
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(interiorNetNS netns.NsHandle, eth0LinkInfo *SaveLinkInfo, actorLogger *ActorLogger) *AteomService {
	svc := &AteomService{
		interiorNetNS: interiorNetNS,
		eth0LinkInfo:  eth0LinkInfo,
		actorLogger:   actorLogger,
	}
	return svc
}

// ensureEth0InPodNetns moves eth0 back to the pod netns if a prior
// Run/Restore left it stuck in the interior netns. Idempotent: returns
// nil if eth0 is already in the pod netns or absent from both.
func ensureEth0InPodNetns(ctx context.Context, s *AteomService) error {
	if _, err := netlink.LinkByName("eth0"); err == nil {
		return nil
	}
	podNetNS, err := netns.Get()
	if err != nil {
		return fmt.Errorf("while getting pod netns: %w", err)
	}
	var moved bool
	err = netNSDo(ctx, s.interiorNetNS, func(_ context.Context) error {
		link, lookupErr := netlink.LinkByName("eth0")
		if lookupErr != nil {
			return nil
		}
		if mvErr := netlink.LinkSetNsFd(link, int(podNetNS)); mvErr != nil {
			return fmt.Errorf("while moving eth0 to pod netns: %w", mvErr)
		}
		moved = true
		return nil
	})
	if moved {
		slog.WarnContext(ctx, "Recovered eth0 from interior netns to pod netns")
	}
	return err
}

func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.actorLogger.EmitLifecycleLog("Actor starting", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	// Contract with atelet:
	//
	//   * Correct runsc version is downloaded and placed on disk.
	//   * All OCI bundles are set up, including for "pause" container.

	if err := ensureEth0InPodNetns(ctx, s); err != nil {
		return nil, fmt.Errorf("while recovering eth0 from prior failure: %w", err)
	}

	// Move pod eth0 into interior netns
	eth0Link, err := netlink.LinkByName("eth0")
	if err != nil {
		return nil, fmt.Errorf("while getting netlink link for eth0: %w", err)
	}
	if err := netlink.LinkSetNsFd(eth0Link, int(s.interiorNetNS)); err != nil {
		return nil, fmt.Errorf("while moving eth0 into interior network namespace: %w", err)
	}
	// Roll eth0 back to the pod netns if any subsequent step errors,
	// otherwise the worker pod is bricked for the next actor.
	defer func() {
		if retErr != nil {
			if cleanupErr := ensureEth0InPodNetns(ctx, s); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to roll back eth0 after Run failure", "err", cleanupErr)
			}
		}
	}()

	slog.InfoContext(ctx, "Restoring eth0 routes/addresses in interior netns")
	err = netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		loLink, err := netlink.LinkByName("lo")
		if err != nil {
			return fmt.Errorf("while acquiring lo in interior netns: %w", err)
		}
		if err := netlink.LinkSetUp(loLink); err != nil {
			return fmt.Errorf("while bringing up lo in interior netns: %w", err)
		}

		eth0Link, err := netlink.LinkByName("eth0")
		if err != nil {
			return fmt.Errorf("while acquiring eth0 in interior netns: %w", err)
		}
		if err := netlink.LinkSetUp(eth0Link); err != nil {
			return fmt.Errorf("while bringing up eth0 in interior netns: %w", err)
		}
		if err := restoreLink(ctx, eth0Link, s.eth0LinkInfo); err != nil {
			return fmt.Errorf("while restoring eth0 routes and addresses in interior netns: %w", err)
		}
		if err := dumpNetInfo(ctx, "Interior NetNS "); err != nil {
			return fmt.Errorf("while dumping links of interior netns: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while restoring eth0 in interior netns: %w", err)
	}

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

	checkpointPath := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
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

	// Yoink eth0 back to the pod netns.
	podNetNS, err := netns.Get()
	if err != nil {
		return nil, fmt.Errorf("while getting pod netns: %w", err)
	}
	err = netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		eth0Link, err := netlink.LinkByName("eth0")
		if err != nil {
			return fmt.Errorf("while acquiring eth0 in interior netns: %w", err)
		}
		if err := netlink.LinkSetNsFd(eth0Link, int(podNetNS)); err != nil {
			return fmt.Errorf("while sending eth0 back to pod netns: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while restoring eth0 in interior netns: %w", err)
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

	if err := ensureEth0InPodNetns(ctx, s); err != nil {
		return nil, fmt.Errorf("while recovering eth0 from prior failure: %w", err)
	}

	// Move pod eth0 into interior netns
	eth0Link, err := netlink.LinkByName("eth0")
	if err != nil {
		return nil, fmt.Errorf("while getting netlink link for eth0: %w", err)
	}
	if err := netlink.LinkSetNsFd(eth0Link, int(s.interiorNetNS)); err != nil {
		return nil, fmt.Errorf("while moving eth0 into interior network namespace: %w", err)
	}
	// Roll eth0 back to the pod netns if any subsequent step errors,
	// otherwise the worker pod is bricked for the next actor.
	defer func() {
		if retErr != nil {
			if cleanupErr := ensureEth0InPodNetns(ctx, s); cleanupErr != nil {
				slog.WarnContext(ctx, "Failed to roll back eth0 after Restore failure", "err", cleanupErr)
			}
		}
	}()

	// Restore route and IP information from save onto eth0.
	slog.InfoContext(ctx, "Restoring eth0 routes/addresses in interior netns")
	err = netNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		loLink, err := netlink.LinkByName("lo")
		if err != nil {
			return fmt.Errorf("while acquiring lo in interior netns: %w", err)
		}
		if err := netlink.LinkSetUp(loLink); err != nil {
			return fmt.Errorf("while bringing up lo in interior netns: %w", err)
		}

		eth0Link, err := netlink.LinkByName("eth0")
		if err != nil {
			return fmt.Errorf("while acquiring eth0 in interior netns: %w", err)
		}
		if err := netlink.LinkSetUp(eth0Link); err != nil {
			return fmt.Errorf("while bringing up eth0 in interior netns: %w", err)
		}
		if err := restoreLink(ctx, eth0Link, s.eth0LinkInfo); err != nil {
			return fmt.Errorf("while restoring eth0 routes and addresses in interior netns: %w", err)
		}

		if err := dumpNetInfo(ctx, "Interior NetNS "); err != nil {
			return fmt.Errorf("while dumping links of interior netns: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while restoring eth0 in interior netns: %w", err)
	}

	rcmd := &runsc{
		path:                   req.GetRunscPath(),
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	checkpointDir := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())

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

type SaveLinkInfo struct {
	Addresses []SaveAddr
	Routes    []SaveRoute
}

type SaveAddr struct {
	Addr      net.IPNet
	Scope     int
	Broadcast net.IP
}

type SaveRoute struct {
	Scope    uint8
	Dst      net.IPNet
	Src      net.IP
	Gateway  net.IP
	Protocol int
	Type     int
}

func scrapeLink(link netlink.Link) (*SaveLinkInfo, error) {
	rawAddrs, err := netlink.AddrList(link, netlink.FAMILY_ALL)
	if err != nil {
		return nil, fmt.Errorf("while scraping addresses: %w", err)
	}

	var addrs []SaveAddr
	for _, rawAddr := range rawAddrs {
		addr := SaveAddr{
			Addr:      *rawAddr.IPNet,
			Scope:     rawAddr.Scope,
			Broadcast: rawAddr.Broadcast,
		}
		addrs = append(addrs, addr)
	}

	rawRoutes, err := netlink.RouteList(link, netlink.FAMILY_ALL)
	if err != nil {
		return nil, fmt.Errorf("while scraping routes: %w", err)
	}

	var routes []SaveRoute
	for _, rawRoute := range rawRoutes {
		route := SaveRoute{
			Scope:    uint8(rawRoute.Scope),
			Dst:      *rawRoute.Dst,
			Src:      rawRoute.Src,
			Gateway:  rawRoute.Gw,
			Protocol: int(rawRoute.Protocol),
			Type:     rawRoute.Type,
		}
		routes = append(routes, route)
	}

	return &SaveLinkInfo{
		Addresses: addrs,
		Routes:    routes,
	}, nil
}

func restoreLink(ctx context.Context, link netlink.Link, info *SaveLinkInfo) error {
	for i, saveAddr := range info.Addresses {
		addr := &netlink.Addr{
			IPNet:     &saveAddr.Addr,
			Scope:     saveAddr.Scope,
			Broadcast: saveAddr.Broadcast,
		}
		if err := netlink.AddrReplace(link, addr); err != nil {
			return fmt.Errorf("while restoring addr %d onto link: %w", i, err)
		}
	}
	// Link-scope routes must be installed before gateway routes so the
	// kernel can resolve each gateway's nexthop (fib_check_nh_v4_gw).
	routes := append([]SaveRoute(nil), info.Routes...)
	sort.SliceStable(routes, func(i, j int) bool {
		return routes[i].Gateway == nil && routes[j].Gateway != nil
	})
	for i, saveRoute := range routes {
		route := &netlink.Route{
			LinkIndex: link.Attrs().Index,
			Scope:     netlink.Scope(saveRoute.Scope),
			Dst:       &saveRoute.Dst,
			Src:       saveRoute.Src,
			Gw:        saveRoute.Gateway,
			Protocol:  netlink.RouteProtocol(saveRoute.Protocol),
			Type:      saveRoute.Type,
		}
		slog.InfoContext(ctx, "Restoring route", slog.String("dst", saveRoute.Dst.String()), slog.String("src", saveRoute.Src.String()), slog.String("gateway", saveRoute.Gateway.String()))
		if err := netlink.RouteReplace(route); err != nil {
			return fmt.Errorf("while restoring route %d: %w", i, err)
		}
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
