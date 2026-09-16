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

// Command ateom-kata implements Ateom and the minimal Kata runtime lifecycle
// required by a Worker Pod.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomcapacity"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"github.com/agent-substrate/substrate/internal/sandboxd"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/vishvananda/netns"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var (
	podUID                      = flag.String("pod-uid", "", "Worker Pod UID")
	sandboxdRoot                = flag.String("sandboxd-root", filepath.Join(ateompath.BasePath, "ateom-kata", "sandboxd"), "persistent Kata shim bundle root")
	atunnelListenAddress        = flag.String("atunnel-listen-address", "0.0.0.0:443", "Actor ingress HTTPS")
	atunnelConnectListenAddress = flag.String("atunnel-connect-listen-address", "0.0.0.0:444", "Actor ingress mTLS CONNECT")
	workerCredentialBundle      = flag.String("atunnel-credential-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Worker credential bundle")
	podIdentityTrustBundle      = flag.String("atunnel-trust-bundle", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "Router client trust bundle")
	atunnelClientIdentity       = flag.String("atunnel-client-identity", "spiffe://cluster.local/ns/ate-system/sa/atenet-router", "Allowed router SPIFFE identity")
	readinessListenAddress      = flag.String("readiness-listen-address", "0.0.0.0:8080", "Address for HTTP readiness checks")
)

const (
	kataRuntimeBundleAssetName = "ateom-kata-runtime-bundle"
	kataRuntimeConfigAssetName = "ateom-kata-runtime-config"
	rpcRunWorkload             = "RunWorkload"
	rpcRestoreWorkload         = "RestoreWorkload"
)

type activeRPCInfo struct {
	name   string
	cancel context.CancelFunc
}

// cancelableMutex keeps shutdown from waiting forever behind a wedged RPC.
type cancelableMutex struct {
	ch chan struct{}
}

func newCancelableMutex() *cancelableMutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return &cancelableMutex{ch: ch}
}

func (m *cancelableMutex) Lock() { <-m.ch }

func (m *cancelableMutex) Unlock() { m.ch <- struct{}{} }

func (m *cancelableMutex) LockContext(ctx context.Context) bool {
	select {
	case <-m.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		slog.Error("ateom-kata exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if *podUID == "" {
		return errors.New("--pod-uid is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ateomDir := ateompath.AteomPath(*podUID)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return err
	}
	socket := ateompath.AteomSocketPath(*podUID)
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	interiorNetNS, err := ateomnet.CreateNetNSWithoutSwitching(ateompath.AteomNetNSName(*podUID))
	if err != nil {
		return fmt.Errorf("create Kata actor netns: %w", err)
	}
	defer interiorNetNS.Close()
	network := &netNSActorNetwork{handle: interiorNetNS, path: ateompath.AteomNetNSPath(*podUID)}
	ingress, err := runKataIngress(ctx)
	if err != nil {
		return err
	}
	service := newServiceWithRuntimeFactory(newKataRuntime, network, ingress)
	server := grpc.NewServer()
	ateompb.RegisterAteomServer(server, service)
	reflection.Register(server)
	readiness := &serverboot.Readiness{}
	go serverboot.StartReadinessServer(ctx, *readinessListenAddress, readiness)
	go func() {
		err := ateomcapacity.Report(ctx, ateomcapacity.ReportConfig{
			SocketPath:           ateompath.AteomSupportSocket,
			CredentialBundlePath: *workerCredentialBundle,
			TrustBundlePath:      *podIdentityTrustBundle,
		})
		if err != nil && ctx.Err() == nil {
			slog.ErrorContext(ctx, "failed to report Kata worker capacity", "error", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig := <-sigCh
		slog.InfoContext(ctx, "received signal; draining Kata worker", "signal", sig.String())
		readiness.MarkNotReady()
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := service.gracefulShutdown(drainCtx); err != nil {
			slog.ErrorContext(ctx, "kata worker drain completed with errors", "error", err)
		}
		drainExpired := drainCtx.Err() != nil
		drainCancel()
		cancel()
		if drainExpired {
			server.Stop()
			return
		}
		server.GracefulStop()
	}()

	slog.InfoContext(ctx, "ateom-kata serving", "socket", socket, "runtime", "embedded-kata")
	return server.Serve(listener)
}

// runKataIngress terminates router mTLS in the Worker Pod and forwards to
// the active Kata VM through its actor veth. It deliberately has no egress
// listener: egress needs the separate actor certificate-broker lifecycle.
func runKataIngress(ctx context.Context) (*atunnel.Server, error) {
	upstream, err := url.Parse("http://" + net.JoinHostPort(ateomnet.ActorVethIP, "80"))
	if err != nil {
		return nil, err
	}
	ingress, err := atunnel.NewServer(atunnel.Config{CredentialBundlePath: *workerCredentialBundle, TrustBundlePath: *podIdentityTrustBundle, AllowedClientID: *atunnelClientIdentity, Upstream: upstream})
	if err != nil {
		return nil, fmt.Errorf("configure Kata ingress atunnel: %w", err)
	}
	httpsListener, err := net.Listen("tcp", *atunnelListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen Kata ingress HTTPS: %w", err)
	}
	connectListener, err := net.Listen("tcp", *atunnelConnectListenAddress)
	if err != nil {
		_ = httpsListener.Close()
		return nil, fmt.Errorf("listen Kata ingress CONNECT: %w", err)
	}
	serve := func(name string, fn func(context.Context, net.Listener) error, l net.Listener) {
		go func() {
			if err := fn(ctx, l); err != nil && ctx.Err() == nil {
				slog.Error("kata ingress listener stopped", "listener", name, "error", err)
				os.Exit(1)
			}
		}()
	}
	serve("https", ingress.Serve, httpsListener)
	serve("connect", ingress.ServeConnect, connectListener)
	return ingress, nil
}

type actorIngress interface {
	Activate(string, string) error
	Deactivate(context.Context) error
}

type runtimeClient interface {
	Run(context.Context, *sandboxd.Sandbox) error
	Checkpoint(context.Context, string, string, string, map[string]string) error
	Restore(context.Context, *sandboxd.Sandbox, string, string) error
	Delete(context.Context, string, []string) error
}

type runtimeClientFactory func(map[string]string) (runtimeClient, error)

var _ runtimeClient = (*sandboxd.Runtime)(nil)

type actorNetwork interface {
	Setup(context.Context) error
	Cleanup(context.Context) error
	Path() string
}

type netNSActorNetwork struct {
	handle netns.NsHandle
	path   string
}

func (n *netNSActorNetwork) Setup(ctx context.Context) error {
	return ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{InteriorNetNS: n.handle})
}

func (n *netNSActorNetwork) Cleanup(ctx context.Context) error {
	return ateomnet.CleanupActorNetwork(ctx, n.handle)
}

func (n *netNSActorNetwork) Path() string { return n.path }

type service struct {
	ateompb.UnimplementedAteomServer
	mu                *cancelableMutex
	client            runtimeClient
	runtimeFactory    runtimeClientFactory
	network           actorNetwork
	ingress           actorIngress
	runs              map[string]*sandboxd.Sandbox
	runClients        map[string]runtimeClient
	runtimeIdentities map[string]string
	waitReady         func(context.Context, []*ateompb.Container, string) error
	active            activeKataState
	draining          kataDrainState
	activeRPCMu       sync.Mutex
	activeRPC         *activeRPCInfo
}

func newService(client runtimeClient, network actorNetwork) *service {
	return &service{
		mu: newCancelableMutex(), client: client, network: network, runs: map[string]*sandboxd.Sandbox{},
		runClients: map[string]runtimeClient{}, runtimeIdentities: map[string]string{},
		waitReady: readyz.WaitAll,
	}
}

func newServiceWithRuntimeFactory(factory runtimeClientFactory, network actorNetwork, ingress actorIngress) *service {
	s := newService(nil, network)
	s.ingress = ingress
	s.runtimeFactory = factory
	return s
}

func newKataRuntime(paths map[string]string) (runtimeClient, error) {
	if _, err := validateKataRuntimeAssetPaths(paths); err != nil {
		return nil, err
	}
	shims, err := sandboxd.NewShimManager(sandboxd.ShimManagerConfig{
		Binary: paths["kata-shim"], Root: *sandboxdRoot, KataConfig: paths[kataRuntimeConfigAssetName],
	})
	if err != nil {
		return nil, err
	}
	tasks := sandboxd.NewTaskManager(shims)
	return sandboxd.NewRuntime(sandboxd.NewSandboxController(shims, tasks), tasks), nil
}

func validateKataRuntimeAssetPaths(paths map[string]string) (string, error) {
	if (paths["kata-qemu"] == "") == (paths["kata-dragonball"] == "") {
		return "", errors.New("runtime_asset_paths must contain exactly one of kata-qemu or kata-dragonball")
	}
	vmmKey := "kata-qemu"
	if paths["kata-dragonball"] != "" {
		vmmKey = "kata-dragonball"
	}
	required := []string{kataRuntimeBundleAssetName, kataRuntimeConfigAssetName, "kata-shim", vmmKey, "kata-kernel"}
	for _, name := range required {
		if paths[name] == "" {
			return "", fmt.Errorf("runtime_asset_paths is missing %q", name)
		}
	}
	if (paths["kata-image"] == "") == (paths["kata-initrd"] == "") {
		return "", errors.New("runtime_asset_paths must contain exactly one of kata-image or kata-initrd")
	}
	bundle := filepath.Clean(paths[kataRuntimeBundleAssetName])
	if !filepath.IsAbs(bundle) {
		return "", errors.New("kata runtime bundle path must be absolute")
	}
	bundleInfo, err := os.Lstat(bundle)
	if err != nil || !bundleInfo.IsDir() {
		return "", fmt.Errorf("kata runtime bundle is missing or not a directory: %v", err)
	}
	for name, path := range paths {
		if name == kataRuntimeBundleAssetName {
			continue
		}
		if !filepath.IsAbs(path) {
			return "", fmt.Errorf("runtime asset %q path must be absolute", name)
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return "", fmt.Errorf("runtime asset %q is missing or not regular: %v", name, err)
		}
		if name != kataRuntimeConfigAssetName {
			rel, err := filepath.Rel(bundle, path)
			if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
				return "", fmt.Errorf("runtime asset %q is outside the verified bundle", name)
			}
		}
	}
	for _, executable := range []string{"kata-shim", vmmKey} {
		info, _ := os.Stat(paths[executable])
		if info.Mode().Perm()&0o111 == 0 {
			return "", fmt.Errorf("runtime asset %q is not executable", executable)
		}
	}
	config, err := os.ReadFile(paths[kataRuntimeConfigAssetName])
	if err != nil {
		return "", fmt.Errorf("read Kata runtime config: %w", err)
	}
	configText := string(config)
	if strings.Contains(configText, "/etc/kata-containers/") {
		return "", errors.New("kata runtime config references a forbidden host default path")
	}
	for _, name := range []string{"kata-kernel", "kata-image", "kata-initrd"} {
		if path := paths[name]; path != "" && !strings.Contains(configText, path) {
			return "", fmt.Errorf("kata runtime config does not reference verified asset %q", name)
		}
	}
	sharedFSCount := 0
	passFDZero := false
	for _, raw := range strings.Split(configText, "\n") {
		line := strings.TrimSpace(strings.SplitN(raw, "#", 2)[0])
		if strings.HasPrefix(line, "shared_fs") && strings.Contains(line, "=") {
			sharedFSCount++
			if line != `shared_fs = "none"` && line != `shared_fs="none"` {
				return "", fmt.Errorf("kata runtime config has unsupported %q", line)
			}
		}
		if strings.HasPrefix(line, "passfd_listener_port") && strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			passFDZero = strings.TrimSpace(parts[1]) == "0"
		}
	}
	if sharedFSCount != 1 {
		return "", fmt.Errorf("kata runtime config has %d active shared_fs entries, require exactly one", sharedFSCount)
	}
	if !passFDZero {
		return "", errors.New("kata runtime config must set passfd_listener_port = 0")
	}
	keys := make([]string, 0, len(paths))
	for key := range paths {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		_, _ = io.WriteString(h, key+"\x00"+filepath.Clean(paths[key])+"\x00")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *service) runtimeForActivation(paths map[string]string) (runtimeClient, string, error) {
	if s.runtimeFactory == nil {
		return s.client, "", nil
	}
	identity, err := validateKataRuntimeAssetPaths(paths)
	if err != nil {
		return nil, "", err
	}
	client, err := s.runtimeFactory(paths)
	return client, identity, err
}

func (s *service) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (_ *ateompb.RunWorkloadResponse, retErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRunWorkload, cancel)
	defer s.clearActiveRPC()
	ns, tmpl, id := req.GetActorTemplateAtespace(), req.GetActorTemplateName(), req.GetActorUid()
	key := actorKey(ns, tmpl, id)
	if err := s.rejectIfActorRunning(key); err != nil {
		return nil, err
	}
	client, runtimeIdentity, err := s.runtimeForActivation(req.GetRuntimeAssetPaths())
	if err != nil {
		return nil, fmt.Errorf("validate Kata runtime assets: %w", err)
	}
	sb, err := buildSandbox(ns, tmpl, id, req.GetSpec(), false, s.network.Path())
	if err != nil {
		return nil, err
	}
	if err := s.network.Setup(ctx); err != nil {
		return nil, fmt.Errorf("setup Kata actor netns: %w", err)
	}
	runtimeStarted := false
	defer func() {
		if retErr != nil {
			cleanupCtx := context.WithoutCancel(ctx)
			if runtimeStarted {
				if err := s.deleteRuntime(cleanupCtx, key, sb, client); err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("rollback Kata sandbox after Run failure: %w", err))
				}
			}
			if s.ingress != nil && req.GetAtespace() != "" {
				_ = s.ingress.Deactivate(cleanupCtx)
			}
			if err := s.network.Cleanup(cleanupCtx); err != nil {
				slog.WarnContext(cleanupCtx, "kata actor netns cleanup failed after Run failure", "error", err)
			}
		}
	}()
	if err := setupBundleRootfs(sb); err != nil {
		return nil, err
	}
	defer func() {
		if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(id)); err != nil {
			slog.WarnContext(ctx, "bundle rootfs cleanup failed", "error", err)
		}
	}()
	if err := prepareSandboxIO(sb); err != nil {
		return nil, err
	}
	if err := client.Run(ctx, sb); err != nil {
		return nil, err
	}
	runtimeStarted = true
	s.rememberRun(key, sb, client, runtimeIdentity)
	s.setActive(req.GetAtespace(), req.GetActorName(), id, ns, tmpl, key, sb, client)
	if err := s.waitReady(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP); err != nil {
		return nil, fmt.Errorf("wait for Kata actor readiness: %w", err)
	}
	if s.ingress != nil && req.GetAtespace() != "" {
		if err := s.ingress.Activate(req.GetAtespace(), req.GetActorName()); err != nil {
			return nil, fmt.Errorf("activate Kata actor ingress: %w", err)
		}
	}
	return &ateompb.RunWorkloadResponse{}, nil
}

func (s *service) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns, tmpl, id := req.GetActorTemplateAtespace(), req.GetActorTemplateName(), req.GetActorUid()
	key := actorKey(ns, tmpl, id)
	sb := s.runs[key]
	if sb == nil {
		return nil, fmt.Errorf("actor %q is not running", key)
	}
	if err := sandboxd.ValidateOperationID(req.GetOperationId()); err != nil {
		return nil, fmt.Errorf("invalid operation ID: %w", err)
	}
	client := s.runClients[key]
	if client == nil {
		client = s.client
	}
	if s.runtimeFactory != nil {
		identity, err := validateKataRuntimeAssetPaths(req.GetRuntimeAssetPaths())
		if err != nil {
			return nil, fmt.Errorf("validate Kata runtime assets: %w", err)
		}
		if identity != s.runtimeIdentities[key] {
			return nil, errors.New("checkpoint runtime assets differ from the active actor runtime")
		}
	}
	inventory := make(map[string]string, len(sb.Tasks))
	for _, task := range sb.Tasks {
		inventory[task.CheckpointKey] = task.TaskID
	}
	if err := client.Checkpoint(ctx, sb.SandboxID, ateompath.CheckpointStateDir(id), req.GetOperationId(), inventory); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(sb.Tasks))
	for _, task := range sb.Tasks {
		ids = append(ids, task.TaskID)
	}
	if err := client.Delete(context.WithoutCancel(ctx), sb.SandboxID, ids); err != nil {
		return nil, fmt.Errorf("checkpoint succeeded but cleanup failed: %w", err)
	}
	if s.ingress != nil && req.GetAtespace() != "" {
		if err := s.ingress.Deactivate(context.WithoutCancel(ctx)); err != nil {
			slog.WarnContext(ctx, "deactivate Kata actor ingress", "error", err)
		}
	}
	// A successful Delete means the actor is no longer running even if
	// inspecting the checkpoint output below fails. Do not retain stale state.
	delete(s.runs, key)
	delete(s.runClients, key)
	delete(s.runtimeIdentities, key)
	s.clearActive(key)
	if err := s.network.Cleanup(context.WithoutCancel(ctx)); err != nil {
		return nil, fmt.Errorf("checkpoint succeeded but actor netns cleanup failed: %w", err)
	}
	files, err := checkpointFiles(ateompath.CheckpointStateDir(id))
	if err != nil {
		return nil, err
	}
	compatibility, err := sandboxd.CheckpointCompatibility(ateompath.CheckpointStateDir(id))
	if err != nil {
		return nil, err
	}
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: files, RuntimeCompatibility: &ateompb.RuntimeCompatibility{Profile: compatibility.Profile, RuntimeFingerprint: compatibility.Fingerprint}}, nil
}

func (s *service) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (_ *ateompb.RestoreWorkloadResponse, retErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRestoreWorkload, cancel)
	defer s.clearActiveRPC()
	ns, tmpl, id := req.GetActorTemplateAtespace(), req.GetActorTemplateName(), req.GetActorUid()
	key := actorKey(ns, tmpl, id)
	if err := s.rejectIfActorRunning(key); err != nil {
		return nil, err
	}
	if err := sandboxd.ValidateOperationID(req.GetOperationId()); err != nil {
		return nil, fmt.Errorf("invalid operation ID: %w", err)
	}
	if expected := req.GetRuntimeCompatibility(); expected != nil {
		actual, err := sandboxd.CheckpointCompatibility(ateompath.RestoreStateDir(id))
		if err != nil {
			return nil, err
		}
		if expected.GetProfile() != actual.Profile || expected.GetRuntimeFingerprint() != actual.Fingerprint {
			return nil, errors.New("transport compatibility does not match Kata checkpoint manifest")
		}
	}
	client, runtimeIdentity, err := s.runtimeForActivation(req.GetRuntimeAssetPaths())
	if err != nil {
		return nil, fmt.Errorf("validate Kata runtime assets: %w", err)
	}
	sb, err := buildSandbox(ns, tmpl, id, req.GetSpec(), true, s.network.Path())
	if err != nil {
		return nil, err
	}
	if err := s.network.Setup(ctx); err != nil {
		return nil, fmt.Errorf("setup Kata actor netns for restore: %w", err)
	}
	runtimeStarted := false
	defer func() {
		if retErr != nil {
			cleanupCtx := context.WithoutCancel(ctx)
			if runtimeStarted {
				if err := s.deleteRuntime(cleanupCtx, key, sb, client); err != nil {
					retErr = errors.Join(retErr, fmt.Errorf("rollback Kata sandbox after Restore failure: %w", err))
				}
			}
			if s.ingress != nil && req.GetAtespace() != "" {
				_ = s.ingress.Deactivate(cleanupCtx)
			}
			if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(id)); err != nil {
				slog.WarnContext(cleanupCtx, "bundle rootfs cleanup failed after Restore failure", "error", err)
			}
			if err := s.network.Cleanup(cleanupCtx); err != nil {
				slog.WarnContext(cleanupCtx, "kata actor netns cleanup failed after Restore failure", "error", err)
			}
		}
	}()
	if err := makeCheckpointReadOnly(ateompath.RestoreStateDir(id)); err != nil {
		return nil, err
	}
	if err := prepareSandboxIO(sb); err != nil {
		return nil, err
	}
	if err := client.Restore(ctx, sb, ateompath.RestoreStateDir(id), req.GetOperationId()); err != nil {
		return nil, err
	}
	runtimeStarted = true
	s.rememberRun(key, sb, client, runtimeIdentity)
	s.setActive(req.GetAtespace(), req.GetActorName(), id, ns, tmpl, key, sb, client)
	if err := s.waitReady(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP); err != nil {
		return nil, fmt.Errorf("wait for restored Kata actor readiness: %w", err)
	}
	if s.ingress != nil && req.GetAtespace() != "" {
		if err := s.ingress.Activate(req.GetAtespace(), req.GetActorName()); err != nil {
			return nil, fmt.Errorf("activate restored Kata actor ingress: %w", err)
		}
	}
	return &ateompb.RestoreWorkloadResponse{}, nil
}

func (s *service) TerminateWorkload(ctx context.Context, req *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := actorKey(req.GetActorTemplateAtespace(), req.GetActorTemplateName(), req.GetActorUid())
	sb := s.runs[key]
	if sb == nil {
		if err := s.rejectIfActorRunning(key); err != nil {
			return nil, err
		}
		return &ateompb.TerminateWorkloadResponse{}, nil
	}
	client := s.runClients[key]
	if client == nil {
		client = s.client
	}
	cleanupCtx := context.WithoutCancel(ctx)
	if err := s.deleteRuntime(cleanupCtx, key, sb, client); err != nil {
		return nil, fmt.Errorf("delete Kata sandbox: %w", err)
	}

	var errs []error
	if s.ingress != nil && req.GetAtespace() != "" {
		if err := s.ingress.Deactivate(cleanupCtx); err != nil {
			errs = append(errs, fmt.Errorf("deactivate Kata actor ingress: %w", err))
		}
	}
	if err := imagecache.UnmountAllUnder(ateompath.OCIBundleDir(req.GetActorUid())); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("unmount Kata actor bundles: %w", err))
	}
	if err := s.network.Cleanup(cleanupCtx); err != nil {
		errs = append(errs, fmt.Errorf("clean up Kata actor netns: %w", err))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return &ateompb.TerminateWorkloadResponse{}, nil
}

func (s *service) rememberRun(key string, sb *sandboxd.Sandbox, client runtimeClient, runtimeIdentity string) {
	s.runs[key] = sb
	s.runClients[key] = client
	s.runtimeIdentities[key] = runtimeIdentity
}

func (s *service) deleteRuntime(ctx context.Context, key string, sb *sandboxd.Sandbox, client runtimeClient) error {
	ids := make([]string, 0, len(sb.Tasks))
	for _, task := range sb.Tasks {
		ids = append(ids, task.TaskID)
	}
	if err := client.Delete(ctx, sb.SandboxID, ids); err != nil {
		return err
	}
	delete(s.runs, key)
	delete(s.runClients, key)
	delete(s.runtimeIdentities, key)
	s.clearActive(key)
	return nil
}

func actorKey(namespace, template, actorID string) string {
	return namespace + ":" + template + ":" + actorID
}

// A Worker owns one interior netns and therefore runs at most one Actor.
// Reject before SetupActorNetwork, which deliberately removes stale links.
func (s *service) rejectIfActorRunning(requested string) error {
	for active := range s.runs {
		return fmt.Errorf("cannot activate actor %q while actor %q is running", requested, active)
	}
	return nil
}

func buildSandbox(namespace, template, actorID string, spec *ateompb.WorkloadSpec, restoring bool, netNSPath string) (*sandboxd.Sandbox, error) {
	if actorID == "" || spec == nil || len(spec.GetContainers()) == 0 || !filepath.IsAbs(netNSPath) {
		return nil, errors.New("actor UID, containers and absolute netns path are required")
	}
	digest := sha256.Sum256([]byte(namespace + "\x00" + template + "\x00" + actorID))
	runtimeID := "ate-" + hex.EncodeToString(digest[:16])
	if restoring {
		// Kata RestoreSandbox supports rebinding checkpoint keys to new runtime
		// IDs. Never adopt a checkpoint under its source sandbox/task IDs.
		runtimeID += "-r"
	}
	config, err := proto.Marshal(&runtimev1.PodSandboxConfig{
		Metadata:    &runtimev1.PodSandboxMetadata{Name: template, Uid: actorID, Namespace: namespace},
		Hostname:    actorID,
		Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": runtimeID},
	})
	if err != nil {
		return nil, err
	}
	// runtime-rs discovers eth0 in this actor-owned interior netns and creates
	// the VM endpoint. The Worker Pod's CNI netns remains owned by Substrate.
	sb := &sandboxd.Sandbox{SandboxID: runtimeID, NetNSPath: netNSPath, PodSandboxConfig: config}
	for _, container := range spec.GetContainers() {
		bundle := ateompath.OCIBundlePath(actorID, container.GetName())
		rootfs := filepath.Join(bundle, "rootfs.raw")
		if restoring {
			rootfs = ""
		}
		sb.Tasks = append(sb.Tasks, &sandboxd.Task{
			CheckpointKey: container.GetName(), TaskID: runtimeID + "-" + container.GetName(), Bundle: bundle,
			Stdout: filepath.Join(bundle, "stdout.fifo"), Stderr: filepath.Join(bundle, "stderr.fifo"),
			RootfsFile: rootfs, RootfsFSType: "ext4", RootfsOptions: []string{"rw", "loop"},
		})
	}
	return sb, nil
}

func setupBundleRootfs(sb *sandboxd.Sandbox) error {
	for _, task := range sb.Tasks {
		if err := imagecache.SetupBundleRootfs(task.Bundle); err != nil {
			_ = imagecache.UnmountAllUnder(filepath.Dir(task.Bundle))
			return fmt.Errorf("setup rootfs for task %q: %w", task.TaskID, err)
		}
		if err := ensureKataLinuxResources(task.Bundle); err != nil {
			_ = imagecache.UnmountAllUnder(filepath.Dir(task.Bundle))
			return fmt.Errorf("set Kata task resources for %q: %w", task.TaskID, err)
		}
	}
	return nil
}

// ensureKataLinuxResources supplies a conservative CPU share only when the
// image OCI spec does not declare Linux resources. runtime-rs requires a
// non-empty resource object during task creation; explicit image resources
// remain authoritative.
func ensureKataLinuxResources(bundle string) error {
	path := filepath.Join(bundle, "config.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var spec map[string]any
	if err := json.Unmarshal(b, &spec); err != nil {
		return err
	}
	linux, ok := spec["linux"].(map[string]any)
	if !ok {
		return errors.New("OCI spec has no linux section")
	}

	// The Kata sandbox already owns the actor netns. Passing the same network
	// namespace again on a task asks the guest runtime to join a host namespace,
	// which it cannot do. This transformation is Kata-local; the shared OCI
	// builder remains unchanged for gVisor and microVM.
	changed := false
	if namespaces, ok := linux["namespaces"].([]any); ok {
		filtered := namespaces[:0]
		for _, namespace := range namespaces {
			entry, _ := namespace.(map[string]any)
			if entry["type"] == "network" {
				changed = true
				continue
			}
			filtered = append(filtered, namespace)
		}
		linux["namespaces"] = filtered
	}
	if resources, ok := linux["resources"]; !ok || resources == nil {
		linux["resources"] = map[string]any{"cpu": map[string]any{"shares": 1024}}
		changed = true
	}
	if !changed {
		return nil
	}
	out, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, info.Mode().Perm())
}

func checkpointFiles(root string) ([]string, error) {
	if !filepath.IsAbs(root) {
		return nil, errors.New("checkpoint root must be absolute")
	}
	var files []string
	err := filepath.WalkDir(root, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported checkpoint entry %q", filePath)
		}
		rel, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." || rel == "" || rel == ".." || len(rel) >= 3 && rel[:3] == "../" {
			return fmt.Errorf("checkpoint path escapes root: %q", rel)
		}
		files = append(files, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

func makeCheckpointReadOnly(root string) error {
	return filepath.WalkDir(root, func(filePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported restore checkpoint entry %q", filePath)
		}
		if err := os.Chmod(filePath, 0o400); err != nil {
			return fmt.Errorf("make restore checkpoint %q read-only: %w", filePath, err)
		}
		return nil
	})
}

func prepareSandboxIO(sb *sandboxd.Sandbox) error {
	for _, task := range sb.Tasks {
		for _, stream := range []struct {
			fifo, log string
			writer    io.Writer
		}{
			{task.Stdout, filepath.Join(task.Bundle, "stdout.log"), os.Stdout},
			{task.Stderr, filepath.Join(task.Bundle, "stderr.log"), os.Stderr},
		} {
			if info, err := os.Lstat(stream.fifo); errors.Is(err, os.ErrNotExist) {
				if err := syscall.Mkfifo(stream.fifo, 0o600); err != nil {
					return fmt.Errorf("create task FIFO %q: %w", stream.fifo, err)
				}
			} else if err != nil {
				return err
			} else if info.Mode()&os.ModeNamedPipe == 0 {
				return fmt.Errorf("task IO path %q is not a FIFO", stream.fifo)
			}
			go consumeTaskIO(stream.fifo, stream.log, stream.writer)
		}
	}
	return nil
}

func consumeTaskIO(fifoPath, logPath string, writer io.Writer) {
	fifo, err := os.Open(fifoPath)
	if err != nil {
		slog.Error("open task FIFO", "path", fifoPath, "error", err)
		return
	}
	defer fifo.Close()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		slog.Error("open task log", "path", logPath, "error", err)
		return
	}
	defer logFile.Close()
	if _, err := io.Copy(io.MultiWriter(writer, logFile), fifo); err != nil {
		slog.Error("copy task IO", "path", fifoPath, "error", err)
	}
}
