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

// Command ateom-microvm is the kata + cloud-hypervisor micro-VM
// implementation of the ateompb.Ateom service, a peer to cmd/ateom-gvisor.
//
// It runs a substrate actor as a cloud-hypervisor micro-VM (launched via the
// kata guest model) and supports full suspend/resume by driving CH's native
// snapshot/restore underneath (see internal/ch).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/hashicorp/go-reap"
	"github.com/vishvananda/netns"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	podUID = flag.String("pod-uid", "", "The UID of the current pod")

	shimBinary = flag.String("shim-binary", "containerd-shim-kata-v2", "Path to the kata containerd shim v2 binary.")

	chBinary = flag.String("cloud-hypervisor-binary", "cloud-hypervisor", "Path to the cloud-hypervisor binary (used to relaunch on restore).")

	virtiofsdBinary = flag.String("virtiofsd-binary", "virtiofsd", "Path to the virtiofsd binary (needs find-paths migration support, >= 1.13; used on restore).")

	kataConfig = flag.String("kata-config", "", "Path to a kata configuration.toml (passed to the shim as KATA_CONF_FILE). Empty uses kata's default. atelet generates one pointing at runtime-fetched assets.")

	kataNamespace = flag.String("kata-namespace", "default", "containerd namespace the kata shim runs under.")

	kataDebug = flag.Bool("kata-debug", false, "Verbose kata debugging: shim -debug, hypervisor/agent/runtime enable_debug, and guest console (incl. kata-agent logs) forwarded into pod logs.")

	showVersion = flag.Bool("version", false, "Print version and exit.")

	// reapLock guards subprocess exec against the child reaper, mirroring
	// ateom-gvisor. ateom-microvm will spawn the kata shim and CH
	// processes under it in later milestones.
	reapLock sync.RWMutex
)

func main() {
	flag.Parse()
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

	serverboot.InitLogger()
	slog.InfoContext(ctx, "ateom-microvm booting", slog.String("version", version.String()))

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "ateom-microvm",
		Sampler:     sdktrace.ParentBased(sdktrace.NeverSample()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	// Create ateom dir.
	ateomDir := ateompath.AteomPath(*podUID)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}

	// Reap children reparented to us (kata shim / cloud-hypervisor in later
	// milestones), guarded so our own exec.Cmd calls can take the wait.
	go reap.ReapChildren(nil, nil, nil, &reapLock)
	slog.InfoContext(ctx, "Child process reaper launched")

	// kata launches virtiofsd with --syslog, which delivers to /dev/log. In a
	// pod there is no syslog daemon, so provide a sink or virtiofsd fails
	// ("Unix syslog delivery error") and Create fails.
	startSyslogSink(ctx)

	// kata's virtio-fs sharing depends on mount propagation: it slave-binds
	// .../shared (served by virtiofsd) from .../mounts and expects the later
	// per-container rootfs bind under mounts/ to propagate across. That only
	// works if the underlying mount is SHARED. On a host systemd makes /
	// rshared, but a container rootfs is rprivate (runc default), so the
	// propagation silently never happens: the guest sees an empty rootfs and
	// createContainer fails ENOENT. Self-bind /run/kata-containers and mark it
	// rshared so kata's propagation chain works inside the pod.
	if err := ensureSharedPropagation(ctx, "/run/kata-containers"); err != nil {
		return fmt.Errorf("while making /run/kata-containers a shared mount: %w", err)
	}

	// Clean up any old socket.
	sockPath := ateompath.AteomSocketPath(*podUID)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	// Networking (mirrors ateom-gvisor's veth model): create a named interior
	// netns; each activation builds a fresh veth pair into it (see net.go) and
	// points kata at it.
	interiorNetNS, err := createNetNSWithoutSwitching(ateompath.AteomNetNSName(*podUID))
	if err != nil {
		return fmt.Errorf("while creating interior netns: %w", err)
	}

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, NewService(*podUID, *shimBinary, *chBinary, *virtiofsdBinary, *kataConfig, *kataNamespace, *kataDebug, interiorNetNS))
	reflection.Register(svr)

	slog.InfoContext(ctx, "ateom-microvm serving", slog.String("socket", sockPath))
	if err := svr.Serve(lis); err != nil {
		return fmt.Errorf("while serving: %w", err)
	}
	return nil
}

// ensureSharedPropagation makes path a mount point with rshared propagation
// (self-bind + MS_SHARED|MS_REC), so mounts created beneath it propagate to
// slave binds (kata's mounts/ -> shared/ chain). Idempotent: skips if path is
// already a shared mount point.
func ensureSharedPropagation(ctx context.Context, path string) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return fmt.Errorf("creating %q: %w", path, err)
	}
	if b, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			// mountinfo: ID parentID major:minor root mountpoint opts optional... - fstype ...
			fields := strings.Fields(line)
			if len(fields) >= 7 && fields[4] == path && strings.Contains(line, "shared:") {
				slog.InfoContext(ctx, "Mount already shared", slog.String("path", path))
				return nil
			}
		}
	}
	if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("self-binding %q: %w", path, err)
	}
	if err := unix.Mount("", path, "", unix.MS_SHARED|unix.MS_REC, ""); err != nil {
		return fmt.Errorf("marking %q rshared: %w", path, err)
	}
	slog.InfoContext(ctx, "Made mount rshared for kata virtio-fs propagation", slog.String("path", path))
	return nil
}

// startSyslogSink binds a unixgram socket at /dev/log and drains it, so
// virtiofsd's --syslog delivery (which kata always enables) succeeds inside a
// pod that has no syslog daemon. Best-effort.
func startSyslogSink(ctx context.Context) {
	const addr = "/dev/log"
	_ = os.Remove(addr)
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: addr, Net: "unixgram"})
	if err != nil {
		slog.WarnContext(ctx, "Failed to create /dev/log syslog sink; virtiofsd --syslog may fail", slog.Any("err", err))
		return
	}
	slog.InfoContext(ctx, "syslog sink listening", slog.String("addr", addr))
	go func() {
		buf := make([]byte, 8192)
		for {
			if _, _, err := conn.ReadFromUnix(buf); err != nil {
				return
			}
		}
	}()
}

// AteomService is the cloud-hypervisor implementation of ateompb.AteomServer.
type AteomService struct {
	ateompb.UnimplementedAteomServer

	// lock serializes RPCs; like ateom-gvisor, the run/checkpoint/restore
	// lifecycle is not safe to drive concurrently.
	lock sync.Mutex

	podUID          string
	shimBinary      string
	chBinary        string
	virtiofsdBinary string
	kataConfig      string
	namespace       string
	kataDebug       bool

	// interiorNetNS hosts the per-activation actor veth peer (see net.go);
	// kata is pointed at it.
	interiorNetNS netns.NsHandle

	// running maps actor id -> the live micro-VM, kept so CheckpointWorkload can
	// pause+snapshot+teardown the same sandbox (and RestoreWorkload can track the
	// CH it relaunched).
	running map[string]*runningActor
}

var _ ateompb.AteomServer = (*AteomService)(nil)

// NewService creates a new AteomService.
func NewService(podUID, shimBinary, chBinary, virtiofsdBinary, kataConfig, namespace string, kataDebug bool, interiorNetNS netns.NsHandle) *AteomService {
	return &AteomService{
		podUID:          podUID,
		shimBinary:      shimBinary,
		chBinary:        chBinary,
		virtiofsdBinary: virtiofsdBinary,
		kataConfig:      kataConfig,
		namespace:       namespace,
		kataDebug:       kataDebug,
		interiorNetNS:   interiorNetNS,
		running:         map[string]*runningActor{},
	}
}
