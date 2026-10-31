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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/types/known/emptypb"
)

// A vsock socket that has gone missing means cloud-hypervisor stopped the VM
// (it unlinks the socket in the vsock device's shutdown), so the poll must give
// up at once instead of spending the whole timeout on a guest that is gone.
func TestDialAgentRetryGuestStopped(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "clh.sock")

	start := time.Now()
	_, err := dialAgentRetry(t.Context(), missing, 30*time.Second)
	elapsed := time.Since(start)

	if !errors.Is(err, errGuestStopped) {
		t.Errorf("dialAgentRetry(%q) error = %v, want it to wrap errGuestStopped", missing, err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("dialAgentRetry(%q) took %v; want it to give up immediately, not poll for the full timeout", missing, elapsed)
	}
}

// The socket exists but nothing answers yet: that is every dial until the guest
// reaches kata-containers.target, so it must keep polling to the timeout.
func TestDialAgentRetryNotListeningYet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clh.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("failed to create test socket: %v", err)
	}
	// Leave the socket file behind, as cloud-hypervisor does while the VM runs:
	// connecting is then refused, which is what an unanswered CONNECT looks like.
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close test listener: %v", err)
	}

	const timeout = 700 * time.Millisecond
	start := time.Now()
	_, err = dialAgentRetry(t.Context(), path, timeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("dialAgentRetry(%q) succeeded, want an error", path)
	}
	if errors.Is(err, errGuestStopped) {
		t.Errorf("dialAgentRetry(%q) error = %v, want a plain dial error (the guest is still running)", path, err)
	}
	if elapsed < timeout {
		t.Errorf("dialAgentRetry(%q) gave up after %v, want it to keep polling for at least %v", path, elapsed, timeout)
	}
}

// A canceled context ends the poll with the context's error, so a caller that
// gives up on the restore is not left waiting out the timeout.
func TestDialAgentRetryContextCanceled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clh.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("failed to create test socket: %v", err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close test listener: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := dialAgentRetry(ctx, path, time.Minute); !errors.Is(err, context.Canceled) {
		t.Errorf("dialAgentRetry(%q) error = %v, want context.Canceled", path, err)
	}
}

// resolveGuestMemMiB gives the guest the whole declared limit, and errors when
// the limit is unset or too small to boot a guest.
func TestResolveGuestMemMiB(t *testing.T) {
	const mib = 1024 * 1024
	tests := []struct {
		name        string
		declaredMiB int64 // 0 => unset
		wantMiB     int
		wantErr     bool
	}{
		{name: "unset", declaredMiB: 0, wantErr: true},
		{name: "declared honored", declaredMiB: 1536, wantMiB: 1536},
		{name: "minimum", declaredMiB: minGuestMemMiB, wantMiB: minGuestMemMiB},
		{name: "below minimum", declaredMiB: minGuestMemMiB - 1, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveGuestMemMiB(tc.declaredMiB * mib)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveGuestMemMiB(%dMiB) = %d, nil; want an error", tc.declaredMiB, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveGuestMemMiB(%dMiB) unexpected error: %v", tc.declaredMiB, err)
			}
			if got != tc.wantMiB {
				t.Errorf("resolveGuestMemMiB(%dMiB) = %d, want %d", tc.declaredMiB, got, tc.wantMiB)
			}
		})
	}
}

func TestInitParams(t *testing.T) {
	// The agent path must be the one the kata guest image actually ships, since the
	// kernel silently panics on an init= that does not exist.
	if got := initParams(true); got != "init=/usr/bin/kata-agent" {
		t.Errorf("initParams(true) = %q", got)
	}
	// Without the agent as PID 1, systemd needs kata's target — it powers the guest
	// off within seconds otherwise — and networkd must stay masked, the agent owns eth0.
	systemd := initParams(false)
	for _, want := range []string{
		"systemd.unit=kata-containers.target",
		"systemd.mask=systemd-networkd.service",
		"systemd.mask=systemd-networkd.socket",
	} {
		if !strings.Contains(systemd, want) {
			t.Errorf("initParams(false) = %q, missing %q", systemd, want)
		}
	}
	if strings.Contains(systemd, "init=") {
		t.Errorf("initParams(false) = %q, must not override init", systemd)
	}
}

// The guest must log over virtio-console, not the emulated UART: the UART traps to
// the VMM per byte, which costs ~800ms of cold boot on the kata guest's boot log.
// Debug mode adds the UART back (with earlycon) to capture the early messages hvc0
// is too late to see.
func TestBuildVMConfigConsole(t *testing.T) {
	const id = "actor-1"
	consoleLog := kata.ConsoleLogPath(id)

	cfg := buildVMConfig(id, "/vmlinux", "/rootfs.img", "", consoleLog, 256, 1, true, false)
	if cfg.Console == nil || cfg.Console.Mode != "File" || cfg.Console.File != consoleLog {
		t.Errorf("Console = %+v, want File %q", cfg.Console, consoleLog)
	}
	if cfg.Serial == nil || cfg.Serial.Mode != "Off" {
		t.Errorf("Serial = %+v, want mode Off", cfg.Serial)
	}
	if !strings.Contains(cfg.Payload.Cmdline, "console=hvc0") {
		t.Errorf("cmdline = %q, want console=hvc0", cfg.Payload.Cmdline)
	}
	if runtime.GOARCH == "amd64" && !strings.Contains(cfg.Payload.Cmdline, "clocksource=kvm-clock") {
		t.Errorf("amd64 cmdline = %q, want clocksource=kvm-clock", cfg.Payload.Cmdline)
	}
	if strings.Contains(cfg.Payload.Cmdline, "earlycon") {
		t.Errorf("cmdline = %q, must not pay for earlycon outside debug mode", cfg.Payload.Cmdline)
	}

	dbg := buildVMConfig(id, "/vmlinux", "/rootfs.img", "", consoleLog, 256, 1, true, true)
	if dbg.Serial == nil || dbg.Serial.Mode != "File" || dbg.Serial.File != kata.SerialLogPath(id) {
		t.Errorf("debug Serial = %+v, want File %q", dbg.Serial, kata.SerialLogPath(id))
	}
	if !strings.Contains(dbg.Payload.Cmdline, "earlycon=") {
		t.Errorf("debug cmdline = %q, want an earlycon", dbg.Payload.Cmdline)
	}
}

// The SIGTERM path signals these ids over ttrpc, and the agent rejects an id it
// does not know with InvalidContainerId — which aborts the whole graceful
// shutdown, so a stale id here silently costs the guest its clean exit.
func TestWorkloadIDs(t *testing.T) {
	dirs := &ateworkerpb.ActorDirs{OciBundleDir: t.TempDir()}
	var containers []*ateworkerpb.Container
	for _, name := range []string{"counter", "w"} {
		if err := os.MkdirAll(filepath.Join(ociBundlePath(dirs, name), "rootfs"), 0o755); err != nil {
			t.Fatal(err)
		}
		containers = append(containers, &ateworkerpb.Container{Name: name, ContainerSpec: &ateworkerpb.ContainerSpec{Args: []string{"/app"}}})
	}
	ctrs, err := (&AteWorkerService{}).buildActorContainers("uid", dirs, containers)
	if err != nil {
		t.Fatal(err)
	}
	got := workloadIDs(ctrs)
	if want := []string{"c_counter", "c_w"}; !slices.Equal(got, want) {
		t.Errorf("workloadIDs() = %v, want %v", got, want)
	}
}

func TestKernelParamsDebugConsole(t *testing.T) {
	prodParams := (&AteWorkerService{guestDebug: false}).kernelParams()
	for _, forbidden := range []string{"agent.debug_console", "agent.debug_console_vport", "1026"} {
		if strings.Contains(prodParams, forbidden) {
			t.Errorf("expected kernelParams() with guestDebug=false not to contain %q, but got %q", forbidden, prodParams)
		}
	}

	dbgParams := (&AteWorkerService{guestDebug: true}).kernelParams()
	for _, want := range []string{"agent.log=debug", "agent.debug_console", "agent.debug_console_vport=1026"} {
		if !strings.Contains(dbgParams, want) {
			t.Errorf("expected kernelParams() with guestDebug=true to contain %q, but got %q", want, dbgParams)
		}
	}
}

func TestBuildVMConfigBalloon(t *testing.T) {
	cfg := buildVMConfig("actor-1", "/vmlinux", "/rootfs.img", "", "/console.log", 256, 1, true, false)
	if cfg.Balloon == nil || !cfg.Balloon.FreePageReporting || cfg.Balloon.Size != 0 {
		t.Fatalf("cfg.Balloon = %+v, want size 0 with free page reporting", cfg.Balloon)
	}
	if b, err := json.Marshal(cfg); err != nil || !strings.Contains(string(b), `"balloon":{"size":0,"free_page_reporting":true}`) {
		t.Errorf("VmConfig JSON = %s, %v; want the balloon section cloud-hypervisor expects", b, err)
	}
}

// hybridVsockListener speaks cloud-hypervisor's hybrid-vsock text handshake
// ("CONNECT <port>\n" -> "OK <port>\n") on each accepted Unix connection before
// handing the stream to the ttrpc server, matching kata.DialAgent.
type hybridVsockListener struct {
	net.Listener
}

type bufferedConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (l hybridVsockListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !strings.HasPrefix(line, "CONNECT ") {
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected handshake %q", line)
	}
	if _, err := fmt.Fprint(conn, "OK 1024\n"); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &bufferedConn{Conn: conn, r: br}, nil
}

func startFakeKataAgent(t *testing.T, overrides map[string]ttrpc.Method) (*kata.AgentClient, string) {
	t.Helper()
	vsockPath := filepath.Join(t.TempDir(), "clh.sock")
	rawLis, err := net.Listen("unix", vsockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv, err := ttrpc.NewServer()
	if err != nil {
		t.Fatalf("ttrpc.NewServer: %v", err)
	}
	emptyOK := func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
		if err := unmarshal(&emptypb.Empty{}); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}
	methods := map[string]ttrpc.Method{
		"CreateContainer": emptyOK,
		"StartContainer":  emptyOK,
	}
	for k, v := range overrides {
		methods[k] = v
	}
	srv.Register("grpc.AgentService", methods)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(context.Background(), hybridVsockListener{Listener: rawLis}) }()
	t.Cleanup(func() {
		_ = srv.Close()
		if err := <-serveErr; err != nil && !errors.Is(err, ttrpc.ErrServerClosed) {
			t.Logf("fake kata-agent server: %v", err)
		}
	})

	ac, err := kata.DialAgent(t.Context(), vsockPath)
	if err != nil {
		t.Fatalf("DialAgent: %v", err)
	}
	t.Cleanup(func() { _ = ac.Close() })
	return ac, vsockPath
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Container logs must start forwarding in startRootfsContainer as soon as
// each container starts, rather than waiting for wakeupprobe.WaitAll to pass.
func TestStartRootfsContainerForwardsLogs(t *testing.T) {
	attr := resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "ns-1", Name: "actor-1"},
		UID:              "uid-1",
		TemplateAtespace: "ns-1",
		TemplateName:     "tmpl-1",
	}

	t.Run("crash output from first container is drained when second container fails to start", func(t *testing.T) {
		var stdoutCalls, stderrCalls int
		var mu sync.Mutex
		ac, vsockPath := startFakeKataAgent(t, map[string]ttrpc.Method{
			"StartContainer": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
				var req agentpb.StartContainerRequest
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				if req.GetContainerId() == "c_second" {
					return nil, errors.New("oci runtime start failed")
				}
				return &emptypb.Empty{}, nil
			},
			"ReadStdout": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
				var req agentpb.ReadStreamRequest
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				mu.Lock()
				defer mu.Unlock()
				stdoutCalls++
				if stdoutCalls == 1 {
					return &agentpb.ReadStreamResponse{Data: []byte("first container booting\n")}, nil
				}
				return &agentpb.ReadStreamResponse{}, nil
			},
			"ReadStderr": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
				var req agentpb.ReadStreamRequest
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				mu.Lock()
				defer mu.Unlock()
				stderrCalls++
				if stderrCalls == 1 {
					return &agentpb.ReadStreamResponse{Data: []byte("fatal: missing config\n")}, nil
				}
				return &agentpb.ReadStreamResponse{}, nil
			},
		})

		var buf syncBuffer
		svc := NewService("pod-1", "cloud-hypervisor", false, vmmMemReserveMiB, 10, nil, actorlog.NewActorLogger(&buf, false))
		ctrs := []actorContainer{
			{name: "first", id: "c_first", spec: &specs.Spec{Hostname: "actor"}},
			{name: "second", id: "c_second", spec: &specs.Spec{Hostname: "actor"}},
		}

		var logWG sync.WaitGroup
		if err := svc.startRootfsContainer(t.Context(), ac, attr, vsockPath, ctrs[0], &logWG); err != nil {
			t.Fatalf("startRootfsContainer(first): %v", err)
		}
		if err := svc.startRootfsContainer(t.Context(), ac, attr, vsockPath, ctrs[1], &logWG); err == nil {
			t.Fatal("startRootfsContainer(second) succeeded, want error")
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			got := buf.String()
			if strings.Contains(got, "first container booting") && strings.Contains(got, "fatal: missing config") {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		svc.closeAgentOnFailure(ac, &logWG)

		got := buf.String()
		for _, want := range []string{
			"first container booting",
			"fatal: missing config",
			`"ate.actor.container.name":"first"`,
			`"ate.actor.uid":"uid-1"`,
		} {
			if !strings.Contains(got, want) {
				t.Errorf("forwarded logs = %s, missing %q", got, want)
			}
		}
		if strings.Contains(got, `"ate.actor.container.name":"second"`) {
			t.Errorf("forwarded logs = %s, must not forward for a container that failed StartContainer", got)
		}
	})

	t.Run("live container streams before probe and unblocks on failure close", func(t *testing.T) {
		firstLineWritten := make(chan struct{})
		var stdoutCalls int
		var mu sync.Mutex
		ac, vsockPath := startFakeKataAgent(t, map[string]ttrpc.Method{
			"ReadStdout": func(ctx context.Context, unmarshal func(interface{}) error) (interface{}, error) {
				var req agentpb.ReadStreamRequest
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				mu.Lock()
				stdoutCalls++
				call := stdoutCalls
				mu.Unlock()
				if call == 1 {
					return &agentpb.ReadStreamResponse{Data: []byte("listening on wrong port\n")}, nil
				}
				if call == 2 {
					close(firstLineWritten)
				}
				// Simulate a still-running container whose stdout blocks while
				// wakeupprobe.WaitAll times out and fails.
				<-ctx.Done()
				return nil, ctx.Err()
			},
			"ReadStderr": func(ctx context.Context, unmarshal func(interface{}) error) (interface{}, error) {
				var req agentpb.ReadStreamRequest
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})

		var buf syncBuffer
		svc := NewService("pod-1", "cloud-hypervisor", false, vmmMemReserveMiB, 10, nil, actorlog.NewActorLogger(&buf, false))
		ctr := actorContainer{name: "app", id: "c_app", spec: &specs.Spec{Hostname: "actor"}}

		var logWG sync.WaitGroup
		if err := svc.startRootfsContainer(t.Context(), ac, attr, vsockPath, ctr, &logWG); err != nil {
			t.Fatalf("startRootfsContainer: %v", err)
		}

		// Before any wakeup probe completes, the first log line has already been
		// read and the forwarder has issued the next ReadStdout call.
		<-firstLineWritten

		// Simulate wakeupprobe.WaitAll failing and the defer closing the agent.
		start := time.Now()
		svc.closeAgentOnFailure(ac, &logWG)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("closeAgentOnFailure took %v on a live container, want prompt close", elapsed)
		}

		if got := buf.String(); !strings.Contains(got, "listening on wrong port") {
			t.Errorf("forwarded logs = %s, want startup log before probe failure", got)
		}
	})
}
