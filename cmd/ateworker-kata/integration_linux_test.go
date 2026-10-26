//go:build linux && kata_integration

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

// These tests run actors in real Kata VMs through a real shim. They need root,
// /dev/kvm and a Kata install, so they are built only with the kata_integration
// tag and are not part of a CI lane. Run them with hack/run-kata-test.sh.
//
// Each test drives the worker through its gRPC interface, as atelet does. The
// tunnel is a stand-in that records the ingress dialer and builds the actor's
// real network.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/cmd/ateworker-kata/internal/shim"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
)

// kataRequired reports whether a missing precondition fails a test instead of
// skipping it. CI sets CI, and REQUIRE_KATA reaches the same branch from a
// terminal. See docs/dev/best-practices/ci-fail-closed.md.
func kataRequired() bool {
	return os.Getenv("CI") == "true" || os.Getenv("REQUIRE_KATA") == "true"
}

// kataEnv is what the tests run against.
type kataEnv struct {
	shim string
	// rootfs holds an unpacked image with sh, uname, nc and wget.
	rootfs string
	// configs maps a hypervisor to its Kata configuration file.
	configs map[string]string
}

// hypervisors are the hypervisors of a Kata install, and the file of each
// configuration under the install's runtime-rs defaults. Firecracker's file
// does not pass the shim's validation as shipped: see hack/run-kata-test.sh.
var hypervisors = map[string]string{
	"qemu":       "configuration-qemu-runtime-rs.toml",
	"clh":        "configuration-clh-runtime-rs.toml",
	"dragonball": "configuration-dragonball.toml",
	"fc":         "configuration-rs-fc.toml",
}

// hypervisorNames are the keys of hypervisors, in the order the tests run them.
var hypervisorNames = []string{"qemu", "clh", "dragonball", "fc"}

// requireKata returns the environment of the install, or skips the test, or
// fails it where kataRequired says so.
func requireKata(t *testing.T) kataEnv {
	t.Helper()
	missing := func(format string, args ...any) {
		t.Helper()
		msg := fmt.Sprintf(format, args...)
		if kataRequired() {
			t.Fatalf("Kata is required (CI or REQUIRE_KATA is set) but: %s", msg)
		}
		t.Skipf("needs a Kata install: %s (run hack/run-kata-test.sh)", msg)
	}
	if os.Geteuid() != 0 {
		missing("not running as root")
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		missing("no /dev/kvm: %v", err)
	}
	root := os.Getenv("KATA_ROOT")
	if root == "" {
		root = "/opt/kata"
	}
	env := kataEnv{
		shim:    filepath.Join(root, "runtime-rs/bin/containerd-shim-kata-v2"),
		rootfs:  os.Getenv("KATA_TEST_ROOTFS"),
		configs: map[string]string{},
	}
	if _, err := os.Stat(env.shim); err != nil {
		missing("no shim: %v", err)
	}
	if env.rootfs == "" {
		missing("KATA_TEST_ROOTFS is not set")
	}
	if _, err := os.Lstat(filepath.Join(env.rootfs, "bin/sh")); err != nil {
		missing("KATA_TEST_ROOTFS has no /bin/sh: %v", err)
	}
	selected := strings.Fields(strings.ReplaceAll(os.Getenv("KATA_TEST_HYPERVISORS"), ",", " "))
	if len(selected) == 0 {
		selected = hypervisorNames
	}
	for _, name := range selected {
		file, ok := hypervisors[name]
		if !ok {
			t.Fatalf("KATA_TEST_HYPERVISORS names %q, want some of qemu, clh, dragonball, fc", name)
		}
		path := os.Getenv("KATA_TEST_CONFIG_" + strings.ToUpper(name))
		if path == "" {
			path = filepath.Join(root, "share/defaults/kata-containers/runtime-rs", file)
		}
		if _, err := os.Stat(path); err != nil {
			missing("no %s configuration: %v", name, err)
		}
		env.configs[name] = path
	}
	delegateOnce.Do(func() { delegateErr = cgroup.Delegate(context.Background()) })
	if delegateErr != nil {
		t.Fatalf("setting up the cgroup of the tests, which hack/run-kata-test.sh does with a cgroup namespace: %v", delegateErr)
	}
	return env
}

var (
	// delegateOnce makes the test process's cgroup what a worker pod's is.
	delegateOnce sync.Once
	delegateErr  error
)

func TestKata(t *testing.T) {
	previous := slog.SetLogLoggerLevel(slog.LevelDebug)
	t.Cleanup(func() { slog.SetLogLoggerLevel(previous) })
	env := requireKata(t)
	for _, hypervisor := range hypervisorNames {
		config, ok := env.configs[hypervisor]
		if !ok {
			continue
		}
		t.Run(hypervisor, func(t *testing.T) {
			t.Run("lifecycle", func(t *testing.T) { testLifecycle(t, env, hypervisor, config) })
			t.Run("crash", func(t *testing.T) { testCrash(t, env, config) })
			t.Run("stop", func(t *testing.T) { testStop(t, env, config) })
			t.Run("restart", func(t *testing.T) { testStaleVM(t, env, config) })
			t.Run("size", func(t *testing.T) { testSize(t, env, config) })
		})
	}
}

// integrationTunnel records ingress and builds each actor's real network. It
// serves no egress and no DNS.
type integrationTunnel struct {
	mu      sync.Mutex
	dials   map[string]ingress.DialFunc
	session map[string]*actornet.SandboxSession
}

func (f *integrationTunnel) PrepareEgress(context.Context, resources.ActorAttribution, *ateworkerpb.EgressGateway) (*atunnel.ActorEgress, error) {
	return nil, nil
}

func (f *integrationTunnel) ActivateEgress(resources.ActorAttribution, *atunnel.ActorEgress) error {
	return nil
}

func (f *integrationTunnel) ActivateIngress(a resources.ActorAttribution, dial ingress.DialFunc) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dials[a.UID] = dial
	return nil
}

func (f *integrationTunnel) Deactivate(_ context.Context, a resources.ActorAttribution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.dials, a.UID)
	return nil
}

func (f *integrationTunnel) ServeNetwork(ctx context.Context, a resources.ActorAttribution) (*actornet.SandboxSession, error) {
	session, err := actornet.ServeSandbox(ctx, actornet.SandboxNetworkConfig{ActorUID: a.UID, Veth: true, EgressPort: 15001}, nil, nil)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.session[a.UID] = session
	return session, nil
}

func (f *integrationTunnel) dialer(uid string) ingress.DialFunc {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials[uid]
}

// syncBuffer is a buffer the actor logger and the test share.
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

// harness is a worker serving gRPC for one actor.
type harness struct {
	t       *testing.T
	env     kataEnv
	uid     string
	dirs    *ateworkerpb.ActorDirs
	service *AteWorkerService
	tunnel  *integrationTunnel
	client  ateworkerpb.WorkerClient
	logs    *syncBuffer
	crashes *fakeCrashReporter
	config  string
}

// newHarness prepares one actor's directories as atelet does, and serves a
// worker over a unix socket.
func newHarness(t *testing.T, env kataEnv, configPath string) *harness {
	t.Helper()
	uid := uuid.NewString()
	base := t.TempDir()

	oldActors := nodepath.ActorsDir
	nodepath.ActorsDir = filepath.Join(base, "actors")
	t.Cleanup(func() { nodepath.ActorsDir = oldActors })
	root := filepath.Join(nodepath.ActorsDir, uid)
	dirs := &ateworkerpb.ActorDirs{
		RootDir:                   root,
		OciBundleDir:              filepath.Join(root, "bundles"),
		CheckpointDir:             filepath.Join(root, "checkpoint"),
		RestoreDir:                filepath.Join(root, "restore"),
		DurableDirVolumeMountsDir: filepath.Join(root, "durable"),
		SystemInfoVolumeRootsDir:  filepath.Join(root, "system-info"),
		ExternalVolumeMountsDir:   filepath.Join(root, "external"),
		SandboxAssetsDir:          nodepath.ActorSandboxAssetsDir(uid),
	}
	for _, dir := range []string{dirs.GetOciBundleDir(), dirs.GetDurableDirVolumeMountsDir(), dirs.GetSandboxAssetsDir()} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, env: env, uid: uid, dirs: dirs, logs: &syncBuffer{}, crashes: newFakeCrashReporter()}
	h.config = filepath.Join(dirs.GetSandboxAssetsDir(), kataConfigAsset)
	if err := os.WriteFile(h.config, data, 0o600); err != nil {
		t.Fatal(err)
	}

	h.tunnel = &integrationTunnel{dials: map[string]ingress.DialFunc{}, session: map[string]*actornet.SandboxSession{}}
	h.service = NewService(h.tunnel, actorlog.NewActorLogger(h.logs, false), env.shim, 4)
	h.service.crashReporter = h.crashes
	h.service.epoch.Store(1)

	sock := filepath.Join(base, "worker.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(ateinterceptors.InternalServerUnaryInterceptor))
	ateworkerpb.RegisterWorkerServer(server, h.service)
	go func() { _ = server.Serve(lis) }()
	conn, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	h.client = ateworkerpb.NewWorkerClient(conn)

	// Whatever the test left is removed when it ends. The tests assert that
	// they leave nothing, so this is for the ones that fail.
	t.Cleanup(func() {
		server.Stop()
		conn.Close()
		req := &ateworkerpb.TerminateWorkloadRequest{
			Atespace:  "team",
			ActorName: "agent",
			ActorUid:  h.uid,
			Spec:      &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{Name: "app"}}},
			ActorDirs: h.dirs,
		}
		if _, err := h.service.TerminateWorkload(context.WithoutCancel(t.Context()), req); err != nil {
			t.Logf("cleanup terminate: %v", err)
		}
	})
	return h
}

// rootfsFor copies the test rootfs to the bundle of a container.
func (h *harness) rootfsFor(name string) {
	h.t.Helper()
	bundle := ociBundlePath(h.dirs, name)
	if err := os.MkdirAll(filepath.Join(bundle, "rootfs"), 0o755); err != nil {
		h.t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", h.env.rootfs+"/.", filepath.Join(bundle, "rootfs")).CombinedOutput(); err != nil {
		h.t.Fatalf("copying the rootfs: %v: %s", err, out)
	}
}

// httpLoop serves a fixed answer on port 80. The wakeup probe reaches it.
const httpLoop = `uname -r; echo ready
while true; do
  printf 'HTTP/1.0 200 OK\r\n\r\nhi-from-actor\n' | nc -l -p 80 &
  wait $!
done`

// httpScript is httpLoop that exits on SIGTERM.
const httpScript = "trap 'exit 0' TERM\n" + httpLoop

// ignoreTermScript is httpLoop that ignores SIGTERM, so a stop has to kill it.
const ignoreTermScript = "trap '' TERM\n" + httpLoop

var defaultCaps = []string{
	"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER", "CAP_MKNOD", "CAP_NET_RAW", "CAP_SETGID",
	"CAP_SETUID", "CAP_SETFCAP", "CAP_SETPCAP", "CAP_NET_BIND_SERVICE", "CAP_SYS_CHROOT", "CAP_KILL", "CAP_AUDIT_WRITE",
}

func appWith(script string) *ateworkerpb.Container {
	return &ateworkerpb.Container{
		Name: "app",
		WakeupProbe: &ateworkerpb.WakeupProbe{
			HttpGet:        &ateworkerpb.HttpGetAction{Path: "/", Port: 80},
			TimeoutSeconds: 60,
		},
		ContainerSpec: &ateworkerpb.ContainerSpec{
			Args:         []string{"/bin/sh", "-c", script},
			Env:          []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			Capabilities: defaultCaps,
		},
	}
}

func sideWith(script string) *ateworkerpb.Container {
	return &ateworkerpb.Container{
		Name: "side",
		ContainerSpec: &ateworkerpb.ContainerSpec{
			Args:         []string{"/bin/sh", "-c", script},
			Env:          []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			Capabilities: defaultCaps,
		},
	}
}

func (h *harness) runRequest(containers ...*ateworkerpb.Container) *ateworkerpb.RunWorkloadRequest {
	for _, c := range containers {
		h.rootfsFor(c.GetName())
	}
	return &ateworkerpb.RunWorkloadRequest{
		Atespace:          "team",
		ActorName:         "agent",
		ActorUid:          h.uid,
		RuntimeAssetPaths: map[string]string{kataConfigAsset: h.config},
		Spec:              &ateworkerpb.WorkloadSpec{Containers: containers},
		ActorDirs:         h.dirs,
	}
}

// run starts the actor and returns once its wakeup probe has passed.
func (h *harness) run(req *ateworkerpb.RunWorkloadRequest) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), 3*time.Minute)
	defer cancel()
	resp, err := h.client.RunWorkload(ctx, req)
	if err != nil {
		h.t.Fatalf("RunWorkload() = %v\nlogs:\n%s", err, h.logs.String())
	}
	if resp.GetWorkerEpoch() != 1 {
		h.t.Errorf("RunWorkload() epoch = %d, want 1", resp.GetWorkerEpoch())
	}
}

// waitFor polls cond until it holds or the test's deadline passes.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}

func hostKernel(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

var kernelLine = regexp.MustCompile(`"message":"(\d+\.\d+\.\d+[^"]*)"`)

// guestKernel returns the first kernel release the actor logged.
func (h *harness) guestKernel() string {
	m := kernelLine.FindStringSubmatch(h.logs.String())
	if m == nil {
		return ""
	}
	return m[1]
}

// vmOfActor returns the actor's VM.
func (h *harness) vmOfActor() *vm {
	h.t.Helper()
	hosted := h.service.lookupActor(h.uid)
	if hosted == nil {
		h.t.Fatal("the actor is not hosted")
	}
	v := h.service.vmOf(hosted)
	if v == nil {
		h.t.Fatal("the actor has no VM")
	}
	return v
}

// execInGuest runs argv in the VM's first container and returns what it prints
// and its exit status.
func (h *harness) execInGuest(argv ...string) (string, uint32) {
	h.t.Helper()
	v := h.vmOfActor()
	s, ok := v.shim.(*shim.Shim)
	if !ok {
		h.t.Fatalf("the VM's shim is a %T", v.shim)
	}
	dir := h.t.TempDir()
	fifo := filepath.Join(dir, "out")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		h.t.Fatal(err)
	}
	out, err := os.OpenFile(fifo, os.O_RDWR|syscall.O_NONBLOCK, 0)
	if err != nil {
		h.t.Fatal(err)
	}
	defer out.Close()

	ctx, cancel := context.WithTimeout(h.t.Context(), time.Minute)
	defer cancel()
	id := v.sandboxContainer().id
	execID := "exec-" + uuid.NewString()[:8]
	process := &specs.Process{
		Args: argv, Cwd: "/",
		Env:          []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
		Capabilities: &specs.LinuxCapabilities{Bounding: defaultCaps, Effective: defaultCaps, Permitted: defaultCaps},
	}
	if err := s.Exec(ctx, id, execID, process, fifo, fifo); err != nil {
		h.t.Fatalf("Exec() = %v", err)
	}
	if err := s.StartExec(ctx, id, execID); err != nil {
		h.t.Fatalf("StartExec() = %v", err)
	}
	status, err := s.Wait(ctx, id, execID)
	if err != nil {
		h.t.Fatalf("Wait() = %v", err)
	}
	// The process is done: whatever it wrote is in the FIFO. Read it without
	// waiting for more.
	var buf bytes.Buffer
	raw, err := out.SyscallConn()
	if err != nil {
		h.t.Fatal(err)
	}
	for more := true; more; {
		chunk := make([]byte, 4096)
		var n int
		var rerr error
		if err := raw.Read(func(fd uintptr) bool {
			n, rerr = syscall.Read(int(fd), chunk)
			return true
		}); err != nil {
			h.t.Fatal(err)
		}
		buf.Write(chunk[:max(n, 0)])
		more = rerr == nil && n > 0
	}
	_ = s.Delete(ctx, id, execID)
	return buf.String(), status
}

// assertNoTrace fails the test for anything the actor left on the host.
func assertNoTrace(t *testing.T, uid string) {
	t.Helper()
	// The shim names its socket by the hash of its address, namespace and ID,
	// see internal/shim.
	sum := sha256.Sum256([]byte("none/substrate/" + uid))
	paths := []string{
		filepath.Join("/run/kata", uid),
		filepath.Join("/run/kata-containers/shared/sandboxes", uid),
		fmt.Sprintf("/run/containerd/s/%X", sum),
		nodepath.ActorNetNSPath(uid),
		filepath.Join("/run/netns", actornet.SandboxGatewayNetNSName(uid)),
		filepath.Join(cgroup.Root, uid),
	}
	for _, p := range paths {
		if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s is left behind (%v)", p, err)
		}
	}

	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		if strings.Contains(line, uid) {
			t.Errorf("mount left behind: %s", line)
		}
	}

	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err == nil && bytes.Contains(cmdline, []byte(uid)) {
			t.Errorf("process %s is left behind: %q", e.Name(), bytes.ReplaceAll(cmdline, []byte{0}, []byte{' '}))
		}
	}

	loops, _ := exec.Command("losetup", "-a").Output()
	if bytes.Contains(loops, []byte(uid)) {
		t.Errorf("loop device left behind:\n%s", loops)
	}
}

func testLifecycle(t *testing.T, env kataEnv, hypervisor, config string) {
	h := newHarness(t, env, config)
	sharesDirs := hypervisor != "fc"

	// Two containers: the first boots the VM, the second joins it. A shared
	// filesystem also carries a volume.
	app := appWith(httpScript)
	side := sideWith(`echo side-ready; trap 'exit 0' TERM; sleep 3600 & wait`)
	if sharesDirs {
		app.DurableDirVolumeMounts = []*ateworkerpb.VolumeMount{{Name: "data", MountPath: "/data"}}
		app.ContainerSpec.Args = []string{"/bin/sh", "-c", "echo from-guest > /data/file; " + httpScript}
		if err := os.MkdirAll(filepath.Join(h.dirs.GetDurableDirVolumeMountsDir(), "data"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	req := h.runRequest(app, side)
	h.run(req)

	// The guest kernel is not the host's.
	waitFor(t, "the guest to log its kernel", time.Minute, func() bool { return h.guestKernel() != "" })
	if got, host := h.guestKernel(), hostKernel(t); got == host {
		t.Errorf("the actor ran on the host kernel %s", got)
	} else {
		t.Logf("%s guest kernel %s, host kernel %s", hypervisor, got, host)
	}
	waitFor(t, "the second container to log", time.Minute, func() bool { return strings.Contains(h.logs.String(), `"message":"side-ready"`) })
	if !regexp.MustCompile(`"ate.actor.container.name":"side"[^\n]*"message":"side-ready"|"message":"side-ready"[^\n]*"ate.actor.container.name":"side"`).MatchString(h.logs.String()) {
		t.Errorf("the second container's log is not tagged with its name:\n%s", h.logs.String())
	}

	// A volume is shared with the host.
	if sharesDirs {
		waitFor(t, "the volume write to reach the host", time.Minute, func() bool {
			b, _ := os.ReadFile(filepath.Join(h.dirs.GetDurableDirVolumeMountsDir(), "data", "file"))
			return string(b) == "from-guest\n"
		})
	}

	// Ingress reaches the guest through the actor's veth, as atunnel's does.
	dial := h.tunnel.dialer(h.uid)
	if dial == nil {
		t.Fatal("ingress was not activated")
	}
	client := &http.Client{Transport: &http.Transport{DialContext: dial}, Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + actornet.ActorVethIP + ":80/")
	if err != nil {
		t.Fatalf("GET through the actor's veth: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "hi-from-actor\n" {
		t.Errorf("GET = %d %q, want 200 hi-from-actor", resp.StatusCode, body)
	}

	// A second process in the guest, and the guest's own network: its address,
	// its resolver and a connection to the actor's gateway.
	if out, exit := h.execInGuest("uname", "-r"); exit != 0 || strings.TrimSpace(out) != h.guestKernel() {
		t.Errorf("exec uname -r = %q (%d), want the guest kernel %q", out, exit, h.guestKernel())
	}
	h.tunnel.mu.Lock()
	session := h.tunnel.session[h.uid]
	h.tunnel.mu.Unlock()
	listeners, err := netns.Listen(t.Context(), session.Network.GatewayNetNS, []uint16{9000})
	if err != nil {
		t.Fatal(err)
	}
	gateway := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "gateway-ok") })}
	go func() { _ = gateway.Serve(listeners[0]) }()
	defer gateway.Close()
	out, exit := h.execInGuest("/bin/sh", "-c", "ip -4 addr show eth0; wget -qO- -T 5 http://"+actornet.ActorVethGateway+":9000/; echo; cat /etc/resolv.conf")
	if exit != 0 {
		t.Errorf("exec of the network checks exited %d: %s", exit, out)
	}
	t.Logf("%s guest network:\n%s", hypervisor, out)
	for _, want := range []string{actornet.ActorVethIP + "/30", "gateway-ok", "nameserver " + actornet.ActorVethGateway} {
		if !strings.Contains(out, want) {
			t.Errorf("guest network check output lacks %q:\n%s", want, out)
		}
	}

	// Stats read the actor's cgroup leaf.
	stats, err := h.client.GetWorkloadStats(t.Context(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: h.uid})
	if err != nil {
		t.Fatalf("GetWorkloadStats() = %v", err)
	}
	if s := stats.GetSample(); s.GetSource() != ateworkerpb.StatsSource_STATS_SOURCE_CGROUP || s.GetMemoryCurrentBytes() == 0 || s.GetSandboxClass() != "kata" {
		t.Errorf("sample = %v, want measured kata memory", s)
	}
	active, err := h.client.GetActiveWorkloadStats(t.Context(), &ateworkerpb.GetActiveWorkloadStatsRequest{})
	if err != nil || len(active.GetSamples()) != 1 {
		t.Errorf("GetActiveWorkloadStats() = %v, %v, want the one actor", active, err)
	}

	// Snapshots are refused with a reason.
	_, err = h.client.CheckpointWorkload(t.Context(), &ateworkerpb.CheckpointWorkloadRequest{
		Atespace: "team", ActorName: "agent", ActorUid: h.uid, Spec: req.GetSpec(), ActorDirs: h.dirs,
		Fidelity: ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("CheckpointWorkload() = %v, want Unimplemented", err)
	}

	if _, err := h.client.TerminateWorkload(t.Context(), h.terminateRequestFor(req)); err != nil {
		t.Fatalf("TerminateWorkload() = %v", err)
	}
	assertNoTrace(t, h.uid)
	if h.service.lookupActor(h.uid) != nil {
		t.Error("the terminated actor is still hosted")
	}
	// Terminating again finds nothing and succeeds.
	if _, err := h.client.TerminateWorkload(t.Context(), h.terminateRequestFor(req)); err != nil {
		t.Errorf("a second TerminateWorkload() = %v", err)
	}
}

func (h *harness) terminateRequestFor(run *ateworkerpb.RunWorkloadRequest) *ateworkerpb.TerminateWorkloadRequest {
	return &ateworkerpb.TerminateWorkloadRequest{
		Atespace:  "team",
		ActorName: "agent",
		ActorUid:  h.uid,
		Spec:      run.GetSpec(),
		ActorDirs: h.dirs,
	}
}

// A VM that dies on its own is torn down and reported.
func testCrash(t *testing.T, env kataEnv, config string) {
	h := newHarness(t, env, config)
	req := h.runRequest(appWith(httpScript))
	h.run(req)

	// Kill everything in the actor's cgroup, as an OOM kill of the hypervisor
	// would.
	if err := os.WriteFile(filepath.Join(cgroup.Root, h.uid, "cgroup.kill"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-h.crashes.reports:
		if got.UID != h.uid {
			t.Errorf("crash reported for %s, want %s", got.UID, h.uid)
		}
	case <-time.After(time.Minute):
		t.Fatal("no crash report")
	}
	waitFor(t, "the dead actor to be unhosted", 30*time.Second, func() bool { return h.service.lookupActor(h.uid) == nil })
	assertNoTrace(t, h.uid)
}

// stop sends SIGTERM and lets a workload that handles it end, and kills one that
// does not.
func testStop(t *testing.T, env kataEnv, config string) {
	for _, tt := range []struct {
		name   string
		script string
	}{
		{"handles SIGTERM", httpScript},
		{"ignores SIGTERM", ignoreTermScript},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, env, config)
			req := h.runRequest(appWith(tt.script))
			h.run(req)
			v := h.vmOfActor()
			h.service.actorsMu.Lock()
			hosted := h.service.actors[h.uid]
			h.service.actorsMu.Unlock()
			hosted.stopping.Store(true)
			if err := v.stop(t.Context(), time.Now().Add(5*time.Second), killWait); err != nil {
				t.Fatalf("stop() = %v", err)
			}
			if _, err := h.client.TerminateWorkload(t.Context(), h.terminateRequestFor(req)); err != nil {
				t.Fatalf("TerminateWorkload() = %v", err)
			}
			assertNoTrace(t, h.uid)
		})
	}
}

// A new worker process replaces the VM an earlier one left, found by the actor
// UID alone.
func testStaleVM(t *testing.T, env kataEnv, config string) {
	h := newHarness(t, env, config)
	req := h.runRequest(appWith(httpScript))
	h.run(req)

	// The first worker process is gone: nothing watches its VM.
	first := h.service.lookupActor(h.uid)
	first.stopping.Store(true)
	first.cancel()

	second := NewService(h.tunnel, actorlog.NewActorLogger(h.logs, false), env.shim, 4)
	second.epoch.Store(1)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	if _, err := second.RunWorkload(ctx, req); err != nil {
		t.Fatalf("RunWorkload() over the leftover VM = %v", err)
	}
	if _, err := second.TerminateWorkload(ctx, h.terminateRequestFor(req)); err != nil {
		t.Fatalf("TerminateWorkload() = %v", err)
	}
	assertNoTrace(t, h.uid)
}

// The shipped Kata configurations set static_sandbox_resource_mgmt, with which
// the shim sizes the VM at boot from the sandbox annotations the worker sets from
// the actor's limits. Without the limits the VM has the size of the
// configuration, 1 vCPU and 2 GiB.
func testSize(t *testing.T, env kataEnv, config string) {
	h := newHarness(t, env, config)
	req := h.runRequest(appWith(httpScript))
	req.Resources = &ateworkerpb.ResourceRequirements{Limits: &ateworkerpb.Resources{CpuMillicores: 2000, MemoryBytes: 512 << 20}}
	h.run(req)
	out, exit := h.execInGuest("/bin/sh", "-c", "nproc; sed -n 's/^MemTotal: *\\([0-9]*\\) kB/\\1/p' /proc/meminfo")
	fields := strings.Fields(out)
	if exit != 0 || len(fields) != 2 {
		t.Fatalf("exec = %q (%d), want the vCPU count and MemTotal", out, exit)
	}
	cpus, _ := strconv.Atoi(fields[0])
	memKB, _ := strconv.Atoi(fields[1])
	// The shim adds its own overhead to the limits. The configuration's own size
	// is 2 GiB and 1 vCPU, which would not have the VM this small.
	if cpus < 2 || memKB < 256<<10 || memKB > 1<<20 {
		t.Errorf("the VM has %d vCPUs and %d kB of memory, want at least 2 vCPUs and about 512 MiB", cpus, memKB)
	}
	if _, err := h.client.TerminateWorkload(t.Context(), h.terminateRequestFor(req)); err != nil {
		t.Fatalf("TerminateWorkload() = %v", err)
	}
	assertNoTrace(t, h.uid)
}

// A hypervisor with no shared filesystem refuses volumes before it starts
// anything.
func TestKataRefusesVolumesWithoutASharedFilesystem(t *testing.T) {
	env := requireKata(t)
	config, ok := env.configs["fc"]
	if !ok {
		t.Skip("firecracker is not selected")
	}
	h := newHarness(t, env, config)
	app := appWith(httpScript)
	app.DurableDirVolumeMounts = []*ateworkerpb.VolumeMount{{Name: "data", MountPath: "/data"}}
	_, err := h.client.RunWorkload(t.Context(), h.runRequest(app))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RunWorkload() = %v, want FailedPrecondition", err)
	}
	assertNoTrace(t, h.uid)
}
