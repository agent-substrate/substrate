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

package shim

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	taskv2 "github.com/containerd/containerd/api/runtime/task/v2"
	options "github.com/containerd/containerd/api/types/runtimeoptions/v1"
	"github.com/containerd/ttrpc"
	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.ate.dev/substrate/internal/pidfd"
)

// The tests run a fake shim: the test binary itself, started by Start the way
// containerd starts the real one. TestMain turns the process into the fake when
// fakeModeEnv is set.
const (
	fakeModeEnv = "ATE_FAKE_SHIM_MODE"
	fakeLogEnv  = "ATE_FAKE_SHIM_LOG"
	fakeFailEnv = "ATE_FAKE_SHIM_FAIL"
	// fakeFDEnv holds the descriptor of the socket the start command bound.
	fakeFDEnv = "ATE_FAKE_SHIM_FD"
	// fakeOwnerEnv holds the pid of the test, which the daemon exits with.
	fakeOwnerEnv = "ATE_FAKE_SHIM_OWNER"
	// fakeDaemonPIDFile is where the start command always records the daemon's
	// pid, for the test to kill it even when the start command fails to.
	fakeDaemonPIDFile = "daemon.pid"
)

func TestMain(m *testing.M) {
	switch os.Getenv(fakeModeEnv) {
	case "":
		os.Exit(m.Run())
	case "command":
		fakeCommand()
		os.Exit(0)
	case "daemon":
		fakeDaemon()
	}
	os.Exit(2)
}

// record is one request the fake shim saw.
type record struct {
	Method string
	Args   []string `json:",omitempty"`
	// Fields of a task request.
	ID, ExecID, Bundle, Stdout, Stderr string `json:",omitempty"`
	Signal                             uint32 `json:",omitempty"`
	Now                                bool   `json:",omitempty"`
	Rootfs                             []string
	ConfigPath                         string `json:",omitempty"`
	Process                            string `json:",omitempty"`
}

func appendRecord(r record) {
	f, err := os.OpenFile(os.Getenv(fakeLogEnv), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(r); err != nil {
		panic(err)
	}
}

// fakeCommand is the shim's command line: start forks the daemon, delete cleans up.
func fakeCommand() {
	args := os.Args[1:]
	verb := args[len(args)-1]
	appendRecord(record{Method: verb, Args: args[:len(args)-1]})
	if verb != "start" {
		return
	}
	for _, name := range []string{"TTRPC_ADDRESS", "KATA_CONF_FILE"} {
		if os.Getenv(name) != "" {
			os.Stderr.WriteString(name + " leaked into the shim\n")
			os.Exit(1)
		}
	}
	if os.Getenv(fakeFailEnv) == "start" {
		os.Stderr.WriteString("no kata configuration\n")
		os.Exit(1)
	}
	dir, _ := os.Getwd()
	socket := filepath.Join(dir, "shim.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		panic(err)
	}
	f, err := l.(*net.UnixListener).File()
	if err != nil {
		panic(err)
	}
	self, _ := os.Executable()
	cmd := exec.Command(self)
	cmd.Env = append(os.Environ(), fakeModeEnv+"=daemon", fakeFDEnv+"=3")
	cmd.ExtraFiles = []*os.File{f}
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	if err := os.WriteFile(fakeDaemonPIDFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		panic(err)
	}
	if os.Getenv(fakeFailEnv) != "pidfile" {
		if err := os.WriteFile("shim.pid", []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
			panic(err)
		}
	}
	if os.Getenv(fakeFailEnv) == "address" {
		os.Stdout.WriteString("not-an-address")
		return
	}
	os.Stdout.WriteString("unix://" + socket)
}

func fakeDaemon() {
	l, err := net.FileListener(os.NewFile(3, "socket"))
	if err != nil {
		panic(err)
	}
	srv, err := ttrpc.NewServer()
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	taskv2.RegisterTTRPCTaskService(srv, &fakeTask{done: done})
	// A test that ends without a Shutdown does not leave the daemon behind.
	owner, err := strconv.Atoi(os.Getenv(fakeOwnerEnv))
	if err != nil {
		panic(err)
	}
	ownerGone, err := pidfd.Exited(owner)
	if err != nil {
		panic(err)
	}
	go func() {
		select {
		case <-done:
			// Shutdown waits for the handler that closed done, whose reply is
			// sent as it returns.
			_ = srv.Shutdown(context.Background())
		case <-ownerGone:
		}
		os.Exit(0)
	}()
	_ = srv.Serve(context.Background(), l)
}

// fakeTask logs the requests it can serve and rejects the rest.
type fakeTask struct {
	taskv2.TTRPCTaskService
	done chan struct{}
}

func (f *fakeTask) Create(_ context.Context, r *taskv2.CreateTaskRequest) (*taskv2.CreateTaskResponse, error) {
	rec := record{Method: "Create", ID: r.GetID(), Bundle: r.GetBundle(), Stdout: r.GetStdout(), Stderr: r.GetStderr()}
	for _, m := range r.GetRootfs() {
		rec.Rootfs = append(rec.Rootfs, m.GetType()+":"+m.GetSource())
	}
	var opts options.Options
	if err := proto.Unmarshal(r.GetOptions().GetValue(), &opts); err != nil {
		return nil, err
	}
	rec.ConfigPath = opts.GetConfigPath()
	appendRecord(rec)
	if os.Getenv(fakeFailEnv) == "create" {
		return nil, errors.New("hypervisor path is not allowed")
	}
	return &taskv2.CreateTaskResponse{Pid: 4242}, nil
}

func (f *fakeTask) Start(_ context.Context, r *taskv2.StartRequest) (*taskv2.StartResponse, error) {
	appendRecord(record{Method: "Start", ID: r.GetID(), ExecID: r.GetExecID()})
	return &taskv2.StartResponse{}, nil
}

func (f *fakeTask) Wait(_ context.Context, r *taskv2.WaitRequest) (*taskv2.WaitResponse, error) {
	appendRecord(record{Method: "Wait", ID: r.GetID(), ExecID: r.GetExecID()})
	return &taskv2.WaitResponse{ExitStatus: 137}, nil
}

func (f *fakeTask) Kill(_ context.Context, r *taskv2.KillRequest) (*emptypb.Empty, error) {
	appendRecord(record{Method: "Kill", ID: r.GetID(), ExecID: r.GetExecID(), Signal: r.GetSignal()})
	return &emptypb.Empty{}, nil
}

func (f *fakeTask) Exec(_ context.Context, r *taskv2.ExecProcessRequest) (*emptypb.Empty, error) {
	appendRecord(record{Method: "Exec", ID: r.GetID(), ExecID: r.GetExecID(), Stdout: r.GetStdout(), Stderr: r.GetStderr(),
		Process: r.GetSpec().GetTypeUrl() + " " + string(r.GetSpec().GetValue())})
	return &emptypb.Empty{}, nil
}

func (f *fakeTask) Delete(_ context.Context, r *taskv2.DeleteRequest) (*taskv2.DeleteResponse, error) {
	appendRecord(record{Method: "Delete", ID: r.GetID(), ExecID: r.GetExecID()})
	return &taskv2.DeleteResponse{}, nil
}

func (f *fakeTask) Shutdown(_ context.Context, r *taskv2.ShutdownRequest) (*emptypb.Empty, error) {
	appendRecord(record{Method: "Shutdown", ID: r.GetID(), Now: r.GetNow()})
	close(f.done)
	return &emptypb.Empty{}, nil
}

// startFake starts the fake shim for container id in a new bundle directory.
func startFake(t *testing.T, fail string) (*Shim, string, func() []record) {
	t.Helper()
	t.Setenv(fakeModeEnv, "command")
	logPath := filepath.Join(t.TempDir(), "requests.json")
	t.Setenv(fakeLogEnv, logPath)
	t.Setenv(fakeFailEnv, fail)
	t.Setenv(fakeOwnerEnv, strconv.Itoa(os.Getpid()))
	// A leaked TTRPC_ADDRESS or KATA_CONF_FILE must not reach the shim.
	t.Setenv("TTRPC_ADDRESS", "/run/containerd/containerd.sock.ttrpc")
	t.Setenv("KATA_CONF_FILE", "/opt/kata/share/defaults/kata-containers/runtime-rs/configuration.toml")
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(t.TempDir(), "b")
	if err := os.Mkdir(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { killDaemon(bundle) })
	s, err := Start(t.Context(), Config{Binary: self, ConfigPath: "/kata/clh.toml"}, "actor-1", bundle, nil)
	records := func() []record {
		f, err := os.Open(logPath)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var out []record
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			var r record
			if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
				t.Fatal(err)
			}
			out = append(out, r)
		}
		return out
	}
	if err != nil {
		if fail == "" {
			t.Fatalf("Start() = %v", err)
		}
		t.Logf("Start() = %v", err)
		return nil, bundle, records
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, bundle, records
}

// killDaemon kills the fake shim daemon the start command left in bundle.
func killDaemon(bundle string) {
	if pid, err := readPID(filepath.Join(bundle, fakeDaemonPIDFile)); err == nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

func TestStartRejectsMissingConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no binary", Config{ConfigPath: "/kata/clh.toml"}},
		{"no config", Config{Binary: "/opt/kata/runtime-rs/bin/containerd-shim-kata-v2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Start(t.Context(), tt.cfg, "actor-1", t.TempDir(), nil); err == nil {
				t.Fatal("Start() = nil, want an error")
			}
		})
	}
}

func TestStartFailures(t *testing.T) {
	tests := []struct {
		name string
		fail string
		want string
	}{
		{"start command fails", "start", "no kata configuration"},
		{"no pid file", "pidfile", "reading the shim pid"},
		{"not an address", "address", "want a unix:// address"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(fakeModeEnv, "command")
			t.Setenv(fakeLogEnv, filepath.Join(t.TempDir(), "requests.json"))
			t.Setenv(fakeFailEnv, tt.fail)
			self, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(fakeOwnerEnv, strconv.Itoa(os.Getpid()))
			bundle := t.TempDir()
			t.Cleanup(func() { killDaemon(bundle) })
			_, err = Start(t.Context(), Config{Binary: self, ConfigPath: "/kata/clh.toml"}, "actor-1", bundle, nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Start() = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

func TestStartRunsTheStartCommand(t *testing.T) {
	_, bundle, records := startFake(t, "")
	got := records()
	if len(got) != 1 || got[0].Method != "start" {
		t.Fatalf("records = %+v, want one start", got)
	}
	want := []string{"-namespace", "substrate", "-address", "none", "-publish-binary", "/bin/true", "-id", "actor-1"}
	if diff := cmp.Diff(want, got[0].Args); diff != "" {
		t.Errorf("start arguments mismatch (-want +got):\n%s", diff)
	}
	if _, err := os.Stat(filepath.Join(bundle, shimPIDFile)); err != nil {
		t.Errorf("the start command ran outside the bundle: %v", err)
	}
}

func TestTaskLifecycle(t *testing.T) {
	s, bundle, records := startFake(t, "")
	ctx := t.Context()

	pid, err := s.Create(ctx, CreateRequest{
		ID: "actor-1", Bundle: bundle, Stdout: "/b/stdout", Stderr: "/b/stderr",
		Rootfs: []Mount{{Type: "ext4", Source: "/dev/loop7"}},
	})
	if err != nil || pid != 4242 {
		t.Fatalf("Create() = %d, %v, want 4242", pid, err)
	}
	if err := s.StartContainer(ctx, "actor-1"); err != nil {
		t.Fatalf("StartContainer() = %v", err)
	}
	proc := &specs.Process{Args: []string{"/bin/true"}, Cwd: "/"}
	if err := s.Exec(ctx, "actor-1", "e1", proc, "/b/stdout", "/b/stderr"); err != nil {
		t.Fatalf("Exec() = %v", err)
	}
	if err := s.StartExec(ctx, "actor-1", "e1"); err != nil {
		t.Fatalf("StartExec() = %v", err)
	}
	if err := s.Kill(ctx, "actor-1", "", syscall.SIGTERM); err != nil {
		t.Fatalf("Kill() = %v", err)
	}
	if status, err := s.Wait(ctx, "actor-1", ""); err != nil || status != 137 {
		t.Fatalf("Wait() = %d, %v, want 137", status, err)
	}
	if err := s.Delete(ctx, "actor-1", "e1"); err != nil {
		t.Fatalf("Delete(exec) = %v", err)
	}
	if err := s.Delete(ctx, "actor-1", ""); err != nil {
		t.Fatalf("Delete() = %v", err)
	}
	if err := s.Shutdown(ctx, "actor-1", true); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	select {
	case <-s.Exited():
	case <-time.After(10 * time.Second):
		t.Fatal("the shim did not exit after Shutdown")
	}

	procJSON, err := json.Marshal(proc)
	if err != nil {
		t.Fatal(err)
	}
	want := []record{
		{Method: "start", Args: []string{"-namespace", "substrate", "-address", "none", "-publish-binary", "/bin/true", "-id", "actor-1"}},
		{Method: "Create", ID: "actor-1", Bundle: bundle, Stdout: "/b/stdout", Stderr: "/b/stderr", Rootfs: []string{"ext4:/dev/loop7"}, ConfigPath: "/kata/clh.toml"},
		{Method: "Start", ID: "actor-1"},
		{Method: "Exec", ID: "actor-1", ExecID: "e1", Stdout: "/b/stdout", Stderr: "/b/stderr", Process: processSpecTypeURL + " " + string(procJSON)},
		{Method: "Start", ID: "actor-1", ExecID: "e1"},
		{Method: "Kill", ID: "actor-1", Signal: uint32(syscall.SIGTERM)},
		{Method: "Wait", ID: "actor-1"},
		{Method: "Delete", ID: "actor-1", ExecID: "e1"},
		{Method: "Delete", ID: "actor-1"},
		{Method: "Shutdown", ID: "actor-1", Now: true},
	}
	if diff := cmp.Diff(want, records()); diff != "" {
		t.Errorf("requests mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateErrorNamesTheContainer(t *testing.T) {
	s, bundle, _ := startFake(t, "create")
	_, err := s.Create(t.Context(), CreateRequest{ID: "actor-1", Bundle: bundle})
	if err == nil {
		t.Fatal("Create() = nil, want an error")
	}
	for _, want := range []string{`creating container "actor-1"`, "hypervisor path is not allowed"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Create() = %v, want it to contain %q", err, want)
		}
	}
}

func TestCleanupRunsTheDeleteCommand(t *testing.T) {
	_, bundle, records := startFake(t, "")
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := Cleanup(t.Context(), self, "actor-1", bundle); err != nil {
		t.Fatalf("Cleanup() = %v", err)
	}
	got := records()
	if len(got) != 2 || got[1].Method != "delete" {
		t.Fatalf("records = %+v, want start then delete", got)
	}
	want := []string{"-namespace", "substrate", "-address", "none", "-publish-binary", "/bin/true", "-id", "actor-1", "-bundle", bundle}
	if diff := cmp.Diff(want, got[1].Args); diff != "" {
		t.Errorf("delete arguments mismatch (-want +got):\n%s", diff)
	}
}

func TestCleanupFailure(t *testing.T) {
	err := Cleanup(t.Context(), "/nonexistent/shim", "actor-1", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "shim delete command") {
		t.Errorf("Cleanup() = %v, want the failed delete command", err)
	}
}

func TestWithoutEnv(t *testing.T) {
	got := withoutEnv([]string{"A=1", "TTRPC_ADDRESS=x", "TTRPC_ADDRESS_2=y", "KATA_CONF_FILE=/f", "B=2"}, "TTRPC_ADDRESS", "KATA_CONF_FILE")
	want := []string{"A=1", "TTRPC_ADDRESS_2=y", "B=2"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("withoutEnv() mismatch (-want +got):\n%s", diff)
	}
}
