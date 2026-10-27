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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/containerd/ttrpc"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/e2fsprogsenv"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func TestCopySparseKeepsDataAndHoles(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "src"), filepath.Join(dir, "dst")
	f, err := os.Create(src)
	if err != nil {
		t.Fatal(err)
	}
	const size = 64 << 20
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	a, b := bytes.Repeat([]byte("a"), 8192), bytes.Repeat([]byte("b"), 4096)
	if _, err := f.WriteAt(a, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(b, 40<<20); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := copySparse(context.Background(), src, dst); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := os.ReadFile(src)
	if len(got) != size || !bytes.Equal(got, want) {
		t.Fatalf("copy differs: len %d", len(got))
	}
	var st unix.Stat_t
	if err := unix.Stat(dst, &st); err != nil {
		t.Fatal(err)
	}
	if alloc := st.Blocks * 512; alloc > 1<<20 {
		t.Errorf("dst allocates %d bytes, want the holes kept", alloc)
	}
}

func TestRewriteUpperDiskPaths(t *testing.T) {
	cfg := map[string]any{"disks": []any{
		map[string]any{"path": "/var/lib/ate/actors/old/rootfs-upper/web" + upperDiskSuffix},
	}}
	changed, err := rewriteUpperDiskPaths(cfg, "config.json", "/var/lib/ate/actors/new/rootfs-upper")
	if err != nil || !changed {
		t.Fatalf("rewriteUpperDiskPaths = %v, %v; want true, nil", changed, err)
	}
	got := cfg["disks"].([]any)[0].(map[string]any)["path"]
	if want := "/var/lib/ate/actors/new/rootfs-upper/web" + upperDiskSuffix; got != want {
		t.Errorf("disk path = %q, want %q", got, want)
	}
	if changed, err := rewriteUpperDiskPaths(cfg, "config.json", "/var/lib/ate/actors/new/rootfs-upper"); err != nil || changed {
		t.Errorf("second rewriteUpperDiskPaths = %v, %v; want false, nil", changed, err)
	}
	// A disk this worker never attaches is not repointed silently.
	bad := map[string]any{"disks": []any{map[string]any{"path": "/elsewhere/disk.img"}}}
	if _, err := rewriteUpperDiskPaths(bad, "config.json", "/x"); err == nil {
		t.Error("rewriteUpperDiskPaths of a non-upper disk = nil error, want one")
	}
}

func TestUpperDisksOrderAndGuestOverlay(t *testing.T) {
	base := t.TempDir()
	ctrs := []actorContainer{{name: "web", id: "web"}, {name: "host", id: "host"}, {name: "db", id: "db"}}
	for _, c := range []string{"web", "db"} {
		if err := os.WriteFile(upperDiskPath(base, c), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	disks, mounts, err := upperDisks(base, ctrs)
	if err != nil {
		t.Fatal(err)
	}
	if len(disks) != 2 || disks[0].Path != upperDiskPath(base, "web") || disks[1].Path != upperDiskPath(base, "db") || disks[0].Readonly {
		t.Fatalf("disks %+v", disks)
	}
	for _, d := range disks {
		if d.PciSegment != 1 {
			t.Errorf("disk %s on PCI segment %d, want 1 beside virtio-fs", d.Path, d.PciSegment)
		}
		if !d.Direct {
			t.Errorf("disk %s is not direct, so its writes would fill the actor's memory.high with host page cache", d.Path)
		}
	}
	if mounts["host"] != nil {
		t.Errorf("container without a disk got a mount %+v", mounts["host"])
	}
	var devices []string
	for _, m := range sandboxUpperDisks(ctrs, mounts) {
		devices = append(devices, m.Device)
	}
	if !slices.Equal(devices, []string{"/dev/vda", "/dev/vdb"}) {
		t.Fatalf("devices %v", devices)
	}
	cfg := buildVMConfig("uid", "/vmlinux", "/rootfs.img", "", "/c.log", 256, 1, true, false, disks)
	if len(cfg.Disks) != 2 || cfg.Pmem[0].File != "/rootfs.img" {
		t.Fatalf("vm config disks %+v pmem %+v", cfg.Disks, cfg.Pmem)
	}
}

func TestUpperDiskSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	src, ckpt, dst := t.TempDir(), t.TempDir(), t.TempDir()
	if ok, err := linkUpperDisk(ctx, src, ckpt, "web"); ok || err != nil {
		t.Fatalf("no disk: ok=%v err=%v", ok, err)
	}
	if err := os.WriteFile(upperDiskPath(src, "web"), []byte("ext4"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ok, err := linkUpperDisk(ctx, src, ckpt, "web"); !ok || err != nil {
		t.Fatalf("link: ok=%v err=%v", ok, err)
	}
	if ok, err := restoreUpperDisk(ctx, dst, ckpt, "web"); !ok || err != nil {
		t.Fatalf("restore: ok=%v err=%v", ok, err)
	}
	if b, _ := os.ReadFile(upperDiskPath(dst, "web")); string(b) != "ext4" {
		t.Fatalf("restored %q", b)
	}
	if ok, _ := restoreUpperDisk(ctx, dst, ckpt, "db"); ok {
		t.Fatal("restored a disk the snapshot does not have")
	}
}

// A snapshot that mixes a disk container and a share container restores each
// the way it was taken.
func TestUntarRootfsUpperRestoresDisks(t *testing.T) {
	snap, dst := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(snap, "rootfs-disk-web.ext4"), []byte("ext4"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	if _, _, err := kata.MkdirUpperWorkDirs(src, "db"); err != nil {
		t.Fatal(err)
	}
	if err := tarRootfsUpper(t.Context(), src, snap, []string{"db"}); err != nil {
		t.Fatal(err)
	}
	if err := untarRootfsUpper(t.Context(), dst, snap, []string{"web", "db"}); err != nil {
		t.Fatal(err)
	}
	if !hasUpperDisk(dst, "web") {
		t.Error("web lost its upper disk")
	}
	if hasUpperDisk(dst, "db") {
		t.Error("db gained an upper disk")
	}
	if _, err := os.Stat(filepath.Join(dst, "db", "fs")); err != nil {
		t.Errorf("db upper dir: %v", err)
	}
}

func TestUntarRootfsUpperHonorsCanceledContext(t *testing.T) {
	snap, dst := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(snap, "rootfs-disk-web.ext4"), []byte("ext4"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := untarRootfsUpper(ctx, dst, snap, []string{"web"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("untarRootfsUpper with canceled ctx = %v, want context.Canceled", err)
	}
}

// testUpperDiskImage is the committed rootfs-upper-disk asset.
var testUpperDiskImage = filepath.Join("..", "..", "hack", "microvm-assets", "upper-disk.ext4.zst")

func TestMakeUpperDisks(t *testing.T) {
	base := t.TempDir()
	if err := makeUpperDisks(base, testUpperDiskImage, []string{"web", "db"}); err != nil {
		t.Fatal(err)
	}
	seen := map[[16]byte]string{}
	for _, name := range []string{"web", "db"} {
		img := upperDiskPath(base, name)
		var st unix.Stat_t
		if err := unix.Stat(img, &st); err != nil {
			t.Fatal(err)
		}
		if st.Size != 16<<30 {
			t.Errorf("%s is %d bytes, want 16 GiB", name, st.Size)
		}
		if alloc := st.Blocks * 512; alloc > 1<<20 {
			t.Errorf("%s allocates %d bytes, want under 1 MiB", name, alloc)
		}
		// The UUID is at offset 0x68 of the superblock, which starts at 1024.
		f, err := os.Open(img)
		if err != nil {
			t.Fatal(err)
		}
		var u [16]byte
		_, err = f.ReadAt(u[:], 1024+0x68)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if other, ok := seen[u]; ok {
			t.Errorf("%s and %s share UUID %x", name, other, u)
		}
		seen[u] = name
	}
	t.Run("fsck", func(t *testing.T) {
		e2fsprogsenv.Need(t, "e2fsck", "debugfs")
		for _, name := range []string{"web", "db"} {
			img := upperDiskPath(base, name)
			if out, err := exec.Command("e2fsck", "-fn", img).CombinedOutput(); err != nil {
				t.Errorf("e2fsck %s: %v: %s", name, err, out)
			}
			// The overlay needs root-owned fs/ (0755) and work/ (0700).
			for dir, mode := range map[string]string{"/fs": "0755", "/work": "0700"} {
				out, err := exec.Command("debugfs", "-R", "stat "+dir, img).Output()
				if err != nil {
					t.Fatalf("debugfs %s: %v: %s", name, err, out)
				}
				got := map[string]string{}
				fields := strings.Fields(string(out))
				for i := 0; i+1 < len(fields); i++ {
					got[fields[i]] = fields[i+1]
				}
				if got["Type:"] != "directory" || got["Mode:"] != mode || got["User:"] != "0" || got["Group:"] != "0" {
					t.Errorf("%s %s: type %s mode %s owner %s:%s, want a root-owned %s directory", name, dir, got["Type:"], got["Mode:"], got["User:"], got["Group:"], mode)
				}
			}
		}
	})
}

func TestMakeUpperDisksTooManyContainers(t *testing.T) {
	base := t.TempDir()
	var names []string
	for i := range kata.MaxUpperDisks + 1 {
		names = append(names, fmt.Sprintf("c%d", i))
	}
	if err := makeUpperDisks(base, testUpperDiskImage, names); err == nil {
		t.Fatal("makeUpperDisks with too many containers = nil error, want one")
	}
	if hasUpperDisk(base, names[0]) {
		t.Error("makeUpperDisks wrote a disk before failing")
	}
}

func TestCheckUpperDiskSupport(t *testing.T) {
	disk := []ch.DiskConfig{{Path: "/upper/app.upper.ext4"}}
	if err := checkUpperDiskSupport(nil, false); err != nil {
		t.Errorf("no disks without agent init = %v, want nil", err)
	}
	if err := checkUpperDiskSupport(disk, true); err != nil {
		t.Errorf("disks with agent init = %v, want nil", err)
	}
	if err := checkUpperDiskSupport(disk, false); err == nil {
		t.Error("disks without agent init = nil, want error")
	}
}

func TestRestorableFidelity(t *testing.T) {
	const (
		volumes = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
		rootfs  = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
		memory  = ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	)
	tests := []struct {
		name       string
		want       ateworkerpb.SnapshotFidelity
		files      []string
		containers []string
		wantGot    ateworkerpb.SnapshotFidelity
	}{
		{
			name:       "ROOTFS snapshot with upper disk restores at ROOTFS",
			want:       rootfs,
			files:      []string{"rootfs-disk-app.ext4"},
			containers: []string{"app"},
			wantGot:    rootfs,
		},
		{
			name:       "MEMORY snapshot with upper tar restores at ROOTFS",
			want:       rootfs,
			files:      []string{memoryRangesFile, "state.json", "config.json", "rootfs-upper-app.tar"},
			containers: []string{"app"},
			wantGot:    rootfs,
		},
		{
			name:       "MEMORY snapshot with upper disk restores at VOLUMES",
			want:       rootfs,
			files:      []string{memoryRangesFile, "state.json", "config.json", "rootfs-disk-app.ext4"},
			containers: []string{"sidecar", "app"},
			wantGot:    volumes,
		},
		{
			name:       "state.json alone marks a MEMORY snapshot",
			want:       rootfs,
			files:      []string{"state.json", "rootfs-disk-app.ext4"},
			containers: []string{"app"},
			wantGot:    volumes,
		},
		{
			name:       "MEMORY restore keeps MEMORY",
			want:       memory,
			files:      []string{memoryRangesFile, "state.json", "config.json", "rootfs-disk-app.ext4"},
			containers: []string{"app"},
			wantGot:    memory,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.files {
				if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := restorableFidelity(t.Context(), tc.want, dir, tc.containers)
			if err != nil || got != tc.wantGot {
				t.Fatalf("restorableFidelity(%v) = %v, %v; want %v", tc.want, got, err, tc.wantGot)
			}
		})
	}
}

type hybridVsockListener struct {
	net.Listener
}

type bufferedConn struct {
	net.Conn
	r io.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (l hybridVsockListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		br := bufio.NewReader(conn)
		line, err := br.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "CONNECT ") {
			_ = conn.Close()
			continue
		}
		if _, err := io.WriteString(conn, "OK 1024\n"); err != nil {
			_ = conn.Close()
			continue
		}
		return &bufferedConn{Conn: conn, r: br}, nil
	}
}

func startFakePowerOffEnv(t *testing.T, onDestroy func() error) (vsockPath string, client *ch.Client, vmState *atomic.Value) {
	t.Helper()
	dir := t.TempDir()
	vmState = &atomic.Value{}
	vmState.Store("Running")

	chSock := filepath.Join(dir, "ch.sock")
	chLis, err := net.Listen("unix", chSock)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/vm.info", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"state":%q}`, vmState.Load().(string))
	})
	httpSrv := &http.Server{Handler: mux}
	go func() { _ = httpSrv.Serve(chLis) }()
	t.Cleanup(func() { _ = httpSrv.Close() })

	vsockPath = filepath.Join(dir, "vsock.sock")
	vsockLis, err := net.Listen("unix", vsockPath)
	if err != nil {
		t.Fatal(err)
	}
	vsockLis.(*net.UnixListener).SetUnlinkOnClose(false)
	srv, err := ttrpc.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	srv.Register("grpc.AgentService", map[string]ttrpc.Method{
		"DestroySandbox": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req agentpb.DestroySandboxRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			if err := onDestroy(); err != nil {
				return nil, err
			}
			return &emptypb.Empty{}, nil
		},
	})
	go func() { _ = srv.Serve(context.Background(), hybridVsockListener{vsockLis}) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = os.Remove(vsockPath)
	})

	return vsockPath, ch.NewClient(chSock), vmState
}

func TestPowerOffGuest(t *testing.T) {
	t.Run("dial failure before DestroySandbox clears stopping", func(t *testing.T) {
		_, client, vmState := startFakePowerOffEnv(t, func() error { return nil })
		// Even if the VMM is Shutdown, an unprompted exit before DestroySandbox
		// never synced the upper disks and must fail rather than report success.
		vmState.Store("Shutdown")
		ra := &runningActor{}
		err := powerOffGuest(t.Context(), filepath.Join(t.TempDir(), "missing.sock"), client, ra)
		if err == nil {
			t.Fatal("powerOffGuest with missing vsock = nil, want error")
		}
		if ra.stopping.Load() {
			t.Error("ra.stopping stayed true after DialAgent failed")
		}
		if ra.poweringOff.Load() || ra.guestPoweredOff.Load() {
			t.Error("power-off state set before DestroySandbox ran")
		}
	})

	t.Run("DestroySandbox error keeps stopping set and retry after shutdown succeeds", func(t *testing.T) {
		var calls atomic.Int32
		vsockPath, client, vmState := startFakePowerOffEnv(t, func() error {
			calls.Add(1)
			return errors.New("connection reset during power off")
		})
		ra := &runningActor{}
		if err := powerOffGuest(t.Context(), vsockPath, client, ra); err == nil {
			t.Fatal("first powerOffGuest = nil, want DestroySandbox error")
		}
		if !ra.stopping.Load() || !ra.poweringOff.Load() {
			t.Fatalf("after DestroySandbox ran: stopping=%v poweringOff=%v, want true/true", ra.stopping.Load(), ra.poweringOff.Load())
		}
		// Simulate the guest finishing its shutdown and CH unlinking vsockPath.
		vmState.Store("Shutdown")
		_ = os.Remove(vsockPath)
		if err := powerOffGuest(t.Context(), vsockPath, client, ra); err != nil {
			t.Fatalf("retry powerOffGuest after shutdown = %v, want nil", err)
		}
		if calls.Load() != 1 {
			t.Errorf("DestroySandbox calls = %d, want 1", calls.Load())
		}
		if !ra.guestPoweredOff.Load() {
			t.Error("ra.guestPoweredOff = false after shutdown, want true")
		}
	})

	t.Run("clean DestroySandbox records guestPoweredOff for idempotent retry", func(t *testing.T) {
		var vmState *atomic.Value
		vsockPath, client, state := startFakePowerOffEnv(t, func() error {
			vmState.Store("Shutdown")
			return nil
		})
		vmState = state
		ra := &runningActor{}
		if err := powerOffGuest(t.Context(), vsockPath, client, ra); err != nil {
			t.Fatalf("powerOffGuest = %v, want nil", err)
		}
		if !ra.stopping.Load() || !ra.isGuestPoweredOff() {
			t.Fatalf("stopping=%v poweredOff=%v, want true/true", ra.stopping.Load(), ra.isGuestPoweredOff())
		}
		// Retry succeeds even after both sockets are gone.
		_ = os.Remove(vsockPath)
		deadClient := ch.NewClient(filepath.Join(t.TempDir(), "gone.sock"))
		if err := powerOffGuest(t.Context(), vsockPath, deadClient, ra); err != nil {
			t.Fatalf("second powerOffGuest = %v, want nil", err)
		}
	})
}
