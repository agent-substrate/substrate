// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"maps"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/actordirs"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/internal/volume/csi"
	"go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

type mountCall struct {
	volumeID   string
	targetPath string
	attributes map[string]string
}

type fakeWorkerPlugin struct {
	mountErr    error
	mountErrs   map[string]error
	unmountErr  error
	unmountErrs map[string]error
	unmounted   []string
	mountCalls  []mountCall
}

func (f *fakeWorkerPlugin) MountVolume(ctx context.Context, req volume.MountVolumeRequest) error {
	f.mountCalls = append(f.mountCalls, mountCall{
		volumeID:   req.VolumeID,
		targetPath: req.TargetPath,
		attributes: req.VolumeContext,
	})
	if f.mountErrs != nil {
		if err, ok := f.mountErrs[req.VolumeID]; ok {
			return err
		}
	}
	return f.mountErr
}

func (f *fakeWorkerPlugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	f.unmounted = append(f.unmounted, volumeID)
	if f.unmountErrs != nil {
		if err, ok := f.unmountErrs[volumeID]; ok {
			return err
		}
	}
	return f.unmountErr
}

var _ volume.VolumePluginWorkerPlane = (*fakeWorkerPlugin)(nil)

func TestUnmountExternalVolumes(t *testing.T) {
	ctx := context.Background()
	actorUID := "test-actor-123"

	extVol1 := &ateletpb.Volume{
		Name: "vol-1",
		External: &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "mock-vol-1",
			VolumeType:      "mock-driver",
		},
	}
	extVol2 := &ateletpb.Volume{
		Name: "vol-2",
		External: &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "mock-vol-2",
			VolumeType:      "mock-driver",
		},
	}
	durableVol := &ateletpb.Volume{
		Name:       "durable-1",
		DurableDir: &ateletpb.DurableDirVolumeSource{},
	}

	t.Run("success", func(t *testing.T) {
		fake := &fakeWorkerPlugin{}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, durableVol, extVol2})
		if err != nil {
			t.Fatalf("unmountExternalVolumes failed unexpectedly: %v", err)
		}
		if len(fake.unmounted) != 2 || fake.unmounted[0] != "mock-vol-1" || fake.unmounted[1] != "mock-vol-2" {
			t.Errorf("unmounted volumes = %v, want [mock-vol-1, mock-vol-2]", fake.unmounted)
		}
	})

	t.Run("unmount failure is blocking", func(t *testing.T) {
		fake := &fakeWorkerPlugin{
			unmountErr: errors.New("device or resource busy"),
		}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1})
		if err == nil {
			t.Fatal("unmountExternalVolumes returned nil, want blocking error")
		}
		if !errors.Is(err, fake.unmountErr) {
			t.Errorf("error = %v, want to contain unmount error", err)
		}
	})

	t.Run("multiple unmount failures return joined error", func(t *testing.T) {
		fake := &fakeWorkerPlugin{
			unmountErr: fmt.Errorf("unmount failed"),
		}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2})
		if err == nil {
			t.Fatal("unmountExternalVolumes returned nil, want blocking error")
		}
		// Both volumes should have attempt made
		if len(fake.unmounted) != 2 {
			t.Errorf("attempted unmount count = %d, want 2", len(fake.unmounted))
		}
	})
}

func TestMountExternalVolumes(t *testing.T) {
	ctx := context.Background()
	actorUID := "test-actor-456"

	extVol1 := &ateletpb.Volume{
		Name: "vol-1",
		External: &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "mock-vol-1",
			VolumeType:      "mock-driver",
			VolumeContext:   map[string]string{"key": "val1"},
		},
	}
	extVol2 := &ateletpb.Volume{
		Name: "vol-2",
		External: &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "mock-vol-2",
			VolumeType:      "mock-driver",
			VolumeContext:   map[string]string{"key": "val2"},
		},
	}
	durableVol := &ateletpb.Volume{
		Name:       "durable-1",
		DurableDir: &ateletpb.DurableDirVolumeSource{},
	}

	t.Run("success mounts external volumes and creates mount directory", func(t *testing.T) {
		useTempNodeDirs(t)

		fake := &fakeWorkerPlugin{}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, durableVol, extVol2})
		if err != nil {
			t.Fatalf("mountExternalVolumes failed unexpectedly: %v", err)
		}

		if len(fake.mountCalls) != 2 {
			t.Fatalf("mountCalls count = %d, want 2", len(fake.mountCalls))
		}

		expectedPath1 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-1")
		expectedPath2 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-2")

		for _, path := range []string{expectedPath1, expectedPath2} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat mount path %q: %v", path, err)
			}
			if !info.IsDir() {
				t.Errorf("%q is not a directory", path)
			}
			if perm := info.Mode().Perm(); perm != 0o750 {
				t.Errorf("perm for %q = %o, want 750", path, perm)
			}
		}

		wantCalls := []mountCall{
			{volumeID: "mock-vol-1", targetPath: expectedPath1, attributes: map[string]string{"key": "val1"}},
			{volumeID: "mock-vol-2", targetPath: expectedPath2, attributes: map[string]string{"key": "val2"}},
		}
		if diff := cmp.Diff(wantCalls, fake.mountCalls, cmp.AllowUnexported(mountCall{})); diff != "" {
			t.Errorf("mountCalls mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("target directory already exists is handled cleanly", func(t *testing.T) {
		useTempNodeDirs(t)

		expectedPath := ateletpath.ExternalVolumeHostPath(actorUID, "vol-1")
		if err := os.MkdirAll(expectedPath, 0o750); err != nil {
			t.Fatalf("pre-creating mount point: %v", err)
		}

		fake := &fakeWorkerPlugin{}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1})
		if err != nil {
			t.Fatalf("mountExternalVolumes with existing directory failed: %v", err)
		}
		if len(fake.mountCalls) != 1 || fake.mountCalls[0].targetPath != expectedPath {
			t.Errorf("mountCalls = %v, want 1 call for %q", fake.mountCalls, expectedPath)
		}
	})

	t.Run("plugin lookup failure returns error", func(t *testing.T) {
		useTempNodeDirs(t)

		unknownVol := &ateletpb.Volume{
			Name: "vol-unknown",
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-unknown",
				VolumeType:      "unknown-driver",
			},
		}

		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{unknownVol})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail with unknown plugin, got nil")
		}
		if !strings.Contains(err.Error(), "unknown-driver") {
			t.Errorf("error = %v, want to contain driver name %q", err, "unknown-driver")
		}
	})

	t.Run("plugin mount failure returns error", func(t *testing.T) {
		useTempNodeDirs(t)

		fake := &fakeWorkerPlugin{
			mountErr: errors.New("mount operation failed: device or resource busy"),
		}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail, got nil")
		}
		if !strings.Contains(err.Error(), "device or resource busy") {
			t.Errorf("error = %v, want to contain underlying mount error", err)
		}
	})

	t.Run("multi-volume partial failure aborts on first failure", func(t *testing.T) {
		useTempNodeDirs(t)

		fake := &fakeWorkerPlugin{
			mountErr: errors.New("cannot mount volume"),
		}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail, got nil")
		}
		if len(fake.mountCalls) != 1 {
			t.Errorf("mountCalls count = %d, want 1 (stopped after first failure)", len(fake.mountCalls))
		}
	})

	t.Run("multi-volume partial failure does not roll back already mounted volume", func(t *testing.T) {
		useTempNodeDirs(t)

		expectedPath1 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-1")
		expectedPath2 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-2")

		fake := &fakeWorkerPlugin{
			mountErrs: map[string]error{
				"mock-vol-2": errors.New("simulated mount error on vol-2"),
			},
		}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2})
		if err == nil {
			t.Fatal("expected mountExternalVolumes to fail when vol-2 fails, got nil")
		}
		if !strings.Contains(err.Error(), "simulated mount error on vol-2") {
			t.Errorf("error = %v, want error to contain %q", err, "simulated mount error on vol-2")
		}

		// Both volumes had MountVolume called: vol-1 succeeded, vol-2 failed.
		if len(fake.mountCalls) != 2 {
			t.Fatalf("mountCalls count = %d, want 2", len(fake.mountCalls))
		}
		if fake.mountCalls[0].volumeID != "mock-vol-1" || fake.mountCalls[1].volumeID != "mock-vol-2" {
			t.Errorf("mountCalls = %v, want calls for mock-vol-1 followed by mock-vol-2", fake.mountCalls)
		}

		// Verify vol-1 was not rolled back or unmounted when vol-2 failed.
		if len(fake.unmounted) != 0 {
			t.Errorf("unmounted count = %d, want 0 (vol-1 was not rolled back)", len(fake.unmounted))
		}

		// Verify host mount directory for vol-1 exists and remains on disk.
		if _, err := os.Stat(expectedPath1); err != nil {
			t.Errorf("stat vol-1 mount path %q: %v", expectedPath1, err)
		}

		// Verify host mount directory for vol-2 was also created prior to MountVolume call.
		if _, err := os.Stat(expectedPath2); err != nil {
			t.Errorf("stat vol-2 mount path %q: %v", expectedPath2, err)
		}
	})
}

func TestVolumeHostDirectoryCleanup(t *testing.T) {
	ctx := context.Background()
	actorUID := "test-actor-cleanup"

	extVol1 := &ateletpb.Volume{
		Name: "vol-1",
		External: &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "mock-vol-1",
			VolumeType:      "mock-driver",
		},
	}
	extVol2 := &ateletpb.Volume{
		Name: "vol-2",
		External: &ateletpb.ExternalVolumeSource{
			StorageVolumeId: "mock-vol-2",
			VolumeType:      "mock-driver",
		},
	}

	t.Run("unmountExternalVolumes preserves host mount directories", func(t *testing.T) {
		useTempNodeDirs(t)

		fake := &fakeWorkerPlugin{}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		if err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("mountExternalVolumes failed: %v", err)
		}

		path1 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-1")
		path2 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-2")
		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("expected mount path %q to exist before unmount: %v", p, err)
			}
		}

		if err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("unmountExternalVolumes failed: %v", err)
		}

		// unmountExternalVolumes unmounts the CSI volumes, but preserves the host directories.
		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); err != nil {
				t.Errorf("mount path %q was unexpectedly deleted by unmountExternalVolumes: %v", p, err)
			}
		}
	})

	t.Run("ResetActorDirs removes empty unmounted volume host directories", func(t *testing.T) {
		useTempNodeDirs(t)

		fake := &fakeWorkerPlugin{}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		// Mount and then unmount external volumes.
		if err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("mountExternalVolumes failed: %v", err)
		}
		if err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2}); err != nil {
			t.Fatalf("unmountExternalVolumes failed: %v", err)
		}

		path1 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-1")
		path2 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-2")
		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); err != nil {
				t.Fatalf("expected mount path %q to exist before reset: %v", p, err)
			}
		}

		// actordirs.ResetActorDirs cleans up empty volume directories left after unmount.
		if err := actordirs.ResetActorDirs(actorUID); err != nil {
			t.Fatalf("ResetActorDirs failed: %v", err)
		}

		for _, p := range []string{path1, path2} {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Errorf("expected mount path %q to be deleted by ResetActorDirs, stat err = %v", p, err)
			}
		}

		// The parent volumes directory is preserved and empty.
		volumesDir := ateletpath.ExternalVolumeMountsDir(actorUID)
		entries, err := os.ReadDir(volumesDir)
		if err != nil {
			t.Fatalf("ReadDir(%q) failed: %v", volumesDir, err)
		}
		if len(entries) != 0 {
			t.Errorf("expected volumes dir to be empty, found %d entries", len(entries))
		}
	})

	t.Run("ResetActorDirs preserves non-empty volume directories and returns error", func(t *testing.T) {
		useTempNodeDirs(t)

		fake := &fakeWorkerPlugin{}
		s := &Atelet{
			volumePlugins: map[string]volume.VolumePluginWorkerPlane{
				"mock-driver": fake,
			},
		}

		if err := s.mountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1}); err != nil {
			t.Fatalf("mountExternalVolumes failed: %v", err)
		}

		// Simulate lingering content in the volume mount point (e.g. unmount failed or data present).
		path1 := ateletpath.ExternalVolumeHostPath(actorUID, "vol-1")
		filePath := filepath.Join(path1, "lingering.txt")
		if err := os.WriteFile(filePath, []byte("volume data"), 0o600); err != nil {
			t.Fatalf("writing test file: %v", err)
		}

		err := actordirs.ResetActorDirs(actorUID)
		if err == nil {
			t.Fatal("expected ResetActorDirs to fail on non-empty volume dir, got nil")
		}
		if !strings.Contains(err.Error(), "while removing volume dir") {
			t.Errorf("error = %v, want error containing 'while removing volume dir'", err)
		}

		// Verify data is preserved and not deleted.
		if _, err := os.Stat(filePath); err != nil {
			t.Errorf("stat %q: %v (data was unexpectedly deleted)", filePath, err)
		}
	})

	t.Run("ResetActorDirs succeeds cleanly when volumes directory does not exist", func(t *testing.T) {
		useTempNodeDirs(t)

		if err := actordirs.ResetActorDirs(actorUID); err != nil {
			t.Fatalf("ResetActorDirs failed when volumes dir does not exist: %v", err)
		}

		volumesDir := ateletpath.ExternalVolumeMountsDir(actorUID)
		if info, err := os.Stat(volumesDir); err != nil || !info.IsDir() {
			t.Errorf("expected volumes dir %q to be created, stat err = %v", volumesDir, err)
		}
	})
}

func TestDirectCSIDriverConfigGetter(t *testing.T) {
	fakeClient := fake.NewSimpleClientset(
		&atev1.CSIDriverConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "test.csi.k8s.io"},
			Spec:       atev1.CSIDriverConfigSpec{DriverName: "test.csi.k8s.io", ControllerEndpoint: "tcp://127.0.0.1:9000"},
		},
		&atev1.CSIDriverConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "custom-nfs-config"},
			Spec:       atev1.CSIDriverConfigSpec{DriverName: "nfs.csi.k8s.io", ControllerEndpoint: "tcp://127.0.0.1:9001", NodeSocketOverride: "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock"},
		},
		&atev1.CSIDriverConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "dup-b"},
			Spec:       atev1.CSIDriverConfigSpec{DriverName: "dup.csi.k8s.io", ControllerEndpoint: "tcp://127.0.0.1:9002"},
		},
		&atev1.CSIDriverConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "dup-a"},
			Spec:       atev1.CSIDriverConfigSpec{DriverName: "dup.csi.k8s.io", ControllerEndpoint: "tcp://127.0.0.1:9003"},
		},
	)
	getter := &directCSIDriverConfigGetter{client: fakeClient}
	ctx := t.Context()

	cfg, err := getter.Get(ctx, "test.csi.k8s.io")
	if err != nil || cfg.GetDriverName() != "test.csi.k8s.io" {
		t.Fatalf("Get(test.csi.k8s.io) = (%v, %v)", cfg, err)
	}

	nfsCfg, err := getter.Get(ctx, "nfs.csi.k8s.io")
	if err != nil || nfsCfg.GetMetadata().GetName() != "custom-nfs-config" || nfsCfg.GetDriverName() != "nfs.csi.k8s.io" {
		t.Fatalf("Get(nfs.csi.k8s.io) = (%v, %v)", nfsCfg, err)
	}
	if nfsCfg.GetNodeSocketOverride() != "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock" {
		t.Errorf("got NodeSocketOverride %q", nfsCfg.GetNodeSocketOverride())
	}

	if _, err := getter.Get(ctx, "dup.csi.k8s.io"); err == nil || !strings.Contains(err.Error(), `multiple CSIDriverConfigs match driver "dup.csi.k8s.io": dup-a, dup-b`) {
		t.Fatalf("Get(dup.csi.k8s.io) error = %v, want deterministic duplicate error", err)
	}

	if _, err := getter.Get(ctx, "missing.csi.k8s.io"); err == nil {
		t.Fatal("expected error for missing driver, got nil")
	}

	listBoom := errors.New("apiserver list unavailable")
	fakeClient.PrependReactor("list", "csidriverconfigs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, listBoom
	})
	if _, err := getter.Get(ctx, "nfs.csi.k8s.io"); !errors.Is(err, listBoom) {
		t.Fatalf("Get with failing List = %v, want %v", err, listBoom)
	}

	getBoom := errors.New("apiserver get forbidden")
	listCalled := false
	fakeClient.PrependReactor("list", "csidriverconfigs", func(k8stesting.Action) (bool, runtime.Object, error) {
		listCalled = true
		return true, nil, listBoom
	})
	fakeClient.PrependReactor("get", "csidriverconfigs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, getBoom
	})
	if _, err := getter.Get(ctx, "nfs.csi.k8s.io"); !errors.Is(err, getBoom) || listCalled {
		t.Fatalf("Get with non-NotFound error = %v (listCalled=%t), want %v without List", err, listCalled, getBoom)
	}
}

type fakeCsiDriverConfigListClient struct {
	mu    sync.Mutex
	pages map[string]*ateapi.ListCsiDriverConfigsResponse
	err   error
}

func (f *fakeCsiDriverConfigListClient) ListCsiDriverConfigs(_ context.Context, req *ateapi.ListCsiDriverConfigsRequest, _ ...grpc.CallOption) (*ateapi.ListCsiDriverConfigsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if resp, ok := f.pages[req.GetOptions().GetPageToken()]; ok {
		return resp, nil
	}
	return &ateapi.ListCsiDriverConfigsResponse{}, nil
}

func (f *fakeCsiDriverConfigListClient) setSingle(cfg *ateapi.CsiDriverConfig, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err, f.pages = err, nil
	if cfg != nil {
		f.pages = map[string]*ateapi.ListCsiDriverConfigsResponse{"": {CsiDriverConfigs: []*ateapi.CsiDriverConfig{cfg}}}
	}
}

type spyCSIGetter struct {
	inner csi.CSIDriverConfigGetter
	calls atomic.Int32
}

func (s *spyCSIGetter) Get(ctx context.Context, driverName string) (*ateapi.CsiDriverConfig, error) {
	s.calls.Add(1)
	return s.inner.Get(ctx, driverName)
}

func TestAteapiCSIDriverConfigGetter(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	crdClient := fake.NewSimpleClientset(
		&atev1.CSIDriverConfig{ObjectMeta: metav1.ObjectMeta{Name: "crd-nfs"}, Spec: atev1.CSIDriverConfigSpec{DriverName: "nfs.csi.k8s.io", NodeSocketOverride: "unix:///crd-nfs.sock"}},
		&atev1.CSIDriverConfig{ObjectMeta: metav1.ObjectMeta{Name: "crd-pd"}, Spec: atev1.CSIDriverConfigSpec{DriverName: "pd.csi.storage.gke.io", NodeSocketOverride: "unix:///crd-pd.sock"}},
	)
	crdFallback := &directCSIDriverConfigGetter{client: crdClient}
	control := &fakeCsiDriverConfigListClient{pages: map[string]*ateapi.ListCsiDriverConfigsResponse{
		"":   {CsiDriverConfigs: []*ateapi.CsiDriverConfig{{Metadata: &ateapi.ResourceMetadata{Name: "other"}, DriverName: "other.csi.io"}}, NextPageToken: "p2"},
		"p2": {CsiDriverConfigs: []*ateapi.CsiDriverConfig{{Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs"}, DriverName: "nfs.csi.k8s.io", NodeSocketOverride: "unix:///ateapi-nfs.sock"}}},
	}}
	spy := &spyCSIGetter{inner: crdFallback}
	getter := &ateapiCSIDriverConfigGetter{control: control, fallback: spy}

	if nfs, err := getter.Get(ctx, "nfs.csi.k8s.io"); err != nil || nfs.GetNodeSocketOverride() != "unix:///ateapi-nfs.sock" || spy.calls.Load() != 0 {
		t.Fatalf("Get(nfs) = (%v, %v), fallbackCalls = %d", nfs, err, spy.calls.Load())
	}
	if pd, err := getter.Get(ctx, "pd.csi.storage.gke.io"); err != nil || pd.GetNodeSocketOverride() != "unix:///crd-pd.sock" || spy.calls.Load() != 1 {
		t.Fatalf("Get(pd) = (%v, %v), fallbackCalls = %d", pd, err, spy.calls.Load())
	}
	if _, err := getter.Get(ctx, "missing.csi.io"); !apierrors.IsNotFound(err) {
		t.Fatalf("Get(missing) error = %v, want apierrors.IsNotFound", err)
	}
	if _, err := (&ateapiCSIDriverConfigGetter{control: control}).Get(ctx, "missing.csi.io"); status.Code(err) != codes.NotFound {
		t.Fatalf("Get(missing, nil fallback) code = %v, want NotFound", status.Code(err))
	}

	spy.calls.Store(0)
	control.err = status.Error(codes.Unimplemented, "unknown method ListCsiDriverConfigs")
	if got, err := getter.Get(ctx, "nfs.csi.k8s.io"); err != nil || got.GetNodeSocketOverride() != "unix:///crd-nfs.sock" || spy.calls.Load() != 1 {
		t.Fatalf("Get(Unimplemented) = (%v, %v), fallbackCalls = %d", got, err, spy.calls.Load())
	}
	if _, err := (&ateapiCSIDriverConfigGetter{control: control}).Get(ctx, "nfs.csi.k8s.io"); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Get(Unimplemented, nil fallback) = %v, want Unimplemented", err)
	}
	for _, code := range []codes.Code{codes.Unavailable, codes.PermissionDenied} {
		spy.calls.Store(0)
		control.err = status.Error(code, "rpc failed")
		if _, err := getter.Get(ctx, "nfs.csi.k8s.io"); status.Code(err) != code || spy.calls.Load() != 0 {
			t.Fatalf("Get(%v) = %v (fallbackCalls=%d), want %v without fallback", code, err, spy.calls.Load(), code)
		}
	}
	if got, err := (&ateapiCSIDriverConfigGetter{fallback: crdFallback}).Get(ctx, "pd.csi.storage.gke.io"); err != nil || got.GetDriverName() != "pd.csi.storage.gke.io" {
		t.Fatalf("Get(nil control) = (%v, %v)", got, err)
	}
	if _, err := (&ateapiCSIDriverConfigGetter{}).Get(ctx, "pd.csi.storage.gke.io"); err == nil {
		t.Fatal("Get(nil control, nil fallback) = nil, want error")
	}
}

type testCSINodeServer struct {
	csipb.UnimplementedIdentityServer
	csipb.UnimplementedNodeServer
	driverName  string
	publishes   atomic.Int32
	onGetPlugin func()
}

func (s *testCSINodeServer) GetPluginInfo(context.Context, *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	if s.onGetPlugin != nil {
		s.onGetPlugin()
	}
	return &csipb.GetPluginInfoResponse{Name: s.driverName, VendorVersion: "v1.0.0"}, nil
}

func (s *testCSINodeServer) NodeGetCapabilities(context.Context, *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	rpc := &csipb.NodeServiceCapability_Rpc{Rpc: &csipb.NodeServiceCapability_RPC{Type: csipb.NodeServiceCapability_RPC_GET_VOLUME_STATS}}
	return &csipb.NodeGetCapabilitiesResponse{Capabilities: []*csipb.NodeServiceCapability{{Type: rpc}}}, nil
}

func (s *testCSINodeServer) NodePublishVolume(context.Context, *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	s.publishes.Add(1)
	return &csipb.NodePublishVolumeResponse{}, nil
}

func startTestCSINodeSocket(t *testing.T, sockDir, file, driver string, onGetPlugin func()) (string, *testCSINodeServer) {
	t.Helper()
	sockPath := filepath.Join(sockDir, file)
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix %q: %v", sockPath, err)
	}
	srv, impl := grpc.NewServer(), &testCSINodeServer{driverName: driver, onGetPlugin: onGetPlugin}
	csipb.RegisterIdentityServer(srv, impl)
	csipb.RegisterNodeServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); _ = lis.Close() })
	return "unix://" + sockPath, impl
}

func cleanupAteletCSIPlugins(t *testing.T, s *Atelet) *Atelet {
	t.Helper()
	t.Cleanup(func() {
		s.mu.Lock()
		slots := maps.Clone(s.csiPlugins)
		clear(s.csiPlugins)
		s.mu.Unlock()
		for _, slot := range slots {
			_ = slot.plugin.Close()
		}
	})
	return s
}

func assertNodePluginClosed(t *testing.T, p volume.VolumePluginWorkerPlane) {
	t.Helper()
	if err := p.MountVolume(t.Context(), volume.MountVolumeRequest{VolumeID: "v", TargetPath: "/tmp/unused"}); status.Code(err) != codes.Canceled {
		t.Fatalf("MountVolume() on closed plugin = %v, want Canceled", err)
	}
}

func TestAteletGetPlugin(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	sockDir, err := os.MkdirTemp("/tmp", "atelet-csi-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	t.Run("static override and nil getter", func(t *testing.T) {
		t.Parallel()
		static, spy := &fakeWorkerPlugin{}, &spyCSIGetter{inner: &ateapiCSIDriverConfigGetter{}}
		s := &Atelet{volumePlugins: map[string]volume.VolumePluginWorkerPlane{"nfs.csi.k8s.io": static}, csiDriverConfigGetter: spy}
		if got, err := s.getPlugin(ctx, "nfs.csi.k8s.io"); err != nil || got != static || spy.calls.Load() != 0 {
			t.Fatalf("getPlugin(static) = (%v, %v), getterCalls = %d", got, err, spy.calls.Load())
		}
		if _, err := (&Atelet{}).getPlugin(ctx, "nfs.csi.k8s.io"); err == nil {
			t.Fatal("getPlugin(nil getter) = nil, want error")
		}
	})

	t.Run("reuse, socket swap, bad redial, transient error, and delete", func(t *testing.T) {
		t.Parallel()
		const driver = "nfs.csi.k8s.io"
		sock1, srv1 := startTestCSINodeSocket(t, sockDir, "s1.sock", driver, nil)
		sock2, srv2 := startTestCSINodeSocket(t, sockDir, "s2.sock", driver, nil)
		crdClient := fake.NewSimpleClientset(&atev1.CSIDriverConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "crd-nfs"},
			Spec:       atev1.CSIDriverConfigSpec{DriverName: driver, ControllerEndpoint: "tcp://127.0.0.1:9000", NodeSocketOverride: sock1},
		})
		control := &fakeCsiDriverConfigListClient{}
		s := cleanupAteletCSIPlugins(t, &Atelet{csiDriverConfigGetter: &ateapiCSIDriverConfigGetter{control: control, fallback: &directCSIDriverConfigGetter{client: crdClient}}})

		mustMount := func(wantSrv *testCSINodeServer, wantCount int32) *csi.Plugin {
			t.Helper()
			got, err := s.getPlugin(ctx, driver)
			p, ok := got.(*csi.Plugin)
			if err != nil || !ok {
				t.Fatalf("getPlugin() = (%T, %v), want *csi.Plugin", got, err)
			}
			if err := p.MountVolume(ctx, volume.MountVolumeRequest{VolumeID: "v1", TargetPath: filepath.Join(sockDir, "mnt")}); err != nil || wantSrv.publishes.Load() != wantCount {
				t.Fatalf("MountVolume() err = %v, publishes = %d (want %d)", err, wantSrv.publishes.Load(), wantCount)
			}
			return p
		}

		p1 := mustMount(srv1, 1)
		control.setSingle(&ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u1", Version: 1}, DriverName: driver, ControllerEndpoint: "tcp://127.0.0.1:9001", NodeSocketOverride: sock1}, nil)
		if p := mustMount(srv1, 2); p != p1 {
			t.Fatalf("promotion changed plugin pointer %p -> %p", p1, p)
		}
		control.setSingle(&ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u1", Version: 2}, DriverName: driver, ControllerEndpoint: "tcp://127.0.0.1:9999", NodeSocketOverride: sock1, Tls: &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "x"}}, nil)
		if p := mustMount(srv1, 3); p != p1 {
			t.Fatalf("controller/tls edit changed node plugin pointer %p -> %p", p1, p)
		}
		control.setSingle(nil, nil)
		if p := mustMount(srv1, 4); p != p1 {
			t.Fatalf("demotion to identical CRD changed plugin pointer %p -> %p", p1, p)
		}

		control.setSingle(&ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u1", Version: 3}, DriverName: driver, NodeSocketOverride: sock2}, nil)
		p2 := mustMount(srv2, 1)
		if p2 == p1 {
			t.Fatal("socket update returned stale plugin")
		}
		assertNodePluginClosed(t, p1)

		control.setSingle(nil, status.Error(codes.Unavailable, "api-server restarting"))
		if _, err := s.getPlugin(ctx, driver); status.Code(err) != codes.Unavailable {
			t.Fatalf("getPlugin(Unavailable) = %v, want Unavailable", err)
		}
		if err := p2.MountVolume(ctx, volume.MountVolumeRequest{VolumeID: "v1", TargetPath: filepath.Join(sockDir, "mnt")}); err != nil {
			t.Fatalf("p2 closed on transient error: %v", err)
		}

		control.setSingle(&ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u1", Version: 4}, DriverName: driver, NodeSocketOverride: "unix://" + filepath.Join(sockDir, "missing.sock")}, nil)
		if _, err := s.getPlugin(ctx, driver); err == nil {
			t.Fatal("getPlugin(missing.sock) = nil, want dial error")
		}
		assertNodePluginClosed(t, p2)

		control.setSingle(&ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u1", Version: 5}, DriverName: driver, NodeSocketOverride: sock2}, nil)
		p3 := mustMount(srv2, 3)
		control.setSingle(nil, nil)
		if err := crdClient.AteV1().CSIDriverConfigs().Delete(ctx, "crd-nfs", metav1.DeleteOptions{}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.getPlugin(ctx, driver); !apierrors.IsNotFound(err) {
			t.Fatalf("getPlugin(deleted) = %v, want IsNotFound", err)
		}
		assertNodePluginClosed(t, p3)
		if len(s.csiPlugins) != 0 {
			t.Fatalf("len(csiPlugins) = %d, want 0", len(s.csiPlugins))
		}
	})

	for _, mode := range []string{"update", "delete"} {
		t.Run("concurrent slow dial vs "+mode, func(t *testing.T) {
			t.Parallel()
			driver := "race-" + mode + ".csi.k8s.io"
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			slowSock, _ := startTestCSINodeSocket(t, sockDir, mode+"-1.sock", driver, func() { once.Do(func() { close(entered); <-release }) })
			fastSock, fastSrv := startTestCSINodeSocket(t, sockDir, mode+"-2.sock", driver, nil)
			control := &fakeCsiDriverConfigListClient{}
			control.setSingle(&ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "cfg"}, DriverName: driver, NodeSocketOverride: slowSock}, nil)
			s := cleanupAteletCSIPlugins(t, &Atelet{csiDriverConfigGetter: &ateapiCSIDriverConfigGetter{control: control}})

			var gotPlugin, wantPlugin volume.VolumePluginWorkerPlane
			var gotErr, err error
			var wg sync.WaitGroup
			wg.Go(func() { gotPlugin, gotErr = s.getPlugin(ctx, driver) })
			<-entered
			if mode == "update" {
				control.setSingle(&ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "cfg"}, DriverName: driver, NodeSocketOverride: fastSock}, nil)
				if wantPlugin, err = s.getPlugin(ctx, driver); err != nil {
					t.Fatalf("fast getPlugin() error = %v", err)
				}
			} else {
				control.setSingle(nil, nil)
				if _, err := s.getPlugin(ctx, driver); status.Code(err) != codes.NotFound {
					t.Fatalf("delete getPlugin() = %v, want NotFound", err)
				}
			}
			close(release)
			wg.Wait()
			if mode == "delete" {
				if status.Code(gotErr) != codes.NotFound || len(s.csiPlugins) != 0 {
					t.Fatalf("slow getPlugin() = (%v, %v), slots = %d; want NotFound and 0 slots", gotPlugin, gotErr, len(s.csiPlugins))
				}
				return
			}
			if gotErr != nil || gotPlugin != wantPlugin || s.csiPlugins[driver].inflight != 0 {
				t.Fatalf("slow getPlugin() = (%v, %v), inflight = %d; want (%v, nil)", gotPlugin, gotErr, s.csiPlugins[driver].inflight, wantPlugin)
			}
			if err := gotPlugin.MountVolume(ctx, volume.MountVolumeRequest{VolumeID: "v", TargetPath: filepath.Join(sockDir, "m")}); err != nil || fastSrv.publishes.Load() != 1 {
				t.Fatalf("MountVolume on winner = %v, fastSrv.publishes = %d", err, fastSrv.publishes.Load())
			}
		})
	}
}
