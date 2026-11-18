// Copyright 2026 The Agent Substrate Authors
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

package controlapi

import (
	"context"
	"errors"
	"maps"
	"net"
	"strings"
	"sync"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/internal/volume/csi"
	atelisters "go.ate.dev/substrate/pkg/client/listers/ate/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

type testCSIServer struct {
	csipb.UnimplementedIdentityServer
	csipb.UnimplementedControllerServer
	driverName  string
	volumeID    string
	onGetPlugin func()
}

func (s *testCSIServer) GetPluginInfo(context.Context, *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	if s.onGetPlugin != nil {
		s.onGetPlugin()
	}
	return &csipb.GetPluginInfoResponse{Name: s.driverName, VendorVersion: "v1.0.0"}, nil
}

func (s *testCSIServer) CreateVolume(context.Context, *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	return &csipb.CreateVolumeResponse{Volume: &csipb.Volume{VolumeId: s.volumeID}}, nil
}

func startTestCSIServer(t *testing.T, driverName, volumeID string, onGetPlugin func()) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	srv := grpc.NewServer()
	impl := &testCSIServer{driverName: driverName, volumeID: volumeID, onGetPlugin: onGetPlugin}
	csipb.RegisterIdentityServer(srv, impl)
	csipb.RegisterControllerServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); _ = lis.Close() })
	return "tcp://" + lis.Addr().String()
}

type faultyServiceStore struct {
	serviceStore
	err error
}

func (f *faultyServiceStore) GetCsiDriverConfigByDriverName(ctx context.Context, driverName string) (*ateapi.CsiDriverConfig, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.serviceStore.GetCsiDriverConfigByDriverName(ctx, driverName)
}

func cleanupCSIPlugins(t *testing.T, svc *RPCService) *RPCService {
	t.Helper()
	t.Cleanup(func() {
		svc.mu.Lock()
		slots := maps.Clone(svc.csiPlugins)
		clear(svc.csiPlugins)
		svc.mu.Unlock()
		for _, slot := range slots {
			_ = slot.plugin.Close()
		}
	})
	return svc
}

func newCRDIndexer(driverName, ep string) (cache.Indexer, *atev1.CSIDriverConfig, atelisters.CSIDriverConfigLister) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	crd := &atev1.CSIDriverConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "crd-" + driverName, UID: types.UID("crd-uid"), ResourceVersion: "5"},
		Spec:       atev1.CSIDriverConfigSpec{DriverName: driverName, ControllerEndpoint: ep},
	}
	_ = indexer.Add(crd)
	return indexer, crd, atelisters.NewCSIDriverConfigLister(indexer)
}

func assertPluginClosed(t *testing.T, plugin volume.VolumePluginControlPlane) {
	t.Helper()
	_, err := plugin.CreateVolume(t.Context(), volume.CreateVolumeRequest{Name: "closed", CapacityBytes: 1 << 20})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("CreateVolume() on evicted plugin = %v (code %v), want %v", err, status.Code(err), codes.Canceled)
	}
}

func mustGetCSIPlugin(t *testing.T, svc *RPCService, driverName, wantVolID string) *csi.Plugin {
	t.Helper()
	got, err := svc.GetPlugin(t.Context(), driverName)
	plugin, ok := got.(*csi.Plugin)
	if err != nil || !ok {
		t.Fatalf("GetPlugin(%q) = (%T, %v), want (*csi.Plugin, nil)", driverName, got, err)
	}
	if resp, err := plugin.CreateVolume(t.Context(), volume.CreateVolumeRequest{Name: "vol", CapacityBytes: 1 << 20}); err != nil || resp.VolumeID != wantVolID {
		t.Fatalf("CreateVolume() = (%q, %v), want (%q, nil)", resp.VolumeID, err, wantVolID)
	}
	return plugin
}

func mustGetSamePlugin(t *testing.T, svc *RPCService, driverName, wantVolID string, want *csi.Plugin) {
	t.Helper()
	if got := mustGetCSIPlugin(t, svc, driverName, wantVolID); got != want {
		t.Fatalf("GetPlugin(%q) = %p, want preserved %p", driverName, got, want)
	}
}
func mustGetSwappedPlugin(t *testing.T, svc *RPCService, driverName, wantVolID string, prev *csi.Plugin) *csi.Plugin {
	t.Helper()
	got := mustGetCSIPlugin(t, svc, driverName, wantVolID)
	if got == prev {
		t.Fatalf("GetPlugin(%q) returned stale %p", driverName, prev)
	}
	assertPluginClosed(t, prev)
	return got
}
func mustFailGetPlugin(t *testing.T, svc *RPCService, driverName string, wantErr error, wantSubstr string) {
	t.Helper()
	_, err := svc.GetPlugin(t.Context(), driverName)
	if err == nil || errors.Is(err, store.ErrNotFound) || (wantErr != nil && !errors.Is(err, wantErr)) || !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("GetPlugin(%q) error = %v, want non-store.ErrNotFound matching (%v, %q)", driverName, err, wantErr, wantSubstr)
	}
}

func mustCreateCfg(t *testing.T, st store.Interface, name, driverName, ep string) *ateapi.CsiDriverConfig {
	t.Helper()
	cfg := &ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: name}, DriverName: driverName, ControllerEndpoint: ep}
	created, err := st.CreateCsiDriverConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("CreateCsiDriverConfig(%q) error = %v", name, err)
	}
	t.Cleanup(func() { _, _ = st.DeleteCsiDriverConfig(context.Background(), name, store.DeletePreconditions{}) })
	return created
}
func mustUpdateCfg(t *testing.T, st store.Interface, prev *ateapi.CsiDriverConfig, ep, sock string, tls *ateapi.CsiDriverTlsConfig) *ateapi.CsiDriverConfig {
	t.Helper()
	updated, err := st.UpdateCsiDriverConfig(t.Context(), prev.GetMetadata().GetName(), store.PreconditionFrom(prev), func(cfg *ateapi.CsiDriverConfig) error {
		cfg.ControllerEndpoint, cfg.NodeSocketOverride, cfg.Tls = ep, sock, tls
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateCsiDriverConfig(%q) error = %v", prev.GetMetadata().GetName(), err)
	}
	return updated
}
func mustDeleteCfg(t *testing.T, st store.Interface, name string) {
	t.Helper()
	if _, err := st.DeleteCsiDriverConfig(t.Context(), name, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteCsiDriverConfig(%q) error = %v", name, err)
	}
}

func TestRPCServiceGetPlugin(t *testing.T) {
	st := storetest.SetupPostgresPersistence(t)
	ctx := t.Context()

	t.Run("static override and cache key fields", func(t *testing.T) {
		base := &ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "a", Uid: "u1", Version: 1}, ControllerEndpoint: "tcp://127.0.0.1:9000", NodeSocketOverride: "unix:///a.sock"}
		same := &ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: "b", Uid: "u2", Version: 9}, ControllerEndpoint: "tcp://127.0.0.1:9000", NodeSocketOverride: "unix:///b.sock"}
		if newCSIPluginCacheKey(base) != newCSIPluginCacheKey(same) {
			t.Fatalf("newCSIPluginCacheKey() differed across metadata or node_socket_override")
		}
		for _, tls := range []*ateapi.CsiDriverTlsConfig{{Enabled: true}, {UsePodIdentity: true}, {ServerName: "s"}} {
			if newCSIPluginCacheKey(base) == newCSIPluginCacheKey(&ateapi.CsiDriverConfig{ControllerEndpoint: base.ControllerEndpoint, Tls: tls}) {
				t.Fatalf("newCSIPluginCacheKey() did not distinguish %v", tls)
			}
		}
		if newCSIPluginCacheKey(base) == newCSIPluginCacheKey(&ateapi.CsiDriverConfig{ControllerEndpoint: "tcp://127.0.0.1:9001"}) {
			t.Fatalf("newCSIPluginCacheKey() did not distinguish controller_endpoint")
		}
		staticPlugin := volume.NewMockVolumePlugin()
		plugins := map[string]volume.VolumePluginControlPlane{"nfs.csi.k8s.io": staticPlugin}
		svc := NewRPCService(nil, nil, nil, nil, nil, nil, "", plugins, nil, "", nil, "")
		svc.impl = &faultyServiceStore{err: errors.New("unexpected store call")}
		delete(plugins, "nfs.csi.k8s.io")
		if got, err := svc.GetPlugin(ctx, "nfs.csi.k8s.io"); err != nil || got != staticPlugin {
			t.Fatalf("GetPlugin() = (%v, %v), want static override", got, err)
		}
	})

	t.Run("postgres lookup, node_socket_override reuse, endpoint/tls invalidation, recreate, and delete", func(t *testing.T) {
		const cfgName, driverName = "csi-nfs", "nfs.csi.k8s.io"
		ep1, ep2 := startTestCSIServer(t, driverName, "vol-ep1", nil), startTestCSIServer(t, driverName, "vol-ep2", nil)
		svc := cleanupCSIPlugins(t, NewRPCService(st, nil, nil, nil, nil, nil, "", nil, nil, "", nil, ""))
		v1 := mustCreateCfg(t, st, cfgName, driverName, ep1)
		p1 := mustGetCSIPlugin(t, svc, driverName, "vol-ep1")
		v2 := mustUpdateCfg(t, st, v1, ep1, "unix:///var/lib/kubelet/plugins/nfs/csi.sock", nil)
		mustGetSamePlugin(t, svc, driverName, "vol-ep1", p1)
		mustUpdateCfg(t, st, v2, ep2, "", nil)
		p2 := mustGetSwappedPlugin(t, svc, driverName, "vol-ep2", p1)
		mustDeleteCfg(t, st, cfgName)
		recreated := mustCreateCfg(t, st, cfgName, driverName, ep1)
		p3 := mustGetSwappedPlugin(t, svc, driverName, "vol-ep1", p2)
		withTLS := mustUpdateCfg(t, st, recreated, ep1, "", &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi-nfs.kube-system.svc"})
		mustFailGetPlugin(t, svc, driverName, nil, "TLS")
		assertPluginClosed(t, p3)
		mustUpdateCfg(t, st, withTLS, ep2, "", nil)
		p4 := mustGetCSIPlugin(t, svc, driverName, "vol-ep2")
		mustDeleteCfg(t, st, cfgName)
		mustFailGetPlugin(t, svc, driverName, nil, "not found")
		assertPluginClosed(t, p4)
		if len(svc.csiPlugins) != 0 {
			t.Fatalf("len(csiPlugins) = %d, want 0", len(svc.csiPlugins))
		}
	})

	t.Run("crd fallback, identical promotion/demotion reuse, and differing endpoint swap", func(t *testing.T) {
		const driverName = "pd.csi.storage.gke.io"
		epCRD, epPG := startTestCSIServer(t, driverName, "vol-crd", nil), startTestCSIServer(t, driverName, "vol-pg", nil)
		indexer, crdObj, lister := newCRDIndexer(driverName, epCRD)
		svc := cleanupCSIPlugins(t, NewRPCService(st, nil, lister, nil, nil, nil, "", nil, nil, "", nil, ""))
		nilImplSvc := cleanupCSIPlugins(t, &RPCService{csiDriverConfigLister: lister})
		nilImplPlugin := mustGetCSIPlugin(t, nilImplSvc, driverName, "vol-crd")
		crdPlugin := mustGetCSIPlugin(t, svc, driverName, "vol-crd")

		crdObjBumped := crdObj.DeepCopy()
		crdObjBumped.ResourceVersion, crdObjBumped.Spec.NodeSocketOverride = "6", "unix:///var/lib/kubelet/plugins/pd/csi.sock"
		_ = indexer.Update(crdObjBumped)
		mustGetSamePlugin(t, svc, driverName, "vol-crd", crdPlugin)
		mustCreateCfg(t, st, "csi-pd-same", driverName, epCRD)
		mustGetSamePlugin(t, svc, driverName, "vol-crd", crdPlugin)
		mustDeleteCfg(t, st, "csi-pd-same")
		mustGetSamePlugin(t, svc, driverName, "vol-crd", crdPlugin)
		mustCreateCfg(t, st, "csi-pd", driverName, epPG)
		pgPlugin := mustGetSwappedPlugin(t, svc, driverName, "vol-pg", crdPlugin)
		mustDeleteCfg(t, st, "csi-pd")
		crdPlugin2 := mustGetSwappedPlugin(t, svc, driverName, "vol-crd", pgPlugin)
		_ = indexer.Delete(crdObjBumped)
		mustFailGetPlugin(t, svc, driverName, nil, "not found")
		assertPluginClosed(t, crdPlugin2)
		mustFailGetPlugin(t, nilImplSvc, driverName, nil, "not found")
		assertPluginClosed(t, nilImplPlugin)
	})

	t.Run("transient store error fails loudly without fallback or eviction", func(t *testing.T) {
		const driverName = "lustre.csi.storage.gke.io"
		epPG, epCRD := startTestCSIServer(t, driverName, "vol-lustre-pg", nil), startTestCSIServer(t, driverName, "vol-lustre-crd", nil)
		_, _, lister := newCRDIndexer(driverName, epCRD)
		faulty := &faultyServiceStore{serviceStore: newServiceImpl(st, nil)}
		svc := cleanupCSIPlugins(t, &RPCService{impl: faulty, csiDriverConfigLister: lister})
		mustCreateCfg(t, st, "csi-lustre", driverName, epPG)
		cachedPlugin := mustGetCSIPlugin(t, svc, driverName, "vol-lustre-pg")

		dbErr := errors.New("database connection lost")
		faulty.err = dbErr
		mustFailGetPlugin(t, svc, driverName, dbErr, "")
		if resp, err := cachedPlugin.CreateVolume(ctx, volume.CreateVolumeRequest{Name: "v", CapacityBytes: 1 << 20}); err != nil || resp.VolumeID != "vol-lustre-pg" {
			t.Fatalf("cachedPlugin.CreateVolume() during store error = (%q, %v)", resp.VolumeID, err)
		}
		faulty.err = nil
		mustGetSamePlugin(t, svc, driverName, "vol-lustre-pg", cachedPlugin)
	})

	for _, mode := range []string{"update", "recreate", "delete"} {
		t.Run("concurrent slow dial vs "+mode, func(t *testing.T) {
			cfgName, driver := "csi-race-"+mode, "race-"+mode+".csi.k8s.io"
			enteredEp1, releaseEp1 := make(chan struct{}), make(chan struct{})
			var once sync.Once
			ep1 := startTestCSIServer(t, driver, "vol-v1", func() { once.Do(func() { close(enteredEp1); <-releaseEp1 }) })
			ep2 := startTestCSIServer(t, driver, "vol-v2", nil)
			svc := cleanupCSIPlugins(t, NewRPCService(st, nil, nil, nil, nil, nil, "", nil, nil, "", nil, ""))
			created := mustCreateCfg(t, st, cfgName, driver, ep1)
			var gotPlugin volume.VolumePluginControlPlane
			var gotErr error
			var wg sync.WaitGroup
			wg.Go(func() { gotPlugin, gotErr = svc.GetPlugin(ctx, driver) })
			<-enteredEp1
			var wantPlugin *csi.Plugin
			switch mode {
			case "update":
				mustUpdateCfg(t, st, created, ep2, "", nil)
				wantPlugin = mustGetCSIPlugin(t, svc, driver, "vol-v2")
			case "recreate":
				mustDeleteCfg(t, st, cfgName)
				mustCreateCfg(t, st, cfgName, driver, ep2)
				wantPlugin = mustGetCSIPlugin(t, svc, driver, "vol-v2")
			case "delete":
				mustDeleteCfg(t, st, cfgName)
				mustFailGetPlugin(t, svc, driver, nil, "not found")
			}
			close(releaseEp1)
			wg.Wait()
			if mode == "delete" {
				if gotErr == nil || errors.Is(gotErr, store.ErrNotFound) || !strings.Contains(gotErr.Error(), "not found") || len(svc.csiPlugins) != 0 {
					t.Fatalf("concurrent GetPlugin() = (%v, %v), slots = %d; want non-store.ErrNotFound not-found error and 0 slots", gotPlugin, gotErr, len(svc.csiPlugins))
				}
				return
			}
			if gotErr != nil || gotPlugin != wantPlugin || svc.csiPlugins[driver].inflight != 0 {
				t.Fatalf("concurrent GetPlugin() = (%v, %v), inflight = %d; want (%v, nil) and inflight 0", gotPlugin, gotErr, svc.csiPlugins[driver].inflight, wantPlugin)
			}
		})
	}
}
