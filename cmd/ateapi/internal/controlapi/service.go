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
	"fmt"
	"maps"
	"sync"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/internal/volume/csi"
	atelisters "go.ate.dev/substrate/pkg/client/listers/ate/v1"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

type csiPluginCacheKey struct {
	controllerEndpoint string
	tlsEnabled         bool
	tlsUsePodIdentity  bool
	tlsServerName      string
}

type csiPluginSlot struct {
	gen      uint64
	inflight int
	key      csiPluginCacheKey
	plugin   *csi.Plugin
}

func newCSIPluginCacheKey(cfg *ateapi.CsiDriverConfig) csiPluginCacheKey {
	return csiPluginCacheKey{
		controllerEndpoint: cfg.GetControllerEndpoint(),
		tlsEnabled:         cfg.GetTls().GetEnabled(),
		tlsUsePodIdentity:  cfg.GetTls().GetUsePodIdentity(),
		tlsServerName:      cfg.GetTls().GetServerName(),
	}
}

// RPCService implements ateapi.ControlServer and provides the implementation of
// the RPC service.
//
// Methods on this service should be as light as possible, delegating to the
// ServiceImpl for business logic and invariants.
type RPCService struct {
	ateapi.UnimplementedControlServer
	impl                  serviceStore
	persistence           serviceStore
	workerCache           *workercache.Cache
	dialer                *AteletDialer
	csiDriverConfigLister atelisters.CSIDriverConfigLister
	actorWorkflow         *ActorWorkflow
	workerWorkflow        *WorkerWorkflow
	instruments           *Instruments
	staticVolumePlugins   map[string]volume.VolumePluginControlPlane
	mu                    sync.RWMutex
	csiPlugins            map[string]csiPluginSlot
	objectStorePlugin     objectstore.ControlProviderClient

	actorJWTIssuer string
	actorIDJWTPool localjwtauthority.Pool
}

var _ ateapi.ControlServer = (*RPCService)(nil)

// VolumePluginRegistry defines the interface for dynamic CSI plugin resolution.
type VolumePluginRegistry interface {
	GetPlugin(ctx context.Context, name string) (volume.VolumePluginControlPlane, error)
}

// NewRPCService creates an instance of the ControlServer service. This is what
// implements the outward-facing RPC interface.
//
// instruments may be nil; the record helpers no-op.
//
// objectStorePlugin may be nil, which leaves external snapshots in place instead
// of copying and releasing them. Only tests that never reach those steps pass
// nil; ate-api always builds one.
//
// actorJWTIssuer is copied verbatim into the iss claim of every actor JWT.
//
// snapshotLocation is ate-api-server's cluster-wide --snapshot-location; see
// resolveSnapshotLocation.
func NewRPCService(
	persistence store.Interface,
	workerCache *workercache.Cache,
	csiDriverConfigLister atelisters.CSIDriverConfigLister,
	storageClassLister storagev1listers.StorageClassLister,
	dialer *AteletDialer,
	instruments *Instruments,
	egressGatewayAddress string,
	volumePlugins map[string]volume.VolumePluginControlPlane,
	objectStorePlugin objectstore.ControlProviderClient,
	actorJWTIssuer string,
	actorIDJWTPool localjwtauthority.Pool,
	snapshotLocation string,
) *RPCService {
	impl := newServiceImpl(persistence, storageClassLister)
	impl.clusterSnapshotLocation = snapshotLocation
	s := &RPCService{
		impl:                  impl,
		persistence:           persistence,
		workerCache:           workerCache,
		csiDriverConfigLister: csiDriverConfigLister,
		dialer:                dialer,
		instruments:           instruments,
		staticVolumePlugins:   maps.Clone(volumePlugins),
		csiPlugins:            make(map[string]csiPluginSlot),
		objectStorePlugin:     objectStorePlugin,
		actorJWTIssuer:        actorJWTIssuer,
		actorIDJWTPool:        actorIDJWTPool,
	}
	s.actorWorkflow = NewActorWorkflow(impl, workerCache, dialer, storageClassLister, instruments, egressGatewayAddress, s, objectStorePlugin)
	s.actorWorkflow.clusterSnapshotLocation = snapshotLocation
	s.workerWorkflow = NewWorkerWorkflow(impl, dialer)
	return s
}

// serviceStore enumerates the exact storage methods needed by
// the control API and nothing more.
type serviceStore interface {
	CreateActor(ctx context.Context, actor *ateapi.Actor) (*ateapi.Actor, error)
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapi.Actor, error)
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapi.Actor) error) (*ateapi.Actor, error)
	ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.Actor], error)
	CreateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapi.EgressPolicy) (*ateapi.EgressPolicy, error)
	GetEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapi.EgressPolicy, error)
	UpdateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapi.EgressPolicy) error) (*ateapi.EgressPolicy, error)
	DeleteEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapi.EgressPolicy, error)
	GetTag(ctx context.Context, tagRef resources.TagRef) (*ateapi.Tag, error)
	ListTags(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.Tag], error)
	UpdateTag(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(toUpdate *ateapi.Tag) error) (*ateapi.Tag, error)
	DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapi.Tag, error)
	CreateAtespace(ctx context.Context, atespace *ateapi.Atespace) (*ateapi.Atespace, error)
	GetAtespace(ctx context.Context, name string) (*ateapi.Atespace, error)
	ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.Atespace], error)
	DeleteAtespace(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.Atespace, error)
	CreateExternalVolumeClass(ctx context.Context, class *ateapi.ExternalVolumeClass) (*ateapi.ExternalVolumeClass, error)
	GetExternalVolumeClass(ctx context.Context, name string) (*ateapi.ExternalVolumeClass, error)
	ListExternalVolumeClasses(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.ExternalVolumeClass], error)
	DeleteExternalVolumeClass(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.ExternalVolumeClass, error)
	CreateCsiDriverConfig(ctx context.Context, config *ateapi.CsiDriverConfig) (*ateapi.CsiDriverConfig, error)
	GetCsiDriverConfig(ctx context.Context, name string) (*ateapi.CsiDriverConfig, error)
	GetCsiDriverConfigByDriverName(ctx context.Context, driverName string) (*ateapi.CsiDriverConfig, error)
	ListCsiDriverConfigs(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.CsiDriverConfig], error)
	UpdateCsiDriverConfig(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.CsiDriverConfig) error) (*ateapi.CsiDriverConfig, error)
	DeleteCsiDriverConfig(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.CsiDriverConfig, error)
	CreateGlobalAccessPolicy(ctx context.Context, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error)
	GetGlobalAccessPolicy(ctx context.Context) (*ateapi.AccessPolicy, error)
	UpdateGlobalAccessPolicy(ctx context.Context, precondition store.Precondition, mutate func(*ateapi.AccessPolicy) error) (*ateapi.AccessPolicy, error)
	CreateAtespaceAccessPolicy(ctx context.Context, name string, policy *ateapi.AccessPolicy) (*ateapi.AccessPolicy, error)
	GetAtespaceAccessPolicy(ctx context.Context, name string) (*ateapi.AccessPolicy, error)
	UpdateAtespaceAccessPolicy(ctx context.Context, name string, precondition store.Precondition, mutate func(*ateapi.AccessPolicy) error) (*ateapi.AccessPolicy, error)
	DeleteAtespaceAccessPolicy(ctx context.Context, name string, precondition store.DeletePreconditions) (*ateapi.AccessPolicy, error)
	CreateActorTemplate(ctx context.Context, template *ateapi.ActorTemplate) (*ateapi.ActorTemplate, error)
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapi.ActorTemplate, error)
	ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapi.ActorTemplate], error)
	DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (*ateapi.ActorTemplate, error)
	ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapi.Worker], error)
	GetWorker(ctx context.Context, name string) (*ateapi.Worker, error)
	ListWorkerAssignments(ctx context.Context, workerName string, opts store.ListOptions) (store.ListResponse[*ateapi.ActorAssignment], error)
	CreateWorker(ctx context.Context, worker *ateapi.Worker) (*ateapi.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapi.Worker) error) (*ateapi.Worker, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

func (s *RPCService) resolveCsiDriverConfig(ctx context.Context, driverName string) (*ateapi.CsiDriverConfig, bool, error) {
	if s.impl != nil {
		cfg, err := s.impl.GetCsiDriverConfigByDriverName(ctx, driverName)
		if err == nil {
			return cfg, false, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, false, fmt.Errorf("failed to retrieve CsiDriverConfig for %q: %w", driverName, err)
		}
		if s.csiDriverConfigLister == nil {
			return nil, true, fmt.Errorf("failed to retrieve CsiDriverConfig for %q: not found", driverName)
		}
	}
	cfg, k8sErr := csi.NewK8sCSIDriverConfigGetter(s.csiDriverConfigLister).Get(ctx, driverName)
	if k8sErr != nil {
		return nil, true, fmt.Errorf("failed to retrieve CsiDriverConfig for %q: %w", driverName, k8sErr)
	}
	return cfg, false, nil
}

func (s *RPCService) evictSlotLocked(driverName string, cur csiPluginSlot) (*csi.Plugin, uint64) {
	evicted := cur.plugin
	cur.plugin, cur.key = nil, csiPluginCacheKey{}
	cur.gen++
	s.csiPlugins[driverName] = cur
	return evicted, cur.gen
}

func (s *RPCService) getPluginOnce(ctx context.Context, driverName string) (volume.VolumePluginControlPlane, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	if s.csiPlugins == nil {
		s.csiPlugins = make(map[string]csiPluginSlot)
	}
	slot := s.csiPlugins[driverName]
	slot.inflight++
	startGen := slot.gen
	s.csiPlugins[driverName] = slot
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		cur := s.csiPlugins[driverName]
		cur.inflight--
		if cur.plugin == nil && cur.inflight == 0 {
			delete(s.csiPlugins, driverName)
		} else {
			s.csiPlugins[driverName] = cur
		}
		s.mu.Unlock()
	}()

	cfg, evictOnErr, err := s.resolveCsiDriverConfig(ctx, driverName)
	if err != nil {
		if !evictOnErr {
			return nil, false, err
		}
		s.mu.Lock()
		cur := s.csiPlugins[driverName]
		if cur.gen != startGen {
			retry := cur.plugin != nil
			s.mu.Unlock()
			return nil, retry, err
		}
		evicted, _ := s.evictSlotLocked(driverName, cur)
		s.mu.Unlock()
		_ = evicted.Close()
		return nil, false, err
	}

	key := newCSIPluginCacheKey(cfg)
	s.mu.Lock()
	cur := s.csiPlugins[driverName]
	if cur.plugin != nil && cur.key == key {
		s.mu.Unlock()
		return cur.plugin, false, nil
	}
	if cur.gen != startGen {
		s.mu.Unlock()
		return nil, true, nil
	}
	var preEvicted *csi.Plugin
	if cur.plugin != nil {
		preEvicted, startGen = s.evictSlotLocked(driverName, cur)
	}
	s.mu.Unlock()
	_ = preEvicted.Close()

	csiPlugin, err := csi.NewCSIPluginFromConfig(ctx, cfg, driverName, true)
	if err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	cur = s.csiPlugins[driverName]
	if (cur.plugin != nil && cur.key == key) || cur.gen != startGen {
		matched, retry := cur.plugin, cur.plugin == nil || cur.key != key
		s.mu.Unlock()
		_ = csiPlugin.Close()
		return matched, retry, nil
	}
	cur.plugin, cur.key = csiPlugin, key
	cur.gen++
	s.csiPlugins[driverName] = cur
	s.mu.Unlock()
	return csiPlugin, false, nil
}

// GetPlugin retrieves a CSI volume plugin by driver name, reading CsiDriverConfig
// from Postgres first and falling back to the Kubernetes CSIDriverConfig CRD.
func (s *RPCService) GetPlugin(ctx context.Context, driverName string) (volume.VolumePluginControlPlane, error) {
	if plugin, ok := s.staticVolumePlugins[driverName]; ok {
		return plugin, nil
	}
	for {
		plugin, retry, err := s.getPluginOnce(ctx, driverName)
		if !retry {
			return plugin, err
		}
	}
}

// ServiceImpl implements store.Interface and provides the "middleware" layer
// between the RPC and storage layers.  It enforces invariants and validation
// rules, and may implement additional logic beyond the storage layer.
//
// Methods on this service should hold most of the logic.
type ServiceImpl struct {
	// This field is explicitly named to prevent accidentally satisfying
	// methods we need to trap.
	store store.Interface

	storageClassLister storagev1listers.StorageClassLister

	// clusterSnapshotLocation is ate-api-server's --snapshot-location; see
	// resolveSnapshotLocation.
	clusterSnapshotLocation string
}

var _ store.Interface = (*ServiceImpl)(nil)

// newServiceImpl creates an instance of the service's middleware
// implementation layer.
func newServiceImpl(
	persistence store.Interface,
	storageClassLister storagev1listers.StorageClassLister,
) *ServiceImpl {
	s := &ServiceImpl{
		store:              persistence,
		storageClassLister: storageClassLister,
	}
	return s
}

// Pass-through.
func (s *ServiceImpl) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	return s.store.AcquireLease(ctx, key)
}
