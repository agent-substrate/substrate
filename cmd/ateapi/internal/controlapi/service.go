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

package controlapi

import (
	"context"
	"sync"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/internal/volume/csi"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// RPCService implements ateapipb.ControlServer and provides the implementation of
// the RPC service.
//
// Methods on this service should be as light as possible, delegating to the
// ServiceImpl for business logic and invariants.
type RPCService struct {
	ateapipb.UnimplementedControlServer
	impl                  serviceStore
	workerCache           *workercache.Cache
	dialer                *AteletDialer
	workerPoolLister      listersv1alpha1.WorkerPoolLister
	csiDriverConfigLister listersv1alpha1.CSIDriverConfigLister
	actorWorkflow         *ActorWorkflow
	workerWorkflow        *WorkerWorkflow
	instruments           *Instruments
	mu                    sync.RWMutex
	volumePlugins         map[string]volume.VolumePluginControlPlane
}

var _ ateapipb.ControlServer = (*RPCService)(nil)

// VolumePluginRegistry defines the interface for dynamic CSI plugin resolution.
type VolumePluginRegistry interface {
	GetPlugin(ctx context.Context, name string) (volume.VolumePluginControlPlane, error)
}

// NewRPCService creates an instance of the ControlServer service. This is what
// implements the outward-facing RPC interface.
//
// instruments may be nil; the record helpers no-op.
func NewRPCService(
	persistence store.Interface,
	workerCache *workercache.Cache,
	actorTemplateLister listersv1alpha1.ActorTemplateLister,
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	csiDriverConfigLister listersv1alpha1.CSIDriverConfigLister,
	storageClassLister storagev1listers.StorageClassLister,
	dialer *AteletDialer,
	instruments *Instruments,
	egressGatewayAddress string,
	volumePlugins map[string]volume.VolumePluginControlPlane,
) *RPCService {
	impl := newServiceImpl(persistence, actorTemplateLister, storageClassLister)
	s := &RPCService{
		impl:                  impl,
		workerCache:           workerCache,
		workerPoolLister:      workerPoolLister,
		csiDriverConfigLister: csiDriverConfigLister,
		dialer:                dialer,
		instruments:           instruments,
		volumePlugins:         volumePlugins,
	}
	s.actorWorkflow = NewActorWorkflow(impl, workerCache, dialer, actorTemplateLister, workerPoolLister, sandboxConfigLister, storageClassLister, instruments, egressGatewayAddress, s)
	s.workerWorkflow = NewWorkerWorkflow(impl)
	return s
}

// serviceStore enumerates the exact storage methods needed by
// the control API and nothing more.
type serviceStore interface {
	CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error)
	GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error)
	UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.Actor) error) (*ateapipb.Actor, error)
	ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error)
	GetActorSnapshot(ctx context.Context, snapshotRef resources.ActorSnapshotRef) (*ateapipb.ActorSnapshot, error)
	ListActorSnapshots(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorSnapshot], error)
	CreateActorSnapshotTag(ctx context.Context, snapshotRef resources.ActorSnapshotRef, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error)
	GetActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error)
	UpdateActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error)
	DeleteActorSnapshotTag(ctx context.Context, tagRef resources.ActorSnapshotTagRef) (*ateapipb.ActorSnapshotTag, error)
	CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error)
	GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error)
	ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Atespace], error)
	DeleteAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error)
	CreateActorTemplate(ctx context.Context, template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error)
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)
	ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error)
	DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)
	ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error)
	GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error)
	CreateWorker(ctx context.Context, worker *ateapipb.Worker) (*ateapipb.Worker, error)
	UpdateWorker(ctx context.Context, name string, precondition store.Precondition, mutate func(toUpdate *ateapipb.Worker) error) (*ateapipb.Worker, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// GetPlugin retrieves a CSI volume plugin by driver name, dynamically discovering it if not present.
func (s *RPCService) GetPlugin(ctx context.Context, driverName string) (volume.VolumePluginControlPlane, error) {
	s.mu.RLock()
	plugin, ok := s.volumePlugins[driverName]
	s.mu.RUnlock()
	if ok {
		return plugin, nil
	}

	csiPlugin, err := csi.NewCSIPlugin(ctx, s.csiDriverConfigLister, driverName, true /*isController*/)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.volumePlugins[driverName] = csiPlugin
	s.mu.Unlock()
	return csiPlugin, nil
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

	actorTemplateLister listersv1alpha1.ActorTemplateLister
	storageClassLister  storagev1listers.StorageClassLister
}

var _ store.Interface = (*ServiceImpl)(nil)

// newServiceImpl creates an instance of the service's middleware
// implementation layer.
func newServiceImpl(
	persistence store.Interface,
	actorTemplateLister listersv1alpha1.ActorTemplateLister,
	storageClassLister storagev1listers.StorageClassLister,
) *ServiceImpl {
	s := &ServiceImpl{
		store:               persistence,
		actorTemplateLister: actorTemplateLister,
		storageClassLister:  storageClassLister,
	}
	return s
}

// Pass-through.
func (s *ServiceImpl) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	return s.store.AcquireLease(ctx, key)
}

// Pass-through.
func (s *ServiceImpl) DebugClearAll(ctx context.Context) error {
	return s.store.DebugClearAll(ctx)
}
