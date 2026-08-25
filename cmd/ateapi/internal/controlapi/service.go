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

// RPCService implements ateapipb.ControlServer.  This is the user-facing RPC
// interface.
type RPCService struct {
	ateapipb.UnimplementedControlServer
	persistence           serviceStore
	workerCache           *workercache.Cache
	dialer                *AteletDialer
	actorTemplateLister   listersv1alpha1.ActorTemplateLister
	workerPoolLister      listersv1alpha1.WorkerPoolLister
	csiDriverConfigLister listersv1alpha1.CSIDriverConfigLister
	storageClassLister    storagev1listers.StorageClassLister
	actorWorkflow         *ActorWorkflow
	instruments           *Instruments
	mu                    sync.RWMutex
	volumePlugins         map[string]volume.VolumePluginControlPlane
}

var _ ateapipb.ControlServer = (*RPCService)(nil)

// VolumePluginRegistry defines the interface for dynamic CSI plugin resolution.
type VolumePluginRegistry interface {
	GetPlugin(ctx context.Context, name string) (volume.VolumePluginControlPlane, error)
}

// NewRPCService creates a RPC service. instruments may be nil; the record helpers no-op.
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
	s := &RPCService{
		persistence:           persistence,
		workerCache:           workerCache,
		actorTemplateLister:   actorTemplateLister,
		workerPoolLister:      workerPoolLister,
		csiDriverConfigLister: csiDriverConfigLister,
		storageClassLister:    storageClassLister,
		dialer:                dialer,
		instruments:           instruments,
		volumePlugins:         volumePlugins,
	}
	s.actorWorkflow = NewActorWorkflow(persistence, workerCache, dialer, actorTemplateLister, workerPoolLister, sandboxConfigLister, storageClassLister, instruments, egressGatewayAddress, s)
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
	DeleteWorker(ctx context.Context, name string, pre store.DeletePreconditions) (*ateapipb.Worker, error)
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
