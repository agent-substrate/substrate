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
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/internal/volume/csi"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/client-go/kubernetes"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// Service implements ateapipb.Control
type Service struct {
	ateapipb.UnimplementedControlServer
	persistence           store.Interface
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

var _ ateapipb.ControlServer = (*Service)(nil)

// VolumePluginRegistry defines the interface for dynamic CSI plugin resolution.
type VolumePluginRegistry interface {
	GetPlugin(ctx context.Context, name string) (volume.VolumePluginControlPlane, error)
}

// NewService creates a service. instruments may be nil; the record helpers no-op.
func NewService(
	persistence store.Interface,
	workerCache *workercache.Cache,
	actorTemplateLister listersv1alpha1.ActorTemplateLister,
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	csiDriverConfigLister listersv1alpha1.CSIDriverConfigLister,
	storageClassLister storagev1listers.StorageClassLister,
	dialer *AteletDialer,
	kubeClient kubernetes.Interface,
	instruments *Instruments,
	egressGatewayAddress string,
	volumePlugins map[string]volume.VolumePluginControlPlane,
) *Service {
	s := &Service{
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
	s.actorWorkflow = NewActorWorkflow(persistence, workerCache, dialer, actorTemplateLister, workerPoolLister, sandboxConfigLister, storageClassLister, kubeClient, instruments, egressGatewayAddress, s)
	return s
}

// GetPlugin retrieves a CSI volume plugin by driver name, dynamically discovering it if not present.
func (s *Service) GetPlugin(ctx context.Context, driverName string) (volume.VolumePluginControlPlane, error) {
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
