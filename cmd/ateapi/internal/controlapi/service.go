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
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/client-go/kubernetes"
)

// Service implements ateapipb.Control
type Service struct {
	ateapipb.UnimplementedControlServer
	persistence         store.Interface
	dialer              *AteletDialer
	actorTemplateLister listersv1alpha1.ActorTemplateLister
	workerPoolLister    listersv1alpha1.WorkerPoolLister
	actorWorkflow       *ActorWorkflow
}

var _ ateapipb.ControlServer = (*Service)(nil)

// NewService creates a service.
func NewService(
	persistence store.Interface,
	workerCache *workercache.Cache,
	actorTemplateLister listersv1alpha1.ActorTemplateLister,
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	dialer *AteletDialer,
	kubeClient kubernetes.Interface,
) *Service {
	s := &Service{
		persistence:         persistence,
		actorTemplateLister: actorTemplateLister,
		workerPoolLister:    workerPoolLister,
		dialer:              dialer,
		actorWorkflow:       NewActorWorkflow(persistence, workerCache, dialer, actorTemplateLister, workerPoolLister, sandboxConfigLister, kubeClient),
	}

	return s
}
