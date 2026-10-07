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

// Package admission enforces resource invariants, cross-resource preconditions,
// and spec/status update separation on top of store.Interface.
package admission

import (
	"context"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// Admission mediates all access to store.Interface for RPC handlers, workflows,
// and controllers, enforcing resource validation and cross-resource invariants.
type Admission struct {
	store               store.Interface
	sandboxConfigLister listersv1alpha1.SandboxConfigLister
	storageClassLister  storagev1listers.StorageClassLister
}

// New creates a new Admission layer backed by store.
func New(
	store store.Interface,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	storageClassLister storagev1listers.StorageClassLister,
) *Admission {
	return &Admission{
		store:               store,
		sandboxConfigLister: sandboxConfigLister,
		storageClassLister:  storageClassLister,
	}
}

// AcquireLease acquires a distributed lease for key.
//
// Returns store.ErrLeaseConflict if the lease is already held by another client.
func (a *Admission) AcquireLease(ctx context.Context, key string) (*store.Lease, error) {
	return a.store.AcquireLease(ctx, key)
}
