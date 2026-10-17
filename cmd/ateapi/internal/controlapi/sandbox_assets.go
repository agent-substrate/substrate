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
	"fmt"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "go.ate.dev/substrate/pkg/client/listers/api/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

// resolvePoolSandboxConfig resolves the SandboxConfig the controller confirmed
// for the WorkerPool of the actor's assigned worker.
func resolvePoolSandboxConfig(
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	assignment *ateapipb.WorkerAssignment,
) (*atev1alpha1.SandboxConfig, error) {
	namespace, poolName := assignment.GetWorkerNamespace(), assignment.GetWorkerPool()
	if namespace == "" || poolName == "" {
		return nil, fmt.Errorf("worker assignment names no worker pool")
	}
	pool, err := workerPoolLister.WorkerPools(namespace).Get(poolName)
	if k8serrors.IsNotFound(err) {
		return nil, apierror.FailedPrecondition("WorkerPool %s/%s not found", namespace, poolName)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting WorkerPool %s/%s: %w", namespace, poolName, err)
	}
	configName := pool.ResolvedSandboxConfig()
	if configName == "" {
		return nil, apierror.FailedPrecondition(
			"WorkerPool %s/%s has not resolved a SandboxConfig for sandbox class %q", namespace, poolName, pool.Spec.DefaultSandboxClass())
	}
	sc, err := sandboxConfigLister.Get(configName)
	if k8serrors.IsNotFound(err) {
		return nil, apierror.FailedPrecondition("SandboxConfig %q of WorkerPool %s/%s not found", configName, namespace, poolName)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting SandboxConfig %q: %w", configName, err)
	}
	return sc, nil
}

// resolveSandboxAssets determines the sandbox binaries and pause image an actor
// boots with on its assigned worker and projects them onto the
// ateletpb.SandboxAssets atelet fetches: the default version of the
// SandboxConfig the worker's pool resolved for its sandbox class, which is the
// runtime the worker reports as its default.
//
// TODO: Remove with sandbox_assets. atelet already resolves the pool's
// SandboxConfig and its versions for every worker on the node (its sandbox
// runtimes syncer), and Run and Restore carry sandbox_runtime; once atelet
// resolves the version from that field instead of taking the assets from the
// request, this lookup, the WorkerPool and SandboxConfig listers on
// ActorWorkflow, and ateapi's sandboxconfigs RBAC go.
func resolveSandboxAssets(
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	assignment *ateapipb.WorkerAssignment,
) (*ateletpb.SandboxAssets, error) {
	sc, err := resolvePoolSandboxConfig(workerPoolLister, sandboxConfigLister, assignment)
	if err != nil {
		return nil, err
	}
	ver, ok := sc.Spec.DefaultVersionConfig()
	if !ok {
		return nil, apierror.FailedPrecondition(
			"SandboxConfig %q has no enabled version %q named by its defaultVersion", sc.Name, sc.Spec.DefaultVersion)
	}
	return sandboxAssetsProto(sc, ver), nil
}

// sandboxAssetsProto converts a version of a resolved SandboxConfig into the
// proto atelet consumes.
func sandboxAssetsProto(sc *atev1alpha1.SandboxConfig, ver *atev1alpha1.SandboxVersionConfig) *ateletpb.SandboxAssets {
	out := &ateletpb.SandboxAssets{
		SandboxClass: string(sc.Spec.SandboxClass),
		PauseImage:   ver.PauseImage,
		Assets:       make(map[string]*ateletpb.ArchAssets, len(ver.Assets)),
	}
	for arch, files := range ver.Assets {
		archAssets := &ateletpb.ArchAssets{Files: make(map[string]*ateletpb.AssetFile, len(files))}
		for name, f := range files {
			archAssets.Files[name] = &ateletpb.AssetFile{Url: f.URL, Sha256: f.SHA256}
		}
		out.Assets[arch] = archAssets
	}
	return out
}
