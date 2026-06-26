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

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"k8s.io/apimachinery/pkg/labels"
)

// resolveSandboxAssets determines the sandbox binaries an actor should boot with
// and projects them onto the ateletpb.SandboxAssets atelet fetches. It takes the
// SandboxClass (default gvisor) of a given worker pool, then picks the SandboxConfig
// named by the pool — or, if none is named, the cluster default SandboxConfig for that class.
func resolveSandboxAssets(
	workerPoolLister listersv1alpha1.WorkerPoolLister,
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	poolNamespace, poolName string,
) (*ateletpb.SandboxAssets, error) {
	wp, err := workerPoolLister.WorkerPools(poolNamespace).Get(poolName)
	if err != nil {
		return nil, fmt.Errorf("while getting WorkerPool %s/%s: %w", poolNamespace, poolName, err)
	}

	class := wp.Spec.SandboxClass
	if class == "" {
		class = atev1alpha1.SandboxClassGvisor
	}

	var sc *atev1alpha1.SandboxConfig
	if name := wp.Spec.SandboxConfigName; name != "" {
		sc, err = sandboxConfigLister.Get(name)
		if err != nil {
			return nil, fmt.Errorf("while getting SandboxConfig %q: %w", name, err)
		}
		if sc.Spec.SandboxClass != class {
			return nil, fmt.Errorf("SandboxConfig %q has class %q but WorkerPool %s/%s is class %q",
				name, sc.Spec.SandboxClass, poolNamespace, poolName, class)
		}
	} else {
		sc, err = defaultSandboxConfig(sandboxConfigLister, class)
		if err != nil {
			return nil, err
		}
	}

	return sandboxAssetsProto(class, sc), nil
}

// defaultSandboxConfig returns the single SandboxConfig marked Default for the
// given class, erroring if there are zero or more than one.
func defaultSandboxConfig(lister listersv1alpha1.SandboxConfigLister, class atev1alpha1.SandboxClass) (*atev1alpha1.SandboxConfig, error) {
	all, err := lister.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("while listing SandboxConfigs: %w", err)
	}
	var match *atev1alpha1.SandboxConfig
	for _, sc := range all {
		if sc.Spec.SandboxClass == class && sc.Spec.Default {
			if match != nil {
				return nil, fmt.Errorf("multiple default SandboxConfigs for class %q (%q and %q)", class, match.Name, sc.Name)
			}
			match = sc
		}
	}
	if match == nil {
		return nil, fmt.Errorf("no default SandboxConfig for class %q; set one with spec.default=true or name one via WorkerPool.spec.sandboxConfigName", class)
	}
	return match, nil
}

// sandboxAssetsProto converts a resolved SandboxConfig into the proto atelet
// consumes.
func sandboxAssetsProto(class atev1alpha1.SandboxClass, sc *atev1alpha1.SandboxConfig) *ateletpb.SandboxAssets {
	out := &ateletpb.SandboxAssets{
		SandboxClass: string(class),
		Assets:       make(map[string]*ateletpb.ArchAssets, len(sc.Spec.Assets)),
	}
	for arch, files := range sc.Spec.Assets {
		archAssets := &ateletpb.ArchAssets{Files: make(map[string]*ateletpb.AssetFile, len(files))}
		for name, f := range files {
			archAssets.Files[name] = &ateletpb.AssetFile{Url: f.URL, Sha256: f.SHA256}
		}
		out.Assets[arch] = archAssets
	}
	return out
}
