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
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/labels"
)

// resolveSandboxAssets picks the sandbox assets to call atelet with.
func (w *ActorWorkflow) resolveSandboxAssets(ctx context.Context, actor *ateapipb.Actor, poolNamespace, poolName string) (*ateletpb.SandboxAssets, error) {
	ref := actor.GetActorTemplate()
	if ref == nil {
		return resolveSandboxAssetsByWorkerPool(w.workerPoolLister, w.sandboxConfigLister, poolNamespace, poolName)
	}
	templateRef := resources.ActorTemplateRefFromObjectRef(ref)
	tmpl, err := w.store.GetActorTemplate(ctx, templateRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s not found", templateRef)
		}
		return nil, fmt.Errorf("while getting ActorTemplate %s: %w", templateRef, err)
	}
	frozen := tmpl.GetStatus().GetSandboxAssets()
	if frozen == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %s has no sandbox assets frozen in its status", templateRef)
	}
	return ateletSandboxAssets(frozen), nil
}

// ateletSandboxAssets projects a template's frozen ateapipb.SandboxAssets
// onto the proto atelet fetches.
func ateletSandboxAssets(in *ateapipb.SandboxAssets) *ateletpb.SandboxAssets {
	out := &ateletpb.SandboxAssets{
		SandboxClass: sandboxClassString(in.GetSandboxClass()),
		PauseImage:   in.GetPauseImage(),
		Assets:       make(map[string]*ateletpb.ArchAssets, len(in.GetAssets())),
	}
	for arch, archAssets := range in.GetAssets() {
		files := &ateletpb.ArchAssets{Files: make(map[string]*ateletpb.AssetFile, len(archAssets.GetFiles()))}
		for name, f := range archAssets.GetFiles() {
			files.Files[name] = &ateletpb.AssetFile{Url: f.GetUrl(), Sha256: f.GetSha256()}
		}
		out.Assets[arch] = files
	}
	return out
}

// sandboxClassString maps the API enum onto the class string atelet keys its
// runtimes on. Unspecified defaults to gvisor, matching resolveSandboxAssets.
func sandboxClassString(class ateapipb.SandboxClass) string {
	if class == ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM {
		return string(atev1alpha1.SandboxClassMicroVM)
	}
	return string(atev1alpha1.SandboxClassGvisor)
}

// resolveSandboxAssetsByWorkerPool determines the sandbox binaries and pause image an actor
// should boot with and projects them onto the ateletpb.SandboxAssets atelet
// fetches. It takes the SandboxClass (default gvisor) of a given worker pool,
// then picks the SandboxConfig named by the pool — or, if none is named, the
// cluster default SandboxConfig for that class.
func resolveSandboxAssetsByWorkerPool(
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
		PauseImage:   sc.Spec.PauseImage,
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
