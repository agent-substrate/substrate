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

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/internal/cachesync"
	atefake "go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestRequiredKubernetesCachesWithoutSandboxConfigs(t *testing.T) {
	kube := kubefake.NewClientset()
	ate := atefake.NewSimpleClientset()
	ate.PrependReactor("list", "sandboxconfigs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "ate.dev", Resource: "sandboxconfigs"}, "", errors.New("no SandboxConfig permission"))
	})
	ateFactory := externalversions.NewSharedInformerFactoryWithOptions(ate, 0, externalversions.WithNamespace("workers"))
	ateFactory.Api().V1alpha1().WorkerPools().Lister()
	ateFactory.Api().V1alpha1().CSIDriverConfigs().Lister()
	ateletFactory, ateletPods := controlapi.AteletInformer(kube, "ate-system")
	scFactory := informers.NewSharedInformerFactory(kube, 0)
	scFactory.Storage().V1().StorageClasses().Lister()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(func() {
		cancel()
		ateFactory.Shutdown()
		ateletFactory.Shutdown()
		scFactory.Shutdown()
	})
	ateFactory.Start(ctx.Done())
	ateletFactory.Start(ctx.Done())
	scFactory.Start(ctx.Done())
	if err := cachesync.WaitForCacheSync(ctx, time.Second, requiredKubernetesCaches(ateletPods, ateFactory, scFactory)); err != nil {
		t.Fatalf("required caches should sync without SandboxConfigs: %v", err)
	}
	for _, action := range ate.Actions() {
		if action.GetResource().Resource == "sandboxconfigs" {
			t.Fatalf("unexpected SandboxConfig request: %v", action)
		}
	}
}
