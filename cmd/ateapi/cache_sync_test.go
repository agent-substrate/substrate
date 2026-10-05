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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	atefake "github.com/agent-substrate/substrate/pkg/client/clientset/versioned/fake"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type cacheSyncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *cacheSyncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *cacheSyncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestWaitForAPICachesWorkerPoolForbidden(t *testing.T) {
	var logs cacheSyncLogBuffer
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	//nolint:staticcheck // NewSimpleClientset is the generated fake for these CRDs.
	client := atefake.NewSimpleClientset()
	var denied atomic.Bool
	denied.Store(true)
	client.PrependReactor("list", "workerpools", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if denied.Load() {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "ate.dev", Resource: "workerpools"}, "", fmt.Errorf("missing RoleBinding in %s", action.GetNamespace()))
		}
		return false, nil, nil
	})
	factory := externalversions.NewSharedInformerFactoryWithOptions(client, 0, externalversions.WithNamespace("agent-workloads"))
	workerPools := factory.Api().V1alpha1().WorkerPools().Informer()
	sandboxConfigs := factory.Api().V1alpha1().SandboxConfigs().Informer()
	csiConfigs := factory.Api().V1alpha1().CSIDriverConfigs().Informer()
	factory.Start(ctx.Done())
	t.Cleanup(func() {
		cancel()
		factory.Shutdown()
	})
	if !cache.WaitForCacheSync(ctx.Done(), sandboxConfigs.HasSynced, csiConfigs.HasSynced) {
		t.Fatal("required caches did not sync")
	}
	if err := waitForAPICaches(ctx, 100*time.Millisecond, "agent-workloads", workerPools.HasSynced, map[string]cache.InformerSynced{
		"SandboxConfigs": sandboxConfigs.HasSynced, "CSIDriverConfigs": csiConfigs.HasSynced,
	}); err != nil {
		t.Fatalf("optional WorkerPool cache blocked API startup: %v", err)
	}
	if workerPools.HasSynced() {
		t.Fatal("forbidden WorkerPool cache unexpectedly synced")
	}
	if err := wait.PollUntilContextCancel(ctx, 10*time.Millisecond, true, func(context.Context) (bool, error) {
		return strings.Contains(logs.String(), "WorkerPool cache has not synced"), nil
	}); err != nil {
		t.Fatalf("missing cache diagnostic: %v", err)
	}
	for _, want := range []string{`"level":"WARN"`, `"watch-namespace":"agent-workloads"`, "list/watch RBAC"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("cache diagnostic lacks %q: %s", want, logs.String())
		}
	}
	// The diagnostic deadline must not stop the informer: fixing RBAC should
	// restore metric seeding without restarting the API.
	denied.Store(false)
	if !cache.WaitForCacheSync(ctx.Done(), workerPools.HasSynced) {
		t.Fatal("WorkerPool cache did not recover after list permission was granted")
	}
}

func TestWaitForCacheSync(t *testing.T) {
	ready := func() bool { return true }
	pending := func() bool { return false }
	t.Run("ready", func(t *testing.T) {
		if err := waitForCacheSync(t.Context(), time.Second, map[string]cache.InformerSynced{"SandboxConfigs": ready}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("timeout names only unsynced caches", func(t *testing.T) {
		err := waitForCacheSync(t.Context(), 10*time.Millisecond, map[string]cache.InformerSynced{
			"SandboxConfigs": pending, "CSIDriverConfigs": pending, "atelet Pods": ready,
		})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "CSIDriverConfigs, SandboxConfigs") || strings.Contains(err.Error(), "atelet Pods") {
			t.Fatalf("cache timeout = %v, want deadline and only unsynced cache names", err)
		}
	})
	t.Run("shutdown cancels wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := waitForCacheSync(ctx, time.Minute, map[string]cache.InformerSynced{"StorageClasses": pending}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cache wait = %v, want cancellation", err)
		}
	})
}
