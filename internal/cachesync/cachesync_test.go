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

package cachesync

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

	atefake "go.ate.dev/substrate/pkg/client/clientset/versioned/fake"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestOptionalSyncForbiddenCache mirrors ateapi's startup: the optional
// WorkerPool cache is Forbidden while the required caches sync, the warn
// callback fires naming only WorkerPools, and the diagnostic deadline leaves
// the informer retrying so granting permission recovers it without a restart.
func TestOptionalSyncForbiddenCache(t *testing.T) {
	var logs logBuffer
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
	OptionalSync(ctx, 100*time.Millisecond, map[string]cache.InformerSynced{"WorkerPools": workerPools.HasSynced},
		func(ctx context.Context, err error) {
			slog.WarnContext(ctx, "WorkerPool cache has not synced; worker-count metrics may omit empty pools",
				"watch-namespace", "agent-workloads", "err", err)
		})
	if err := WaitForCacheSync(ctx, 100*time.Millisecond, map[string]cache.InformerSynced{
		"SandboxConfigs": sandboxConfigs.HasSynced, "CSIDriverConfigs": csiConfigs.HasSynced,
	}); err != nil {
		t.Fatalf("optional WorkerPool cache blocked startup: %v", err)
	}
	if workerPools.HasSynced() {
		t.Fatal("forbidden WorkerPool cache unexpectedly synced")
	}
	if err := wait.PollUntilContextCancel(ctx, 10*time.Millisecond, true, func(context.Context) (bool, error) {
		return strings.Contains(logs.String(), "WorkerPool cache has not synced"), nil
	}); err != nil {
		t.Fatalf("missing optional-cache diagnostic: %v", err)
	}
	for _, want := range []string{`"level":"WARN"`, "caches not synced (WorkerPools)", "check logs for more details"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("optional-cache diagnostic lacks %q: %s", want, logs.String())
		}
	}
	// The diagnostic deadline must not stop the informer: fixing the
	// underlying cause should restore it without a restart.
	denied.Store(false)
	if !cache.WaitForCacheSync(ctx.Done(), workerPools.HasSynced) {
		t.Fatal("WorkerPool cache did not recover after list permission was granted")
	}
}

func TestWaitForCacheSync(t *testing.T) {
	ready := func() bool { return true }
	pending := func() bool { return false }
	t.Run("ready", func(t *testing.T) {
		if err := WaitForCacheSync(t.Context(), time.Second, map[string]cache.InformerSynced{"SandboxConfigs": ready}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("timeout names only unsynced caches", func(t *testing.T) {
		err := WaitForCacheSync(t.Context(), 10*time.Millisecond, map[string]cache.InformerSynced{
			"SandboxConfigs": pending, "CSIDriverConfigs": pending, "atelet Pods": ready,
		})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "CSIDriverConfigs, SandboxConfigs") || strings.Contains(err.Error(), "atelet Pods") {
			t.Fatalf("cache timeout = %v, want deadline and only unsynced cache names", err)
		}
		if !strings.Contains(err.Error(), "check logs for more details") {
			t.Fatalf("cache timeout = %v, want a pointer to the component logs", err)
		}
	})
	t.Run("shutdown cancels wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := WaitForCacheSync(ctx, time.Minute, map[string]cache.InformerSynced{"StorageClasses": pending}); !errors.Is(err, context.Canceled) {
			t.Fatalf("cache wait = %v, want cancellation", err)
		}
	})
}

func TestOptionalSyncSkipsWarnOnShutdown(t *testing.T) {
	warned := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	OptionalSync(ctx, time.Minute, map[string]cache.InformerSynced{"WorkerPools": func() bool { return false }},
		func(context.Context, error) { warned <- struct{}{} })
	select {
	case <-warned:
		t.Fatal("warn called for a canceled context")
	case <-time.After(500 * time.Millisecond):
	}
}
