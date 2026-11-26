// Copyright 2026 The Agent Substrate Authors
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
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/cache"
)

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
