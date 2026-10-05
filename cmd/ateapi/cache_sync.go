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
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"k8s.io/client-go/tools/cache"
)

const cacheSyncTimeout = time.Minute

func waitForAPICaches(ctx context.Context, timeout time.Duration, namespace string, workerPools cache.InformerSynced, required map[string]cache.InformerSynced) error {
	// WorkerPools only seed zero-valued metric series. Their informer keeps
	// retrying after this diagnostic timeout without gating API availability.
	go func() {
		if err := waitForCacheSync(ctx, timeout, map[string]cache.InformerSynced{"WorkerPools": workerPools}); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "WorkerPool cache has not synced; worker-count metrics may omit empty pools, check list/watch RBAC", "watch-namespace", namespace, "all-namespaces", namespace == "", "err", err)
		}
	}()
	return waitForCacheSync(ctx, timeout, required)
}

func waitForCacheSync(ctx context.Context, timeout time.Duration, informers map[string]cache.InformerSynced) error {
	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	synced := make([]cache.InformerSynced, 0, len(informers))
	for _, hasSynced := range informers {
		synced = append(synced, hasSynced)
	}
	if cache.WaitForCacheSync(syncCtx.Done(), synced...) {
		return nil
	}
	var pending []string
	for name, hasSynced := range informers {
		if !hasSynced() {
			pending = append(pending, name)
		}
	}
	slices.Sort(pending)
	return fmt.Errorf("caches not synced (%s); check list/watch RBAC: %w", strings.Join(pending, ", "), syncCtx.Err())
}
