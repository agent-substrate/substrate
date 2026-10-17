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

// Package cachesync waits for Kubernetes informer caches at component startup
// and names any caches that fail to sync.
package cachesync

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/client-go/tools/cache"
)

// WaitForCacheSync blocks until every informer reports synced or timeout
// passes, naming any informers still pending in the returned error. Only the
// wait is bounded: the informers themselves keep retrying.
func WaitForCacheSync(ctx context.Context, timeout time.Duration, informers map[string]cache.InformerSynced) error {
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
	return fmt.Errorf("caches not synced (%s); check logs for more details: %w", strings.Join(pending, ", "), syncCtx.Err())
}

// OptionalSync waits in the background for caches a component can serve
// without and never blocks or fails: after timeout it calls warn with an
// error naming the informers that have not synced, so the component can
// report what degraded. A canceled parent context (shutdown) does not call
// warn. The informers keep retrying, so a cache that syncs later recovers on
// its own.
func OptionalSync(ctx context.Context, timeout time.Duration, informers map[string]cache.InformerSynced, warn func(context.Context, error)) {
	go func() {
		if err := WaitForCacheSync(ctx, timeout, informers); err != nil && ctx.Err() == nil {
			warn(ctx, err)
		}
	}()
}
