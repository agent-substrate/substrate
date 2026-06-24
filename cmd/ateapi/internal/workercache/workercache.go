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

// Package workercache maintains an in-memory view of all workers, kept current
// via store.Interface.WatchWorkers. It exposes Workers() for fast O(1) reads
// during actor scheduling and is the natural home for future indices (by node,
// by label, etc.) as scheduling requirements grow.
package workercache

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Cache maintains an in-memory snapshot of all workers.
//
// TODO: add metrics — at minimum a gauge for worker count, a counter for
// resync events, and a counter for failed PUBLISH operations (in ateredis).
type Cache struct {
	store store.Interface

	mu      sync.RWMutex
	workers map[string]*ateapipb.Worker

	ready atomic.Bool
}

// New creates a Cache backed by a given store.
func New(store store.Interface) *Cache {
	return &Cache{
		store:   store,
		workers: make(map[string]*ateapipb.Worker),
	}
}

// Start syncs the cache synchronously, then spawns a background goroutine
// that streams updates and resyncs on connection loss.
// Returns as soon as the initial sync succeeds.
func (c *Cache) Start(ctx context.Context) error {
	watch, err := c.sync(ctx)
	if err != nil {
		return err
	}
	go c.watchEvents(ctx, watch)
	return nil
}

// Workers returns a snapshot of all currently known workers. The returned
// slice and its elements must not be modified by the caller. Returns an error
// if the cache is not ready (brief window during reconnect); callers are
// expected to retry.
func (c *Cache) Workers() ([]*ateapipb.Worker, error) {
	if !c.ready.Load() {
		return nil, fmt.Errorf("worker cache not ready")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*ateapipb.Worker, 0, len(c.workers))
	for _, w := range c.workers {
		out = append(out, w)
	}
	return out, nil
}

func (c *Cache) sync(ctx context.Context) (*store.WorkerWatch, error) {
	watch, err := c.store.WatchWorkers(ctx)
	if err != nil {
		return nil, fmt.Errorf("WatchWorkers: %w", err)
	}

	workers, err := c.store.ListWorkers(ctx)
	if err != nil {
		watch.Close()
		return nil, fmt.Errorf("ListWorkers: %w", err)
	}

	newMap := make(map[string]*ateapipb.Worker, len(workers))
	for _, w := range workers {
		newMap[workerKey(w)] = w
	}

	c.mu.Lock()
	c.workers = newMap
	c.mu.Unlock()
	c.ready.Store(true)

	slog.InfoContext(ctx, "worker cache synced", slog.Int("count", len(newMap)))
	return watch, nil
}

func (c *Cache) watchEvents(ctx context.Context, watch *store.WorkerWatch) {
	for {
		// TODO: Add a periodic "relist" so we can recover from state drifts
		// caused by missed watch events.
		select {
		case event, ok := <-watch.Events:
			if !ok {
				c.ready.Store(false)
				watch.Close()
				if ctx.Err() != nil {
					return
				}
				slog.WarnContext(ctx, "worker cache: watch channel closed, resyncing")
				watch = c.resync(ctx)
				if watch == nil {
					return // context cancelled
				}
			} else {
				c.applyEvent(event)
			}
		case <-ctx.Done():
			c.ready.Store(false)
			watch.Close()
			return
		}
	}
}

func (c *Cache) resync(ctx context.Context) *store.WorkerWatch {
	backoff := wait.Backoff{
		Duration: time.Second,
		Factor:   2.0,
		Cap:      30 * time.Second,
		Steps:    5,
	}
	var watch *store.WorkerWatch
	_ = backoff.DelayFunc().Until(ctx, true, false, func(ctx context.Context) (bool, error) {
		var err error
		watch, err = c.sync(ctx)
		if err != nil {
			slog.WarnContext(ctx, "worker cache resync failed", slog.Any("err", err))
			return false, nil
		}
		return true, nil
	})
	return watch
}

func (c *Cache) applyEvent(event store.WorkerEvent) {
	key := workerKey(event.Worker)
	c.mu.Lock()
	defer c.mu.Unlock()
	switch event.Type {
	case store.WorkerEventDeleted:
		delete(c.workers, key)
	case store.WorkerEventCreated, store.WorkerEventUpdated:
		existing, ok := c.workers[key]
		if !ok || event.Worker.GetVersion() >= existing.GetVersion() {
			c.workers[key] = event.Worker
		}
	}
}

func workerKey(w *ateapipb.Worker) string {
	return w.GetWorkerNamespace() + ":" + w.GetWorkerPod()
}
