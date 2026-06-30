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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Cache maintains an in-memory snapshot of all workers.
type Cache struct {
	store          store.Interface
	relistInterval time.Duration

	mu      sync.RWMutex
	workers map[string]*ateapipb.Worker

	ready atomic.Bool

	metricWorkerCount      metric.Int64Gauge
	metricNotReadyDuration metric.Float64Counter
	metricResyncs          metric.Int64Counter
	metricRelists          metric.Int64Counter
}

// New creates a Cache backed by a given store. relistInterval controls how
// often the cache performs a full ListWorkers to recover from state drifts
// caused by missing WorkerWatch events.
func New(s store.Interface, relistInterval time.Duration) (*Cache, error) {
	c := &Cache{
		store:          s,
		relistInterval: relistInterval,
		workers:        make(map[string]*ateapipb.Worker),
	}
	m := otel.Meter("workercache")
	var err error
	c.metricWorkerCount, err = m.Int64Gauge(
		"cache.worker.count",
		metric.WithUnit("{worker}"),
		metric.WithDescription("Current number of workers in the cache."))
	if err != nil {
		return nil, fmt.Errorf("create cache.worker.count gauge failed: %w", err)
	}
	c.metricNotReadyDuration, err = m.Float64Counter(
		"cache.not_ready.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Total time the worker cache spent not ready, between a watch disconnect and a successful resync."))
	if err != nil {
		return nil, fmt.Errorf("create cache.not_ready.duration counter failed: %w", err)
	}
	c.metricResyncs, err = m.Int64Counter(
		"cache.resyncs",
		metric.WithUnit("{resync}"),
		metric.WithDescription("Total full resyncs triggered by watch disconnects."))
	if err != nil {
		return nil, fmt.Errorf("create cache.resyncs counter failed: %w", err)
	}
	c.metricRelists, err = m.Int64Counter(
		"cache.relists",
		metric.WithUnit("{relist}"),
		metric.WithDescription("Total relists (initial, resync, and periodic)."))
	if err != nil {
		return nil, fmt.Errorf("create cache.relists counter failed: %w", err)
	}
	return c, nil
}

// Start syncs the cache synchronously, then spawns a background goroutine
// that streams updates, relists periodically, and resyncs on connection loss.
// Returns as soon as the initial sync succeeds.
func (c *Cache) Start(ctx context.Context) error {
	watch, err := c.sync(ctx)
	if err != nil {
		return err
	}
	c.ready.Store(true)
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
	if err := c.relist(ctx); err != nil {
		watch.Close()
		return nil, err
	}
	return watch, nil
}

func (c *Cache) relist(ctx context.Context) error {
	workers, err := c.store.ListWorkers(ctx)
	if err != nil {
		c.metricRelists.Add(ctx, 1, metric.WithAttributes(attribute.String("error.type", "_OTHER")))
		return fmt.Errorf("ListWorkers: %w", err)
	}
	newMap := make(map[string]*ateapipb.Worker, len(workers))
	for _, w := range workers {
		newMap[workerKey(w)] = w
	}
	c.mu.Lock()
	c.workers = newMap
	count := int64(len(newMap))
	c.mu.Unlock()
	c.metricWorkerCount.Record(ctx, count)
	c.metricRelists.Add(ctx, 1)
	slog.InfoContext(ctx, "worker cache synced", slog.Int("count", int(count)))
	return nil
}

func (c *Cache) watchEvents(ctx context.Context, watch *store.WorkerWatch) {
	ticker := time.NewTicker(c.relistInterval)
	defer ticker.Stop()
	for {
		select {
		case event, ok := <-watch.Events:
			if !ok {
				c.ready.Store(false)
				notReadySince := time.Now()
				watch.Close()
				if ctx.Err() != nil {
					return
				}
				slog.WarnContext(ctx, "worker cache: watch channel closed, resyncing")
				watch = c.resync(ctx)
				if watch == nil {
					return // context cancelled
				}
				c.ready.Store(true)
				c.metricNotReadyDuration.Add(ctx, time.Since(notReadySince).Seconds())
			} else {
				c.applyEvent(ctx, event)
			}
		case <-ticker.C:
			if err := c.relist(ctx); err != nil {
				slog.WarnContext(ctx, "worker cache: periodic relist failed", slog.Any("err", err))
			}
		case <-ctx.Done():
			c.ready.Store(false)
			watch.Close()
			return
		}
	}
}

func (c *Cache) resync(ctx context.Context) *store.WorkerWatch {
	c.metricResyncs.Add(ctx, 1)
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

func (c *Cache) applyEvent(ctx context.Context, event store.WorkerEvent) {
	key := workerKey(event.Worker)
	c.mu.Lock()
	switch event.Type {
	case store.WorkerEventDeleted:
		delete(c.workers, key)
	case store.WorkerEventCreated, store.WorkerEventUpdated:
		existing, ok := c.workers[key]
		if !ok || event.Worker.GetVersion() >= existing.GetVersion() {
			c.workers[key] = event.Worker
		}
	}
	count := int64(len(c.workers))
	c.mu.Unlock()
	c.metricWorkerCount.Record(ctx, count)
}

func workerKey(w *ateapipb.Worker) string {
	return w.GetWorkerNamespace() + ":" + w.GetWorkerPod()
}
