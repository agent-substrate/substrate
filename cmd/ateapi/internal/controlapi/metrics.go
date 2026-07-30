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
	"fmt"

	"github.com/agent-substrate/substrate/internal/ateattr"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	workerpoolWorkersMetric = "ate.workerpool.workers"
	actorCrashesMetric      = "ate.actor.crashes"
)

var actorCrashesCounter metric.Int64Counter

// RegisterActorCrashes initializes the ate.actor.crashes counter instrument.
func RegisterActorCrashes(meter metric.Meter) error {
	counter, err := meter.Int64Counter(
		actorCrashesMetric,
		metric.WithUnit("{actor}"),
		metric.WithDescription("Total number of actors that transitioned to STATUS_CRASHED."),
	)
	if err != nil {
		return fmt.Errorf("create %s counter: %w", actorCrashesMetric, err)
	}
	actorCrashesCounter = counter
	return nil
}

// recordActorCrash records a crash event on ate.actor.crashes with low-cardinality attributes.
func recordActorCrash(ctx context.Context, actor *ateapipb.Actor, sandboxClass, opName, reason string) {
	if actorCrashesCounter == nil || actor == nil {
		return
	}
	actorCrashesCounter.Add(ctx, 1, metric.WithAttributes(ateattr.ActorMetricAttributes(actor, sandboxClass, opName, reason)...))
}

// RegisterWorkerCount wires the ate.workerpool.workers observable against workers
// (workercache.Cache.Workers in prod) and listPools (a WorkerPool lister's List,
// used to seed zero-valued series). Worker counts are spatially summable (over
// states = pool size, over pools = fleet), which is the UpDownCounter contract; a
// gauge would be wrong for a value meant to be summed.
func RegisterWorkerCount(meter metric.Meter, workers func() ([]*ateapipb.Worker, error), listPools func(labels.Selector) ([]*atev1alpha1.WorkerPool, error)) error {
	counter, err := meter.Int64ObservableUpDownCounter(
		workerpoolWorkersMetric,
		metric.WithUnit("{worker}"),
		metric.WithDescription("Number of workers by pool namespace, pool, worker state, and sandbox class."),
	)
	if err != nil {
		return fmt.Errorf("create %s updowncounter: %w", workerpoolWorkersMetric, err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		ws, err := workers()
		if err != nil {
			// Worker cache unavailable (warmup/reconnect): skip the whole observation.
			return nil
		}
		type key struct{ namespace, pool, state, class string }
		tally := make(map[key]int64)
		// Seed both states at 0 for every known pool so a saturated or empty pool
		// reports 0, not an absent series that breaks idle==0 alerts. A failed
		// list just means no seeding this cycle, not a broken observation.
		if pools, err := listPools(labels.Everything()); err == nil {
			for _, p := range pools {
				class := string(p.Spec.SandboxClass)
				if class == "" {
					class = string(atev1alpha1.SandboxClassGvisor)
				}
				tally[key{p.Namespace, p.Name, ateattr.WorkerStateIdle, class}] = 0
				tally[key{p.Namespace, p.Name, ateattr.WorkerStateAssigned, class}] = 0
			}
		}
		for _, w := range ws {
			state := ateattr.WorkerStateIdle
			if w.GetAssignment() != nil {
				state = ateattr.WorkerStateAssigned
			}
			tally[key{w.GetWorkerNamespace(), w.GetWorkerPool(), state, w.GetSandboxClass()}]++
		}
		for k, n := range tally {
			o.ObserveInt64(counter, n, metric.WithAttributes(
				ateattr.WorkerPoolNamespaceKey.String(k.namespace),
				ateattr.WorkerPoolNameKey.String(k.pool),
				ateattr.WorkerStateKey.String(k.state),
				ateattr.SandboxClassKey.String(k.class),
			))
		}
		return nil
	}, counter)
	if err != nil {
		return fmt.Errorf("register %s callback: %w", workerpoolWorkersMetric, err)
	}
	return nil
}
