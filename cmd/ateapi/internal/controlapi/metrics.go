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
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	workerpoolWorkersMetric   = "ate.workerpool.workers"
	lifecycleOpDurationMetric = "ate.actor.lifecycle.operation.duration"
	schedulerAssignmentMetric = "ate.scheduler.assignment.duration"
	actorCrashesMetric        = "ate.actor.crashes"
)

var actorCrashesCounter metric.Int64Counter

// RegisterActorCrashes initializes the ate.actor.crashes counter instrument.
func RegisterActorCrashes(meter metric.Meter) error {
	counter, err := meter.Int64Counter(
		actorCrashesMetric,
		metric.WithUnit("{crash}"),
		metric.WithDescription("Number of times actors transitioned to STATUS_CRASHED with failure reasons."),
	)
	if err != nil {
		return fmt.Errorf("create %s counter: %w", actorCrashesMetric, err)
	}
	actorCrashesCounter = counter
	return nil
}

// recordActorCrash records a crash event on ate.actor.crashes with low-cardinality attributes.
func recordActorCrash(ctx context.Context, attrs []attribute.KeyValue) {
	if actorCrashesCounter == nil || len(attrs) == 0 {
		return
	}
	actorCrashesCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
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

// Instruments holds ateapi's actor-lifecycle and scheduler duration histograms.
// A nil *Instruments is a valid no-op, so call sites need no guard. Worker-count
// is registered separately (RegisterWorkerCount): a callback-driven observable,
// not a synchronous instrument.
type Instruments struct {
	lifecycleOpDuration         metric.Float64Histogram
	schedulerAssignmentDuration metric.Float64Histogram
}

// NewInstruments builds the two histograms against meter. Assignment buckets are
// finer than lifecycle's (a cache pick plus a few store writes, not a
// multi-second restore) but reach 5s so store latency spikes stay measurable.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	lifecycleOpDuration, err := meter.Float64Histogram(
		lifecycleOpDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of an actor lifecycle operation (create, resume, suspend, pause, delete) handled by ateapi."),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2.5, 5, 10, 30),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", lifecycleOpDurationMetric, err)
	}

	schedulerAssignmentDuration, err := meter.Float64Histogram(
		schedulerAssignmentMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of the scheduler's worker-assignment step during actor resume."),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", schedulerAssignmentMetric, err)
	}

	return &Instruments{
		lifecycleOpDuration:         lifecycleOpDuration,
		schedulerAssignmentDuration: schedulerAssignmentDuration,
	}, nil
}

// recordLifecycleOp records op's duration. A non-nil err is classified onto
// error.type via its gRPC status code; error.type's absence marks success, so
// there is no parallel failure counter. extraAttrs carries the per-operation
// dimensions (template, pool, class, snapshot kind).
func (i *Instruments) recordLifecycleOp(ctx context.Context, op string, start time.Time, err error, extraAttrs ...attribute.KeyValue) {
	if i == nil || i.lifecycleOpDuration == nil {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(extraAttrs)+2)
	attrs = append(attrs, ateattr.ActorOperationNameKey.String(op))
	attrs = append(attrs, extraAttrs...)
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(status.Code(err).String()))
	}
	i.lifecycleOpDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
}

// lifecycleOpAttrs builds the resume/suspend/pause dimensions from workflow
// state. Nil-safe, and omits the pool and snapshot-kind labels until they are
// known so a failure before the assign/restore steps never emits an empty-string
// series. snapshotKind is empty for suspend/pause, which do not restore.
func lifecycleOpAttrs(actor *ateapipb.Actor, template *atev1alpha1.ActorTemplate, snapshotKind string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		ateattr.TemplateNameKey.String(actor.GetActorTemplateName()),
		ateattr.TemplateNamespaceKey.String(actor.GetActorTemplateNamespace()),
	}
	if pool := actor.GetWorkerAssignment().GetWorkerPool(); pool != "" {
		attrs = append(attrs, ateattr.WorkerPoolNameKey.String(pool))
	}
	if template != nil {
		attrs = append(attrs, ateattr.SandboxClassKey.String(string(template.Spec.SandboxClass)))
	}
	if snapshotKind != "" {
		attrs = append(attrs, ateattr.SnapshotKindKey.String(snapshotKind))
	}
	return attrs
}

// recordSchedulerAssignment records one assignment attempt. pool is set only
// when a worker was assigned and error.type only for the Error outcome, so
// no_free_worker (a capacity signal, not a failure) carries neither. class is
// set on every outcome it is known for, so no_free_worker names the capacity
// that ran out and stays comparable with assigned.
func (i *Instruments) recordSchedulerAssignment(ctx context.Context, start time.Time, outcome, pool, class string, err error) {
	if i == nil || i.schedulerAssignmentDuration == nil {
		return
	}
	attrs := make([]attribute.KeyValue, 0, 4)
	attrs = append(attrs, ateattr.SchedulerOutcomeKey.String(outcome))
	if pool != "" {
		attrs = append(attrs, ateattr.WorkerPoolNameKey.String(pool))
	}
	if class != "" {
		attrs = append(attrs, ateattr.SandboxClassKey.String(class))
	}
	if outcome == ateattr.SchedulerOutcomeError && err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(status.Code(err).String()))
	}
	i.schedulerAssignmentDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
}
