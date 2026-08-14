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
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// workerPoolLabel is the label the pool controller stamps on every worker pod
// it creates; the value is the WorkerPool's name and the pod's namespace is
// the pool's. Must agree with cmd/atecontroller/internal/controllers/
// workerpool_apply.go.
const workerPoolLabel = "ate.dev/worker-pool"

// minActorStatsPollInterval is the floor a configured poll interval is clamped
// to. It is the worst-case duration of one ateom's sweep on the micro-VM
// runtime -- maxActorContainers containers at statsCallTimeout each, constants
// that live with that runtime -- so a shorter interval could start a new poll
// into a guest agent still serving the previous one.
const minActorStatsPollInterval = 50 * time.Second

// statsRPCTimeout bounds one ateom's GetActiveWorkloadStats call. It has to
// cover the ateom's own worst-case sweep (see minActorStatsPollInterval);
// anything still unanswered past that is a stuck socket, not a slow guest.
const statsRPCTimeout = 55 * time.Second

// statsSweepConcurrency bounds how many ateoms one sweep probes at once. The
// interval floor protects a single guest from overlapping polls; probing
// DISTINCT ateoms concurrently puts one probe on each guest, so the only
// stacking the cap prevents is on atelet itself -- without it, a node of
// stuck-but-accepting sockets would hold one hung call per ateom for the full
// statsRPCTimeout. With it, such a node degrades the sweep to
// ceil(n/statsSweepConcurrency) timeouts instead of n.
const statsSweepConcurrency = 8

// workerPoolListTimeout bounds the per-sweep pod list that resolves worker
// pools. Pool labels are enrichment: better one unlabeled tick than a sweep
// blocked on the apiserver.
const workerPoolListTimeout = 10 * time.Second

// clampActorStatsPollInterval enforces the floor on a nonzero configured
// interval, warning rather than obeying: an interval below the worst-case
// sweep would pile overlapping polls onto the same guest agent.
func clampActorStatsPollInterval(ctx context.Context, configured time.Duration) time.Duration {
	if configured > 0 && configured < minActorStatsPollInterval {
		slog.WarnContext(ctx, "actor-stats-poll-interval below the worst-case sweep; clamping",
			slog.Duration("configured", configured), slog.Duration("clamped_to", minActorStatsPollInterval))
		return minActorStatsPollInterval
	}
	return configured
}

// activeStatsClient is the one RPC the poller makes, as a narrow interface so
// tests can fake an ateom without a socket. ateompb.AteomClient satisfies it.
type activeStatsClient interface {
	GetActiveWorkloadStats(ctx context.Context, req *ateompb.GetActiveWorkloadStatsRequest, opts ...grpc.CallOption) (*ateompb.GetActiveWorkloadStatsResponse, error)
}

// statsPoller discovers the node's ateoms from the filesystem and turns their
// workload samples into template-level metrics.
//
// It holds no worker-to-actor mapping and never asks the control plane: every
// ateom registers itself on disk by creating its socket directory at boot (the
// same sockets the lifecycle RPCs dial), so one readdir plus one probe per
// socket is complete discovery, and an atelet restart loses nothing because
// nothing was held. Attribution comes solely from the identity echoed inside
// each sample, per the RPC's contract.
type statsPoller struct {
	// interval between the end of one sweep and the start of the next. Sweeps
	// never overlap: a slow sweep delays the next tick rather than stacking a
	// second poll onto the same guests.
	interval time.Duration

	// ateomsDir is the directory whose entries are worker pod UIDs
	// (ateompath.AteomsDir on a real node; a fixture in tests).
	ateomsDir string

	// dial returns a stats client for one ateom plus the closer that releases
	// its connection; the probe closes it before returning, so a connection
	// lives exactly one probe. Deliberately NOT the lifecycle RPCs' cached
	// AteomDialer: at one probe per ateom per minute over a local unix socket
	// a cache saves nothing, and sweeping the node's stale sockets through a
	// shared cache would let telemetry evict connections the lifecycle RPCs
	// are using.
	dial func(ctx context.Context, podUID string) (activeStatsClient, io.Closer, error)

	// workerPools resolves this node's worker pod UIDs to the pool that owns
	// them, called once per sweep. Nil (or a nil map, or a missing entry)
	// degrades to samples grouped without pool labels rather than dropped: the
	// pool is enrichment, the sample is the point. The real resolver lists the
	// node's pods by the ate.dev/worker-pool label the pool controller stamps
	// on every worker (see workerpool_apply.go); the ateom directory name IS
	// the worker pod UID, which is the join key.
	workerPools func(ctx context.Context) map[string]workerPoolRef

	inst *statsInstruments

	// lastCPU is the previous sweep's cpu_usage_usec per actor uid, the
	// baseline the next sweep's deltas are computed against. Only the sweep
	// loop touches it (under collect's mutex), and entries for actors a sweep
	// did not see are dropped at its end -- an actor that leaves the node
	// stops occupying memory here, and one that comes BACK later simply
	// re-baselines. Empty after an atelet restart, so the first sweep
	// contributes zero deltas: an undercount, never an overcount.
	lastCPU map[string]uint64
}

// templateAggregate is one tick's sums for one templateKey group: the bounded
// label set #174 permits on a TSDB series. Actor and
// atespace identity deliberately never reach a metric label; per-actor detail
// is the events channel's job, not this one's.
//
// The memory fields are point-in-time sums the gauges observe. cpuDeltaUsec is
// different: cpu_usage_usec is a cumulative per-epoch counter per actor, so
// summing the raw values across a churning actor set would be meaningless to
// rate() -- instead the poller tracks each actor's last seen value and this
// carries the sweep's INCREASE, which tick adds onto a monotonic counter.
// Counter semantics survive actors joining, leaving, and resetting epochs by
// construction.
type templateAggregate struct {
	sampledActors         int64
	memoryCurrentBytes    int64
	memoryWorkingSetBytes int64
	cpuDeltaUsec          int64
}

// workerPoolRef names one WorkerPool: the pod's namespace and the
// ate.dev/worker-pool label value.
type workerPoolRef struct {
	namespace string
	name      string
}

// templateKey groups samples for aggregation.
type templateKey struct {
	templateNamespace string
	templateName      string
	sandboxClass      string
	source            string
	// workerPool is zero-valued when the pod could not be resolved to a pool
	// (resolver disabled, list failure, pod already gone): those samples group
	// together without pool labels rather than vanish.
	workerPool workerPoolRef
}

// attrs is the bounded label set for one aggregation group. The pool keys are
// omitted while unresolved rather than emitted as empty-string series,
// following the snapshotOp precedent.
func (k templateKey) attrs() metric.MeasurementOption {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs,
		ateattr.TemplateNamespaceKey.String(k.templateNamespace),
		ateattr.TemplateNameKey.String(k.templateName),
		ateattr.SandboxClassKey.String(k.sandboxClass),
		ateattr.StatsSourceKey.String(k.source),
	)
	if k.workerPool != (workerPoolRef{}) {
		attrs = append(attrs,
			ateattr.WorkerPoolNamespaceKey.String(k.workerPool.namespace),
			ateattr.WorkerPoolNameKey.String(k.workerPool.name),
		)
	}
	return metric.WithAttributes(attrs...)
}

// run polls until ctx is cancelled. The caller has already validated and
// clamped interval.
func (p *statsPoller) run(ctx context.Context) {
	slog.InfoContext(ctx, "Actor stats poller starting", slog.Duration("interval", p.interval))
	for {
		p.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.interval):
		}
	}
}

// tick sweeps every ateom on the node once, adds the sweep's CPU increases
// onto the counters, and publishes the aggregates for the next metric
// collection to observe.
func (p *statsPoller) tick(ctx context.Context) {
	aggs := p.collect(ctx)
	p.inst.addCPU(ctx, aggs)
	p.inst.publish(aggs)
}

// collect probes every ateom directory, statsSweepConcurrency at a time, and
// aggregates the samples it gets.
//
// One tolerance rule covers all the noise a scan meets: any failure to dial or
// call an entry means "not a target this tick", never an error worth more than
// a debug line. That uniformly handles stale directories left by deleted
// worker pods (nothing garbage-collects them eagerly), ateoms that have made
// their directory but not yet listened, and workers torn down mid-sweep. The
// no-sample reasons are equally routine: NO_WORKLOAD is an idle worker,
// NOT_MEASURABLE_YET is a boot or restore in progress -- both are skips by the
// RPC's own contract.
func (p *statsPoller) collect(ctx context.Context) map[templateKey]*templateAggregate {
	entries, err := os.ReadDir(p.ateomsDir)
	if err != nil {
		// A node with no ateoms directory yet has no workers to measure; the
		// first RunWorkload dispatch creates it.
		slog.DebugContext(ctx, "Actor stats sweep: no ateoms directory", slog.Any("err", err))
		return nil
	}

	var pools map[string]workerPoolRef
	if p.workerPools != nil {
		pools = p.workerPools(ctx)
	}

	var (
		mu      sync.Mutex
		aggs    = make(map[templateKey]*templateAggregate)
		seenCPU = make(map[string]uint64)
		g       errgroup.Group
	)
	g.SetLimit(statsSweepConcurrency)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		podUID := e.Name()

		g.Go(func() error {
			// One deadline over the whole probe, dial included. The real dial
			// is lazy (grpc.NewClient touches no socket), but the seam does not
			// promise that: a blocking dial implementation must not be able to
			// park a sweep slot past the probe budget.
			callCtx, cancel := context.WithTimeout(ctx, statsRPCTimeout)
			defer cancel()

			client, closer, err := p.dial(callCtx, podUID)
			if err != nil {
				slog.DebugContext(ctx, "Actor stats sweep: skipping ateom", slog.String("pod_uid", podUID), slog.Any("err", err))
				return nil
			}
			defer closer.Close()

			resp, err := client.GetActiveWorkloadStats(callCtx, &ateompb.GetActiveWorkloadStatsRequest{})
			if err != nil {
				slog.DebugContext(ctx, "Actor stats sweep: skipping ateom", slog.String("pod_uid", podUID), slog.Any("err", err))
				return nil
			}

			sample := resp.GetSample()
			if sample == nil {
				// NO_WORKLOAD or NOT_MEASURABLE_YET: normal answers, nothing to
				// add.
				return nil
			}

			key := templateKey{
				templateNamespace: sample.GetActorTemplateNamespace(),
				templateName:      sample.GetActorTemplateName(),
				sandboxClass:      sandboxClassLabel(sample.GetSandboxClass()),
				source:            statsSourceLabel(sample.GetSource()),
				workerPool:        pools[podUID],
			}
			mu.Lock()
			defer mu.Unlock()
			agg := aggs[key]
			if agg == nil {
				agg = &templateAggregate{}
				aggs[key] = agg
			}
			agg.sampledActors++
			agg.memoryCurrentBytes += int64(sample.GetMemoryCurrentBytes())
			agg.memoryWorkingSetBytes += int64(sample.GetMemoryWorkingSetBytes())

			// The counter increase this sample represents. A decrease means the
			// epoch reset underneath us (the cgroup source restarts at zero on
			// restore), so the new value IS the usage since the reset. A sample
			// with NO baseline charges nothing and only records one: atelet
			// cannot tell a new actor from its own restart, and charging the
			// whole epoch-so-far would re-count hours of usage the previous
			// atelet already counted, as one artificial spike. The bounded
			// price is that every actor's boot-to-first-poll usage goes
			// uncounted -- the events channel carries per-actor precision.
			cpu := sample.GetCpuUsageUsec()
			seenCPU[sample.GetActorUid()] = cpu
			if last, ok := p.lastCPU[sample.GetActorUid()]; ok {
				if last <= cpu {
					agg.cpuDeltaUsec += int64(cpu - last)
				} else {
					agg.cpuDeltaUsec += int64(cpu)
				}
			}
			return nil
		})
	}
	// The tasks only ever return nil: a probe that fails is "not a target this
	// tick", never a failed sweep.
	_ = g.Wait()
	// Replacing (not merging) the baselines drops actors this sweep did not
	// see, so lastCPU cannot grow with actor churn.
	p.lastCPU = seenCPU
	return aggs
}

// sandboxClassLabel maps the wire enum to the ate.sandbox.class label values
// the rest of the system uses.
func sandboxClassLabel(c ateompb.SandboxClass) string {
	switch c {
	case ateompb.SandboxClass_SANDBOX_CLASS_GVISOR:
		return "gvisor"
	case ateompb.SandboxClass_SANDBOX_CLASS_MICROVM:
		return "microvm"
	default:
		return ateattr.SandboxClassUnknown
	}
}

// statsSourceLabel maps the wire enum to the ate.stats.source label values.
func statsSourceLabel(s ateompb.StatsSource) string {
	switch s {
	case ateompb.StatsSource_STATS_SOURCE_CGROUP:
		return ateattr.StatsSourceCgroup
	case ateompb.StatsSource_STATS_SOURCE_GUEST_AGENT:
		return ateattr.StatsSourceGuestAgent
	default:
		return ateattr.StatsSourceUnspecified
	}
}

// nodeWorkerPools returns a resolver that lists nodeName's worker pods once
// per sweep and maps pod UID to the pool that owns it. One field-selected,
// label-selected LIST per interval per node is deliberately chosen over a
// standing informer: at the poll cadence the apiserver cost is negligible,
// there is no cache to sync before the first sweep, and a failed list
// degrades to unlabeled samples for one tick instead of blocking anything.
func nodeWorkerPools(client kubernetes.Interface, nodeName string) func(ctx context.Context) map[string]workerPoolRef {
	return func(ctx context.Context) map[string]workerPoolRef {
		// Bounded so a hung apiserver connection cannot stall the sweep it is
		// merely enriching: past the deadline, this tick's samples group
		// without pool labels, which is the same answer as any other failed
		// list.
		listCtx, cancel := context.WithTimeout(ctx, workerPoolListTimeout)
		defer cancel()
		pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(listCtx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + nodeName,
			LabelSelector: workerPoolLabel,
		})
		if err != nil {
			slog.DebugContext(ctx, "Actor stats sweep: worker pool resolution failed; samples group without pool labels", slog.Any("err", err))
			return nil
		}
		pools := make(map[string]workerPoolRef, len(pods.Items))
		for _, pod := range pods.Items {
			pools[string(pod.UID)] = workerPoolRef{
				namespace: pod.Namespace,
				name:      pod.Labels[workerPoolLabel],
			}
		}
		return pools
	}
}

// Metric names follow the OTel semantic-convention shape for container
// resource metrics (container.cpu.time, container.memory.usage,
// container.memory.working_set): dot-separated resource.measurement leaves,
// units carried by the instrument's unit field rather than the name --
// exporters re-attach them per their own conventions (the Prometheus
// rendering of memory.working_set is ate_actor_stats_memory_working_set_bytes).
const (
	sampledActorsMetric = "ate.actor.stats.sampled_actors"
	memoryCurrentMetric = "ate.actor.stats.memory.usage"
	workingSetMetric    = "ate.actor.stats.memory.working_set"
	cpuUsageMetric      = "ate.actor.stats.cpu.time"
)

// statsInstruments exposes the latest sweep's aggregates as observable
// gauges: each metric collection observes exactly the groups the last sweep
// found, so a group that vanishes (last actor of a template leaves the node)
// genuinely disappears from the export. Synchronous gauges would not do that
// -- the SDK re-exports a sync instrument's last recorded value on every
// collection until process exit, which would keep reporting memory for actors
// long gone.
type statsInstruments struct {
	// latest is the snapshot the callback reads: written whole by publish,
	// never mutated in place.
	latest atomic.Pointer[map[templateKey]*templateAggregate]

	// cpuUsage is a plain synchronous counter, unlike the gauges: the sweep's
	// per-actor increases are ADDED here, and cumulative-counter semantics --
	// including a vanished template's series holding its final value rather
	// than disappearing -- are exactly what rate() consumers expect.
	cpuUsage metric.Float64Counter
}

func newStatsInstruments(meter metric.Meter) (*statsInstruments, error) {
	i := &statsInstruments{}

	cpuUsage, err := meter.Float64Counter(
		cpuUsageMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Cumulative CPU time consumed by running actors, in seconds."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s counter: %w", cpuUsageMetric, err)
	}
	i.cpuUsage = cpuUsage

	sampledActors, err := meter.Int64ObservableGauge(
		sampledActorsMetric,
		metric.WithUnit("{actor}"),
		metric.WithDescription("Number of running actors with a current resource usage measurement."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s gauge: %w", sampledActorsMetric, err)
	}
	memoryCurrent, err := meter.Int64ObservableGauge(
		memoryCurrentMetric,
		metric.WithUnit("By"),
		metric.WithDescription("Aggregated current memory usage of running actors, in bytes, including reclaimable page cache."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s gauge: %w", memoryCurrentMetric, err)
	}
	workingSet, err := meter.Int64ObservableGauge(
		workingSetMetric,
		metric.WithUnit("By"),
		metric.WithDescription("Aggregated current memory working set of running actors, in bytes."),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s gauge: %w", workingSetMetric, err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		snapshot := i.latest.Load()
		if snapshot == nil {
			return nil
		}
		for key, agg := range *snapshot {
			opt := key.attrs()
			o.ObserveInt64(sampledActors, agg.sampledActors, opt)
			o.ObserveInt64(memoryCurrent, agg.memoryCurrentBytes, opt)
			o.ObserveInt64(workingSet, agg.memoryWorkingSetBytes, opt)
		}
		return nil
	}, sampledActors, memoryCurrent, workingSet)
	if err != nil {
		return nil, fmt.Errorf("register actor stats callback: %w", err)
	}

	return i, nil
}

// publish makes aggs the snapshot the next collection observes. A nil
// receiver is a valid no-op, like Instruments.
func (i *statsInstruments) publish(aggs map[templateKey]*templateAggregate) {
	if i == nil {
		return
	}
	i.latest.Store(&aggs)
}

// addCPU adds one sweep's CPU increases onto the counters. A nil receiver is
// a valid no-op, like Instruments.
func (i *statsInstruments) addCPU(ctx context.Context, aggs map[templateKey]*templateAggregate) {
	if i == nil {
		return
	}
	for key, agg := range aggs {
		// The wire carries microseconds; the metric is seconds, the base unit
		// CPU time is exported in everywhere else (cAdvisor's
		// container_cpu_usage_seconds_total, OTel's *.cpu.time), so the
		// existing rate() idioms read directly as cores.
		i.cpuUsage.Add(ctx, float64(agg.cpuDeltaUsec)/1e6, key.attrs())
	}
}

// startStatsPoller assembles the poller and starts it. Split from main's boot
// sequence so the sampling subsystem has one obvious entry point.
//
// The poller dials its own per-probe connections (see statsPoller.dial) and
// takes no AteomDialer: the isolation from the lifecycle RPCs' connection
// cache is structural, not just behavioral.
func startStatsPoller(ctx context.Context, interval time.Duration, inst *statsInstruments, k8sClient kubernetes.Interface) {
	poller := &statsPoller{
		interval:  interval,
		ateomsDir: ateompath.AteomsDir(),
		dial: func(_ context.Context, podUID string) (activeStatsClient, io.Closer, error) {
			conn, err := grpc.NewClient(
				"unix://"+ateompath.AteomSocketPath(podUID),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			)
			if err != nil {
				return nil, nil, err
			}
			return ateompb.NewAteomClient(conn), conn, nil
		},
		inst: inst,
	}
	// NODE_NAME comes from the Downward API; without it the samples still
	// flow, just grouped without pool labels.
	if nodeName := os.Getenv("NODE_NAME"); nodeName != "" {
		poller.workerPools = nodeWorkerPools(k8sClient, nodeName)
	} else {
		slog.WarnContext(ctx, "NODE_NAME not set; actor stats will carry no worker pool labels")
	}
	go poller.run(ctx)
}
