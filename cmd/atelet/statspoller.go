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
	"math"
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
	"github.com/agent-substrate/substrate/internal/ateomstats"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// workerPoolLabel is the label the pool controller stamps on every worker pod
// it creates; the value is the WorkerPool's name and the pod's namespace is
// the pool's. Must agree with cmd/atecontroller/internal/controllers/
// workerpool_apply.go.
const workerPoolLabel = "ate.dev/worker-pool"

// minActorStatsPollInterval is the floor a configured poll interval is clamped
// to. The ateoms serve GetActiveWorkloadStats from their own sampling timer, so
// a poll faster than their sample interval only reads the same sample again.
// The floor also bounds the worker pod list each sweep sends the apiserver
// (resolveWorkerPools), so do not lower it to cut lag.
const minActorStatsPollInterval = 50 * time.Second

// statsRPCTimeout bounds one ateom's GetActiveWorkloadStats call. The ateom
// answers from memory in under a millisecond, whatever its actor count or
// guest speed, so the timeout is only for a stuck socket.
const statsRPCTimeout = 5 * time.Second

// statsSweepConcurrency bounds how many ateoms one sweep probes at once, one
// call per ateom whatever it hosts. It caps how many stuck sockets can hold a
// hung call open on atelet at the same time.
const statsSweepConcurrency = 8

// workerPoolListTimeout bounds the per-sweep pod list that fetches worker
// pools: pool labels are enrichment, so a slow apiserver must not stall the
// sweep it is merely enriching.
const workerPoolListTimeout = 10 * time.Second

// clampActorStatsPollInterval enforces the floor on a nonzero configured
// interval, warning rather than obeying.
func clampActorStatsPollInterval(ctx context.Context, configured time.Duration) time.Duration {
	if configured > 0 && configured < minActorStatsPollInterval {
		slog.WarnContext(ctx, "actor-stats-poll-interval below the floor; clamping",
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
// socket is complete discovery, and discovery survives an atelet restart.
// Attribution comes solely from the identity echoed inside each sample, per
// the RPC's contract.
type statsPoller struct {
	// interval between the end of one sweep and the start of the next. Sweeps
	// never overlap: a slow sweep delays the next tick rather than stacking a
	// second poll onto the same guests.
	interval time.Duration

	// ateomsDir is the directory whose entries are worker pod UIDs
	// (nodepath.AteomsDir on a real node; a fixture in tests).
	ateomsDir string

	// dial returns a stats client for one ateom plus the closer that releases
	// its connection; a connection lives exactly one probe. Deliberately NOT
	// the lifecycle RPCs' cached AteomDialer: at one probe per ateom per
	// minute over a local unix socket a cache saves nothing, and sweeping the
	// node's stale sockets through a shared cache would let telemetry evict
	// connections the lifecycle RPCs are using.
	dial func(ctx context.Context, podUID string) (activeStatsClient, io.Closer, error)

	// fetchWorkerPools is the raw fetch: one apiserver list mapping this
	// node's worker pod UIDs to the pool that owns them, called once per
	// sweep by resolveWorkerPools, which layers cachedPools over it. A nil
	// return means the fetch failed. The real fetcher lists the node's pods
	// by workerPoolLabel; the ateom directory name IS the worker pod UID,
	// which is the join key.
	fetchWorkerPools func(ctx context.Context) map[string]workerPoolRef

	inst *statsInstruments

	// cachedPools carries pool resolutions across sweeps, so one failed pod
	// list cannot re-home a tick's samples onto a pool-less label set. Safe
	// because a pod's pool is immutable for the pod's lifetime: an
	// entry can be stale, never wrong. Only the sweep loop touches it;
	// resolveWorkerPools prunes it to the pods whose ateom directories still
	// exist.
	cachedPools map[string]workerPoolRef
}

// templateAggregate is one tick's sums for one templateKey group: the
// bounded label set #174 permits on a TSDB series. Actor and atespace
// identity deliberately never reach a metric label; per-actor detail is on
// the ateoms' usage records.
//
// The memory fields are point-in-time sums the gauges observe.
type templateAggregate struct {
	sampledActors         int64
	memoryCurrentBytes    int64
	memoryWorkingSetBytes int64
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
	// workerPool is zero-valued when no fetch has resolved the pod yet (see
	// resolveWorkerPools): those samples group together without pool labels
	// rather than vanish.
	workerPool workerPoolRef
}

// attrs is the bounded label set for one aggregation group. The pool keys are
// omitted while unresolved rather than emitted as empty-string series,
// following the snapshotOp precedent.
func (k templateKey) attrs() metric.MeasurementOption {
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs,
		ateattr.TemplateAtespaceKey.String(k.templateNamespace),
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

// run polls until ctx is canceled. The caller has already validated and
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

// tick sweeps every ateom on the node once and publishes the aggregates for
// the next metric collection to observe.
func (p *statsPoller) tick(ctx context.Context) {
	aggs := p.collect(ctx)
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
// no-sample answers are equally routine: an empty samples list is an idle
// worker, and a pending entry (source UNSPECIFIED) is a workload the ateom
// has not measured (boot, restore, or an unreached guest), which adds nothing.
func (p *statsPoller) collect(ctx context.Context) map[templateKey]*templateAggregate {
	entries, err := os.ReadDir(p.ateomsDir)
	if err != nil {
		// A node with no ateoms directory yet has no workers to measure; the
		// first RunWorkload dispatch creates it.
		slog.DebugContext(ctx, "Actor stats sweep: no ateoms directory", slog.Any("err", err))
		return nil
	}

	podUIDs := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			podUIDs = append(podUIDs, e.Name())
		}
	}
	pools := p.resolveWorkerPools(ctx, podUIDs)

	var (
		mu   sync.Mutex
		aggs = make(map[templateKey]*templateAggregate)
		g    errgroup.Group
	)
	g.SetLimit(statsSweepConcurrency)
	for _, podUID := range podUIDs {

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

			// One entry per workload the ateom is hosting; empty when it is
			// available.
			for _, sample := range resp.GetSamples() {
				if sample.GetSource() == ateompb.StatsSource_STATS_SOURCE_UNSPECIFIED {
					// Pending: hosted but not measured, so it adds nothing.
					continue
				}
				key := templateKey{
					templateNamespace: sample.GetActorTemplateAtespace(),
					templateName:      sample.GetActorTemplateName(),
					sandboxClass:      ateomstats.SandboxClassLabel(sample.GetSandboxClass()),
					source:            ateomstats.StatsSourceLabel(sample.GetSource()),
					workerPool:        pools[podUID],
				}
				mu.Lock()
				agg := aggs[key]
				if agg == nil {
					agg = &templateAggregate{}
					aggs[key] = agg
				}
				agg.sampledActors++
				agg.memoryCurrentBytes = addSat(agg.memoryCurrentBytes, sample.GetMemoryCurrentBytes())
				agg.memoryWorkingSetBytes = addSat(agg.memoryWorkingSetBytes, sample.GetMemoryWorkingSetBytes())
				mu.Unlock()
			}
			return nil
		})
	}
	// The tasks only ever return nil: a probe that fails is "not a target this
	// tick", never a failed sweep.
	_ = g.Wait()
	return aggs
}

// addSat adds a sample's uint64 counter onto an int64 aggregate, saturating at
// MaxInt64 at both hops: converting the wire value and adding it. The wire
// carries whatever the guest kernel -- or, on the micro-VM runtime, the guest
// agent -- reported, and a corrupt reading above 2^63 must degrade to a pinned
// ceiling, not flip a gauge negative. The same reasoning as
// agentstats.Sample.Plus, one type boundary later.
func addSat(agg int64, v uint64) int64 {
	if v > math.MaxInt64 {
		v = math.MaxInt64
	}
	if agg > math.MaxInt64-int64(v) {
		return math.MaxInt64
	}
	return agg + int64(v)
}

// resolveWorkerPools returns this sweep's pod-UID-to-pool map: fresh
// resolutions win, a failed or partial list falls back to cachedPools, and
// the result -- rebuilt restricted to the pods whose ateom directories exist
// -- becomes the new cache, pruning departed pods and bounding its size. The
// residual unlabeled case is a pod no fetch has resolved yet, whether first
// seen during an outage or omitted by a list racing the directory scan; it
// groups without pool labels until a fetch returns it.
func (p *statsPoller) resolveWorkerPools(ctx context.Context, podUIDs []string) map[string]workerPoolRef {
	if p.fetchWorkerPools == nil {
		return nil
	}
	if len(podUIDs) == 0 {
		// Nothing to resolve: skip the fetch and prune the cache to the
		// empty directory set, as a normal sweep would.
		p.cachedPools = nil
		return nil
	}
	fresh := p.fetchWorkerPools(ctx)
	merged := make(map[string]workerPoolRef, len(podUIDs))
	for _, uid := range podUIDs {
		if ref, ok := fresh[uid]; ok {
			merged[uid] = ref
		} else if ref, ok := p.cachedPools[uid]; ok {
			merged[uid] = ref
		}
	}
	p.cachedPools = merged
	return merged
}

// newWorkerPoolFetcher returns the fetch func behind fetchWorkerPools: one
// field-selected, label-selected LIST of nodeName's worker pods per call.
// Deliberately not a standing informer: at the poll cadence the apiserver
// cost is negligible, there is no informer cache to sync before the first
// sweep, and resolveWorkerPools already absorbs failed lists.
func newWorkerPoolFetcher(client kubernetes.Interface, nodeName string) func(ctx context.Context) map[string]workerPoolRef {
	return func(ctx context.Context) map[string]workerPoolRef {
		// Bounded so a hung apiserver cannot stall the sweep: past the
		// deadline the fetch returns nil and resolveWorkerPools falls back
		// to the cache.
		listCtx, cancel := context.WithTimeout(ctx, workerPoolListTimeout)
		defer cancel()
		pods, err := client.CoreV1().Pods(metav1.NamespaceAll).List(listCtx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=" + nodeName,
			LabelSelector: workerPoolLabel,
		})
		if err != nil {
			slog.DebugContext(ctx, "Actor stats sweep: worker pool list failed; answering from cached resolutions", slog.Any("err", err))
			return nil
		}
		pools := make(map[string]workerPoolRef, len(pods.Items))
		for _, pod := range pods.Items {
			name := pod.Labels[workerPoolLabel]
			if name == "" {
				// The existence selector also matches empty-valued labels
				// (a bare key in YAML parses to ""), and half a pair names
				// no pool: skip it, so absent and unresolvable are the same
				// unlabeled answer downstream.
				continue
			}
			pools[string(pod.UID)] = workerPoolRef{
				namespace: pod.Namespace,
				name:      name,
			}
		}
		return pools
	}
}

// Metric names follow the OTel semantic-convention shape for container
// resource metrics (container.memory.usage, container.memory.working_set):
// dot-separated resource.measurement leaves,
// units carried by the instrument's unit field rather than the name --
// exporters re-attach them per their own conventions (the Prometheus
// rendering of memory.working_set is ate_actor_stats_memory_working_set_bytes).
const (
	sampledActorsMetric = "ate.actor.stats.sampled_actors"
	memoryCurrentMetric = "ate.actor.stats.memory.usage"
	workingSetMetric    = "ate.actor.stats.memory.working_set"
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
}

func newStatsInstruments(meter metric.Meter) (*statsInstruments, error) {
	i := &statsInstruments{}

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

// startStatsPoller assembles the metrics poller and starts it. Split from
// main's boot sequence so the subsystem has one obvious entry point.
//
// The poller dials its own short-lived connection per probe (see
// dialAteomStats) and takes no AteomDialer: the isolation from the lifecycle
// RPCs' connection cache is structural, not just behavioral.
func startStatsPoller(ctx context.Context, interval time.Duration, inst *statsInstruments, k8sClient kubernetes.Interface) {
	poller := &statsPoller{
		interval:  interval,
		ateomsDir: nodepath.AteomsDir(),
		dial: func(_ context.Context, podUID string) (activeStatsClient, io.Closer, error) {
			conn, closer, err := dialAteomStats(podUID)
			if err != nil {
				return nil, nil, err
			}
			return ateompb.NewAteomClient(conn), closer, nil
		},
		inst: inst,
	}
	// NODE_NAME comes from the Downward API; without it the samples still
	// flow, just grouped without pool labels.
	if nodeName := os.Getenv("NODE_NAME"); nodeName != "" {
		poller.fetchWorkerPools = newWorkerPoolFetcher(k8sClient, nodeName)
	} else {
		slog.WarnContext(ctx, "NODE_NAME not set; actor stats will carry no worker pool labels")
	}
	go poller.run(ctx)
}

// dialAteomStats opens the poller's short-lived connection: one per probe,
// closed by the caller, never the lifecycle RPCs' cached AteomDialer.
// grpc.NewClient is lazy, so this cannot block; the caller's deadline bounds
// the actual connect inside the RPC.
func dialAteomStats(podUID string) (*grpc.ClientConn, io.Closer, error) {
	conn, err := grpc.NewClient(
		"unix://"+nodepath.AteomSocketPath(podUID),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, nil, err
	}
	return conn, conn, nil
}
