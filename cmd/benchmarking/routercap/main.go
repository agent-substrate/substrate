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

// routercap measures one arm of the atenet-router capacity sweep: one Envoy
// CPU size, one ladder of offered load, one pair of output streams. Arm
// changes live in run.sh, so this binary needs no write access to the cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/glutton"
	"github.com/agent-substrate/substrate/internal/benchmarking/routercap"
	"k8s.io/client-go/kubernetes"
)

// Exit codes. Distinct so automation can tell "we could not measure this" from
// "the router fell over" without parsing a log.
const (
	exitOK          = 0
	exitFailed      = 1
	exitInterrupted = 2
	exitRigLimited  = 3
	exitPreflight   = 4
)

type config struct {
	// What is being measured.
	arm  int
	pass int
	// expectConcurrency overrides the thread count the startup check demands.
	// Zero means the arm: cores and threads are one variable except in the
	// diagnostic runs that exist to split them.
	expectConcurrency int

	// Where things are.
	kubeconfig       string
	apiEndpoint      string
	routerNamespace  string
	routerSelector   string
	routerPods       int
	workerNamespace  string
	workerSelector   string
	loadgenPod       string
	loadgenNS        string
	loadgenNode      string
	loadgenContainer string

	// The ladder.
	ladder routercap.LadderSpec

	// The actor pool.
	atespace        string
	actors          int
	warmConcurrency int

	// The generator's transport.
	maxInFlight    int64
	requestTimeout time.Duration
	fineInterval   time.Duration
	drainTimeout   time.Duration
	tickCap        time.Duration

	// Sampling.
	pollInterval time.Duration
	maxWait      time.Duration

	// Facts about the deployment under test, recorded so the run explains
	// itself and the design's ordering claim can be checked from the output.
	portRange           string
	circuitBreakerLimit int
	extProcMaxRequests  int

	// Guards.
	guards routercap.GuardConfig

	// Output.
	outputDir       string
	dest            string
	name            string
	tag             string
	recordsToStdout bool
	gitSHA          string
	cluster         string
	location        string
	machineType     string
}

func main() {
	cfg := parseFlags()
	// Logs on stderr, records on stdout. Keeping them apart is what lets run.sh
	// treat the pod's stdout as a data stream rather than something to grep.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// After flags, before anything runs: the client-port guard's ceiling comes
	// from this pod's own source-port range, which the Job spec widens via
	// sysctl. The resolved number is serialized into the run header.
	cfg.guards.ResolveClientCeiling(os.ReadFile, slog.Default())

	code := run(cfg)
	os.Exit(code)
}

func parseFlags() *config {
	c := &config{guards: routercap.DefaultGuardConfig()}

	flag.IntVar(&c.arm, "arm", 0, "Envoy container CPU limit in cores for this arm. Stamped on every record, and checked against envoy_server_concurrency at startup so a patch that did not take fails here rather than producing a mislabeled series.")
	flag.IntVar(&c.expectConcurrency, "expect-concurrency", 0, "Worker-thread count Envoy is expected to report. Zero means the arm's core count — the normal case, where cores and threads are one variable. Set only by diagnostic runs that decouple them (run.sh RC_CONCURRENCY).")
	flag.IntVar(&c.pass, "pass", 1, "Which repeat of this arm. Two passes of the same arm that disagree is itself a finding.")

	flag.StringVar(&c.kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only used when not running in-cluster.")
	flag.StringVar(&c.apiEndpoint, "api-endpoint", "dns:///api.ate-system.svc.cluster.local:443", "ateapi gRPC dial target, used to warm the actor pool.")
	flag.StringVar(&c.routerNamespace, "router-namespace", "ate-system", "Namespace of the router pod under test.")
	flag.StringVar(&c.routerSelector, "router-selector", "app=atenet-router", "Label selector for the router pod. Must match exactly -router-pods running pods: more or fewer means a rollout is in progress and measuring the blend would label two configurations as one.")
	flag.IntVar(&c.routerPods, "router-pods", 1, "How many router replicas the selector must resolve to. Above 1, load round-robins across the pod IPs (each actor sticks to one replica), while the Envoy/sidecar metric scrapes and the span breakdown describe the first pod only — the per-pod instruments see 1/N of the traffic and the run header says so.")
	flag.StringVar(&c.workerNamespace, "worker-namespace", "benchmark-workloads", "Namespace of the worker pods the actors run on.")
	flag.StringVar(&c.workerSelector, "worker-selector", "ate.dev/worker-pool", "Label selector for worker pods. Their CPU is recorded because atunnel now terminates mTLS in the request path.")
	flag.StringVar(&c.loadgenPod, "loadgen-pod", os.Getenv("POD_NAME"), "This pod's name, for measuring the load generator itself. Defaults to $POD_NAME.")
	flag.StringVar(&c.loadgenNS, "loadgen-namespace", os.Getenv("POD_NAMESPACE"), "This pod's namespace. Defaults to $POD_NAMESPACE.")
	flag.StringVar(&c.loadgenNode, "loadgen-node", os.Getenv("NODE_NAME"), "The node this pod runs on. Defaults to $NODE_NAME.")
	flag.StringVar(&c.loadgenContainer, "loadgen-container", "routercap", "This container's name, as cAdvisor reports it.")

	flag.Float64Var(&c.ladder.StartQPS, "start-qps", 1000, "First rung's offered rate.")
	flag.Float64Var(&c.ladder.StepQPS, "step-qps", 1000, "Rate added by each subsequent rung.")
	flag.IntVar(&c.ladder.Rungs, "rungs", 16, "Number of rungs. No early stop: the flat region above saturation is data.")
	flag.DurationVar(&c.ladder.Hold, "hold", 45*time.Second, "How long each rung runs.")
	flag.DurationVar(&c.ladder.Warmup, "warmup", 10*time.Second, "Leading part of each rung flagged as warmup. Still written: a rung's first seconds are where the connection pool grows.")

	flag.StringVar(&c.atespace, "atespace", "routercap", "Atespace the run's actors live in.")
	flag.IntVar(&c.actors, "actors", 100, "Actors to warm, one per worker pod. Sized so the per-worker connection-rate limit never binds before the concurrency limit.")
	flag.IntVar(&c.warmConcurrency, "warm-concurrency", 16, "Parallelism for actor setup and teardown.")

	flag.Int64Var(&c.maxInFlight, "max-in-flight", 70000, "Generator's own concurrency cap. Reaching it is a rig failure recorded as shed requests, never a statement about the router; set above the widened source-port budget (the Job spec's ip_local_port_range sysctl, 64,511 ports) so the router's limits bind first.")
	flag.DurationVar(&c.requestTimeout, "request-timeout", 30*time.Second, "Per-request timeout. Timeouts count as failures and contribute their full latency to the percentiles.")
	flag.DurationVar(&c.fineInterval, "fine-interval", time.Second, "Cadence of the generator-only series in fine.jsonl.")
	flag.DurationVar(&c.drainTimeout, "drain-timeout", 30*time.Second, "How long to wait for in-flight requests after the last rung.")
	flag.DurationVar(&c.tickCap, "tick-cap", 2*time.Millisecond, "Upper bound on the pacer's sleep, which bounds the dispatch lag the dispatch loop itself can add.")

	flag.DurationVar(&c.pollInterval, "cadvisor-poll", time.Second, "How often to re-fetch cAdvisor while waiting for the anchor's timestamp to advance. Well below the kubelet's ~10s cadence so window boundaries are the kubelet's, not the poller's.")
	flag.DurationVar(&c.maxWait, "cadvisor-max-wait", 2*time.Minute, "How long a stuck cAdvisor timestamp is tolerated before the arm fails.")

	flag.StringVar(&c.portRange, "port-range", "", "The router pod's measured net.ipv4.ip_local_port_range as \"low-high\". Read from the live pod by run.sh. Left empty the Linux default is assumed and the header says so.")
	flag.IntVar(&c.circuitBreakerLimit, "circuit-breaker-limit", 20000, "The actor cluster's circuit-breaker threshold, for the port-budget series. Must match xds.go.")
	flag.IntVar(&c.extProcMaxRequests, "extproc-max-requests", 20000, "The router's --extproc-max-requests, recorded so the ordering against the port budget is auditable.")

	flag.Float64Var(&c.guards.LoadgenCPUUtilization, "guard-loadgen-cpu", c.guards.LoadgenCPUUtilization, "Trip when the generator container exceeds this fraction of its own CPU limit. Zero disables.")
	flag.Float64Var(&c.guards.WorkerNewConnsPerSec, "guard-worker-conns-per-sec", c.guards.WorkerNewConnsPerSec, "Trip above this mean new-connection rate per worker pod. Zero disables.")
	flag.Float64Var(&c.guards.MinRequestsPerConnection, "guard-min-rq-per-cx", c.guards.MinRequestsPerConnection, "Trip when the generator averages fewer requests per connection than this, meaning keep-alive is not holding. Zero disables.")
	flag.IntVar(&c.guards.ClientConnectionCeiling, "guard-client-connections", c.guards.ClientConnectionCeiling, "Trip when the generator holds more connections than this, past its own source-port headroom. Zero disables.")
	flag.Float64Var(&c.guards.DispatchLagP95Ms, "guard-dispatch-lag-ms", c.guards.DispatchLagP95Ms, "Trip when the generator falls this far behind its own schedule at p95, unless the system is demonstrably saturated. Zero disables.")
	flag.Float64Var(&c.guards.SaturationLatencyP95Ms, "saturation-latency-p95-ms", c.guards.SaturationLatencyP95Ms, "p95 latency at or above which the system counts as saturated, suspending the dispatch-lag guard.")
	flag.Float64Var(&c.guards.SaturationAchievedRatio, "saturation-achieved-ratio", c.guards.SaturationAchievedRatio, "Achieved-over-offered ratio below which the system counts as saturated.")

	flag.StringVar(&c.outputDir, "output-dir", "", "Directory for samples.jsonl, fine.jsonl and run.json. Overrides --dest.")
	flag.StringVar(&c.dest, "dest", "", "Root directory for results; the run lands in <dest>/<name>/<tag>/arm-<N>c. Local paths only.")
	flag.StringVar(&c.name, "name", "routercap", "Test name, for the output path and the header.")
	flag.StringVar(&c.tag, "tag", "", "Run tag, for the output path and the header.")
	flag.BoolVar(&c.recordsToStdout, "records-to-stdout", false, "Also write every record and the header to stdout as tagged JSONL. The router and generator images are distroless, so kubectl cp cannot retrieve a Job's files; this is how an in-cluster run's output gets out. Logs go to stderr either way.")
	flag.StringVar(&c.gitSHA, "git-sha", "", "Commit the router image was built from.")
	flag.StringVar(&c.cluster, "cluster", "", "Cluster name, recorded in the header.")
	flag.StringVar(&c.location, "location", "", "Cluster location, recorded in the header.")
	flag.StringVar(&c.machineType, "machine-type", "", "Machine type of the router node. A run on 88-core nodes is not the same experiment as one on 176-core nodes.")

	flag.Parse()
	return c
}

// run is main's body, returning an exit code rather than calling os.Exit, so
// every deferred teardown actually runs.
func run(cfg *config) int {
	log := slog.Default()

	outDir, err := cfg.resolveOutputDir()
	if err != nil {
		log.Error("preflight failed", "err", err)
		return exitPreflight
	}

	// SIGTERM is how the Job is deleted and Ctrl-C is how a laptop run ends;
	// both must unwind through the actor teardown rather than abandon a
	// hundred running actors on the worker pods.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	h := routercap.RunHeader{
		Name:                cfg.name,
		Tag:                 cfg.tag,
		StartedAt:           time.Now(),
		GitSHA:              cfg.gitSHA,
		Cluster:             cfg.cluster,
		Location:            cfg.location,
		MachineType:         cfg.machineType,
		CircuitBreakerLimit: cfg.circuitBreakerLimit,
		ExtProcMaxRequests:  cfg.extProcMaxRequests,
		ArmCores:            []int{cfg.arm},
		Actors:              cfg.actors,
		Ladder:              cfg.ladder,
		Guards:              cfg.guards,
		Caveats:             routercap.StandingCaveats(),
	}
	var stream *routercap.StreamSink
	if cfg.recordsToStdout {
		stream = routercap.NewStreamSink(os.Stdout)
	}
	// Written on every exit path, including the failing ones. A run directory
	// whose header is missing because the arm aborted is a directory nobody can
	// interpret later.
	defer func() {
		h.FinishedAt = time.Now()
		h.Guards = cfg.guards
		if outDir != "" {
			if err := routercap.WriteHeader(outDir, h); err != nil {
				log.Error("write run header", "err", err)
			}
		}
		if stream != nil {
			if err := stream.Header(h); err != nil {
				log.Error("stream run header", "err", err)
			}
		}
	}()

	rig, err := setup(ctx, cfg, log)
	if rig != nil {
		defer rig.close(log)
		h.RouterPod = rig.router
		if len(rig.routers) > 1 {
			h.RouterPods = rig.routers
		}
		h.Placement = rig.placement
		h.RouterImage = rig.router.Images["envoy"]
		h.PortRange = rig.portRange
		h.Guards = cfg.guards
	}
	if err != nil {
		log.Error("preflight failed", "err", err)
		return exitPreflight
	}

	var files *routercap.JSONLSink
	sinks := routercap.MultiSink{}
	if outDir != "" {
		files, err = routercap.OpenJSONLSink(outDir)
		if err != nil {
			log.Error("preflight failed", "err", err)
			return exitPreflight
		}
		defer files.Close()
		sinks = append(sinks, files)
	}
	if stream != nil {
		sinks = append(sinks, stream)
	}

	runner := &routercap.Runner{
		Arm:                 cfg.arm,
		Pass:                cfg.pass,
		Rungs:               cfg.ladder.Build(),
		Client:              rig.sender,
		Sink:                sinks,
		Windows:             rig.windows,
		Envoy:               rig.envoy,
		Contention:          rig.contention,
		Router:              rig.routerStats,
		Targets:             rig.targets,
		Guards:              cfg.guards,
		PortRange:           rig.portRange,
		CircuitBreakerLimit: cfg.circuitBreakerLimit,
		MaxInFlight:         cfg.maxInFlight,
		TickCap:             cfg.tickCap,
		FineInterval:        cfg.fineInterval,
		DrainTimeout:        cfg.drainTimeout,
		Log:                 log,
	}

	log.Info("arm start",
		"arm", cfg.arm, "pass", cfg.pass, "rungs", cfg.ladder.Rungs,
		"peak_qps", cfg.ladder.PeakQPS(), "actors", cfg.actors, "output", outDir)

	res, runErr := runner.Run(ctx)
	h.Results = []routercap.RunResult{res}

	if files != nil {
		if err := files.Close(); err != nil {
			log.Error("close output files", "err", err)
		}
	}

	var rigErr *routercap.RigLimitedError
	switch {
	case errors.As(runErr, &rigErr):
		log.Error("arm was rig-limited", "arm", cfg.arm, "err", runErr)
		return exitRigLimited
	case errors.Is(runErr, context.Canceled):
		log.Warn("arm interrupted", "arm", cfg.arm, "windows", res.Windows)
		return exitInterrupted
	case runErr != nil:
		log.Error("arm failed", "arm", cfg.arm, "err", runErr)
		return exitFailed
	}

	log.Info("arm complete",
		"arm", cfg.arm, "pass", cfg.pass, "windows", res.Windows,
		"fine_samples", res.FineSamples, "drained", res.Drained,
		"clock_skew_ms", res.ClockSkewMs)
	return exitOK
}

// resolveOutputDir picks where the run writes.
func (c *config) resolveOutputDir() (string, error) {
	if c.outputDir != "" {
		return c.outputDir, nil
	}
	if c.dest == "" {
		// Streaming to stdout is a complete output on its own: it is how the
		// in-cluster Job reports.
		if c.recordsToStdout {
			return "", nil
		}
		return "", fmt.Errorf("one of --output-dir, --dest or --records-to-stdout is required")
	}
	// A remote --dest honored by writing somewhere local would lose the run;
	// refuse until upload is actually wired in.
	if u, err := url.Parse(c.dest); err == nil && u.Scheme != "" {
		return "", fmt.Errorf("--dest %q is remote; remote upload is not wired yet, pass a local --output-dir", c.dest)
	}
	tag := c.tag
	if tag == "" {
		tag = "untagged"
	}
	return filepath.Join(c.dest, c.name, tag, fmt.Sprintf("arm-%dc", c.arm)), nil
}

// rig is everything the runner needs from the cluster, resolved once.
type rig struct {
	// router is the anchor pod; routers is every replica, in name order.
	// Identical single-element views of the same pod at -router-pods=1.
	router      routercap.PodRef
	routers     []routercap.PodRef
	placement   map[string]string
	portRange   routercap.PortRange
	targets     []routercap.Target
	windows     *routercap.WindowDriver
	envoy       *routercap.EnvoyClient
	contention  *routercap.ContentionClient
	routerStats *routercap.RouterClient
	sender      *routercap.Sender
	pool        *routercap.ActorPool
	closers     []func() error
}

// close tears the rig down. Actors first and always: actors left running hold
// worker pods the next arm needs, and the run exits through a signal far more
// often than through a clean finish.
func (r *rig) close(log *slog.Logger) {
	if r.pool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		r.pool.Teardown(ctx)
	}
	if r.sender != nil {
		r.sender.CloseIdleConnections()
	}
	for _, c := range r.closers {
		if err := c(); err != nil {
			log.Warn("close", "err", err)
		}
	}
}

// setup resolves every source and warms the actor pool. It returns a partially
// built rig even on failure so the caller can still tear down what was created
// and still write a header naming what it found.
func setup(ctx context.Context, cfg *config, log *slog.Logger) (*rig, error) {
	r := &rig{placement: map[string]string{}}

	cs, _, err := routercap.NewKubeClient(cfg.kubeconfig)
	if err != nil {
		return r, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	pods := cfg.routerPods
	if pods < 1 {
		pods = 1
	}
	routers, err := routercap.WaitForPods(waitCtx, cs, cfg.routerNamespace, cfg.routerSelector, 2*time.Second, pods)
	if err != nil {
		return r, fmt.Errorf("resolve router pods: %w", err)
	}
	// The first pod (by name) anchors everything single-valued: the cAdvisor
	// window clock, the Envoy/sidecar scrapes, the port-range read. With
	// replicas those instruments see 1/N of the traffic; the generator-side
	// series sees all of it.
	router := routers[0]
	r.router = router
	r.routers = routers
	r.placement[routercap.RoleEnvoy] = router.Node
	for _, p := range routers {
		log.Info("router pod", "pod", p.Name, "ip", p.IP, "node", p.Node)
	}

	r.portRange, err = parsePortRange(cfg.portRange)
	if err != nil {
		return r, err
	}

	targets, nodes, err := resolveTargets(ctx, cs, cfg, routers, r.placement)
	if err != nil {
		return r, err
	}
	r.targets = targets

	// The generator's own container is the one target whose absence would
	// disable the guard that matters most, so it is checked here.
	if !hasRole(targets, routercap.RoleLoadgen) {
		return r, fmt.Errorf("cannot identify this pod's own container: set --loadgen-pod/--loadgen-namespace/--loadgen-node or the POD_NAME/POD_NAMESPACE/NODE_NAME downward-API env vars, or the guard that measures the load generator cannot run")
	}

	r.windows = &routercap.WindowDriver{
		Client:       routercap.NewMultiNodeCadvisorClient(cs, nodes),
		Anchor:       router.Key(routercap.RoleEnvoy),
		PollInterval: cfg.pollInterval,
		MaxWait:      cfg.maxWait,
	}

	hc := routercap.NewScrapeHTTPClient()
	r.envoy = routercap.NewEnvoyClient(hc, router)
	r.contention = routercap.NewContentionClient(hc, router)
	r.routerStats = routercap.NewRouterClient(hc, router)

	want := cfg.expectConcurrency
	if want == 0 {
		want = cfg.arm
	}
	// Every replica, not just the anchor: a rollout that half-took would leave
	// one pod on the old thread count, and the blend would be mislabeled.
	for _, p := range routers {
		if err := checkConcurrency(ctx, routercap.NewEnvoyClient(hc, p), want); err != nil {
			return r, fmt.Errorf("pod %s: %w", p.Name, err)
		}
	}

	conn, api, err := glutton.DialControl(cfg.apiEndpoint, false)
	if err != nil {
		return r, fmt.Errorf("dial ateapi: %w", err)
	}
	r.closers = append(r.closers, conn.Close)

	r.pool = &routercap.ActorPool{
		API:         api,
		Atespace:    cfg.atespace,
		Concurrency: cfg.warmConcurrency,
		Log:         log,
	}
	if err := r.pool.Warm(ctx, cfg.actors); err != nil {
		return r, err
	}

	urls := make([]string, len(routers))
	for i, p := range routers {
		urls[i] = fmt.Sprintf("http://%s:8080", p.IP)
	}
	r.sender, err = routercap.NewSender(routercap.SenderConfig{
		RouterURLs: urls,
		Actors:     r.pool.Actors(),
		// The idle pool must hold the run's peak concurrency, or Go's idle
		// eviction churns connections at exactly the worst load.
		MaxConnections: int(cfg.maxInFlight),
		RequestTimeout: cfg.requestTimeout,
	})
	if err != nil {
		return r, err
	}
	return r, nil
}

// resolveTargets lists every container the sampler watches and every node whose
// cAdvisor has to be scraped to see them.
func resolveTargets(ctx context.Context, cs kubernetes.Interface, cfg *config, routers []routercap.PodRef, placement map[string]string) ([]routercap.Target, []string, error) {
	var targets []routercap.Target
	var nodes []string

	// Every replica's envoy and sidecar are watched, so a multi-replica run's
	// CPU and memory series sum the whole tier rather than sampling one pod of
	// it. Container keys carry the pod name, so same-named containers across
	// replicas stay distinct.
	for _, router := range routers {
		nodes = append(nodes, router.Node)
		for _, c := range router.Containers {
			role := c
			if c != routercap.RoleEnvoy && c != routercap.RoleSidecar {
				continue
			}
			targets = append(targets, routercap.Target{Role: role, Key: router.Key(c)})
		}
	}

	if cfg.loadgenPod != "" && cfg.loadgenNS != "" && cfg.loadgenNode != "" {
		targets = append(targets, routercap.Target{
			Role: routercap.RoleLoadgen,
			Key: routercap.ContainerKey{
				Namespace: cfg.loadgenNS, Pod: cfg.loadgenPod, Container: cfg.loadgenContainer,
			},
		})
		nodes = append(nodes, cfg.loadgenNode)
		placement[routercap.RoleLoadgen] = cfg.loadgenNode
	}

	// Every other pod in the router's namespace is the control plane, listed
	// rather than named so new ate-system pods stay covered. A throttled
	// ate-api-server is indistinguishable from a slow router at the client.
	cp, err := routercap.FindPods(ctx, cs, cfg.routerNamespace, "")
	if err != nil {
		return nil, nil, fmt.Errorf("list control-plane pods: %w", err)
	}
	isRouter := map[string]bool{}
	for _, router := range routers {
		isRouter[router.Name] = true
	}
	for _, p := range cp {
		if isRouter[p.Name] {
			continue
		}
		for _, c := range p.Containers {
			targets = append(targets, routercap.Target{Role: routercap.RoleControlPlane, Key: p.Key(c)})
		}
		nodes = append(nodes, p.Node)
		notePlacement(placement, routercap.RoleControlPlane, p.Node)
	}

	workers, err := routercap.FindPods(ctx, cs, cfg.workerNamespace, cfg.workerSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("list worker pods: %w", err)
	}
	for _, p := range workers {
		for _, c := range p.Containers {
			targets = append(targets, routercap.Target{Role: routercap.RoleWorker, Key: p.Key(c)})
		}
		nodes = append(nodes, p.Node)
		notePlacement(placement, routercap.RoleWorker, p.Node)
	}
	// Counted, not configured. The per-worker connection-rate guard divides a
	// cluster-wide rate by this number, so a flag that disagreed with the
	// cluster would move the threshold without anyone noticing.
	if len(workers) > 0 {
		cfg.guards.WorkerPods = len(workers)
	}

	return targets, nodes, nil
}

// notePlacement records the nodes a role landed on — what was observed, not
// what was intended. A run where the generator shared the router's node is a
// different experiment.
func notePlacement(placement map[string]string, role, node string) {
	if node == "" {
		return
	}
	cur := placement[role]
	if cur == "" {
		placement[role] = node
		return
	}
	for _, n := range strings.Split(cur, ",") {
		if n == node {
			return
		}
	}
	placement[role] = cur + "," + node
}

func hasRole(targets []routercap.Target, role string) bool {
	for _, t := range targets {
		if t.Role == role {
			return true
		}
	}
	return false
}

// checkConcurrency confirms Envoy's worker-thread count matches what the run
// expects — the arm, unless -expect-concurrency decoupled them. Left unset,
// Envoy sizes threads from the *node's* CPU count, so a 10-core arm on a
// 176-core node would run 176 event loops and measure CFS throttling.
func checkConcurrency(ctx context.Context, c *routercap.EnvoyClient, want int) error {
	s, err := c.Scrape(ctx)
	if err != nil {
		return fmt.Errorf("scrape envoy admin: %w", err)
	}
	if want <= 0 {
		return nil
	}
	if int(s.Concurrency) != want {
		return fmt.Errorf("envoy_server_concurrency is %g but %d was expected: the --concurrency patch did not take, and this arm would be labeled with a thread count it is not running",
			s.Concurrency, want)
	}
	return nil
}

// parsePortRange reads the router pod's measured ephemeral range. Measured
// rather than assumed because every claim about the port wall is a claim about
// these two numbers.
func parsePortRange(s string) (routercap.PortRange, error) {
	if strings.TrimSpace(s) == "" {
		return routercap.DefaultPortRange(), nil
	}
	// Accepts both the sysctl's own tab-separated form and "low-high".
	f := strings.FieldsFunc(s, func(r rune) bool {
		return r == '-' || r == ' ' || r == '\t' || r == ','
	})
	if len(f) != 2 {
		return routercap.PortRange{}, fmt.Errorf("--port-range %q: want two numbers, e.g. 32768-60999", s)
	}
	low, err := strconv.Atoi(f[0])
	if err != nil {
		return routercap.PortRange{}, fmt.Errorf("--port-range %q: %w", s, err)
	}
	high, err := strconv.Atoi(f[1])
	if err != nil {
		return routercap.PortRange{}, fmt.Errorf("--port-range %q: %w", s, err)
	}
	p := routercap.PortRange{Low: low, High: high, Source: routercap.PortRangeMeasured}
	if p.Size() <= 0 {
		return routercap.PortRange{}, fmt.Errorf("--port-range %q describes no ports", s)
	}
	return p, nil
}
