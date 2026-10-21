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

package load

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/procstats"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/report"
)

// sampleInterval is how often atunnel is polled for peaks during a rung.
const sampleInterval = 500 * time.Millisecond

// Runner runs scenarios against a running atunnel and sink.
type Runner struct {
	Targets Targets
	// TunnelAdmin and SinkAdmin are the processes' admin base URLs.
	TunnelAdmin string
	SinkAdmin   string
	Step        time.Duration
	Warmup      time.Duration
	// Workers caps units in flight.
	Workers int
	// ProfileDir, when set, receives a CPU and a heap profile of atunnel per
	// rung.
	ProfileDir string
	// SharedDir is shared with the worker container. It holds the Unix
	// sockets of the egress actor, which runs egress scenarios inside an
	// actor's sandbox, and of the ingress actors, which serve their counters.
	SharedDir string

	tunnel, sink *admin
}

// Prepare waits for both processes, learns the hosted actors and the actor
// port from atunnel, and sends a little traffic down every path so that a
// misconfiguration fails here rather than as a rung full of errors.
func (r *Runner) Prepare(ctx context.Context) error {
	r.tunnel, r.sink = newAdmin(r.TunnelAdmin), newAdmin(r.SinkAdmin)
	readyCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for _, a := range []*admin{r.tunnel, r.sink} {
		if err := a.waitReady(readyCtx); err != nil {
			return err
		}
	}
	st, err := r.tunnel.stats(ctx)
	if err != nil {
		return err
	}
	if st.Tunnel == nil || len(st.Tunnel.Sandboxes) == 0 {
		return fmt.Errorf("%s/stats does not describe a tunnel with hosted actors; is it atunnel-bench tunnel?", r.TunnelAdmin)
	}
	r.Targets.Actors = nil
	for _, sb := range st.Tunnel.Sandboxes {
		r.Targets.Actors = append(r.Targets.Actors, sb.Actor)
	}
	r.Targets.ActorPort = st.Tunnel.ActorPort
	// The worker places the egress actor and the ingress actors in the sandboxes
	// once atunnel hosts them, so they come up a moment after it.
	if err := r.waitForSandboxSockets(readyCtx, len(st.Tunnel.Sandboxes)); err != nil {
		return err
	}

	// The stream check goes first so its bytes have drained through the
	// gateway before the first scenario reads the sink's counters.
	for _, check := range []options{
		{mode: EgressStream, connections: 1, payloadBytes: 1024},
		{mode: IngressH1, rate: 20},
		{mode: IngressH2, rate: 20},
		{mode: Connect, rate: 20},
		{mode: EgressHTTP, rate: 20},
	} {
		check.duration, check.workers = 300*time.Millisecond, 4
		l, err := r.execute(ctx, check)
		if err != nil {
			return err
		}
		if l.Errors > 0 || l.Completed == 0 {
			return fmt.Errorf("check of %s failed: %d completed, %d errors: %s",
				check.mode, l.Completed, l.Errors, strings.Join(l.ErrorSamples, "; "))
		}
	}
	return nil
}

// waitForSandboxSockets waits until the egress actor and every ingress actor
// answer on their sockets in the shared directory.
func (r *Runner) waitForSandboxSockets(ctx context.Context, ingressActors int) error {
	if r.SharedDir == "" {
		return nil
	}
	sockets := []string{filepath.Join(r.SharedDir, "egress-actor.sock")}
	for i := range ingressActors {
		sockets = append(sockets, filepath.Join(r.SharedDir, "ingress-actor-"+strconv.Itoa(i)+".sock"))
	}
	for _, socket := range sockets {
		for {
			conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
			if err == nil {
				_ = conn.Close()
				break
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for %s: %w", socket, err)
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return nil
}

// Run runs s at each rung and reports it. A rung where atunnel stops
// answering ends the scenario; Run then waits for atunnel to come back, as a
// replaced atunnel does, and fails, with the report so far, if it does
// not.
func (r *Runner) Run(ctx context.Context, s Scenario, rungs []float64) (*report.Report, error) {
	rep := &report.Report{Scenario: s.Name, Description: s.Description, Unit: s.Unit, Step: r.Step, Warmup: r.Warmup}
	if s.holdsConnections {
		rep.Warmup = 0
	}
	if s.streams > 0 {
		// Bandwidth has no rate to offer; one rung is the measurement.
		rungs = rungs[:1]
	}
	before, err := r.sinkStats(ctx)
	if err != nil {
		return nil, err
	}
	for _, target := range rungs {
		rung, err := r.rung(ctx, s, target)
		if err != nil {
			return nil, fmt.Errorf("%s at %.0f: %w", s.Name, target, err)
		}
		rep.Rungs = append(rep.Rungs, rung)
		if rung.Verdict == report.VerdictDown {
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			err := r.tunnel.waitReady(waitCtx)
			if err == nil {
				// The replacement's sandboxes get their own egress actor and
				// ingress actors a moment after it is ready.
				err = r.waitForSandboxSockets(waitCtx, len(r.Targets.Actors))
			}
			cancel()
			if err != nil {
				return rep, fmt.Errorf("atunnel did not come back after %s at %.0f: %w", s.Name, target, err)
			}
			break
		}
	}
	after, err := r.settle(ctx)
	if err != nil {
		return nil, err
	}
	rep.SinkCounters = sinkDelta(before, after)
	return rep, nil
}

// settle waits, for up to settleTimeout, until every tunnel the sink saw has
// closed, so bytes still draining through atunnel are counted in this
// scenario rather than the next, and returns the sink's counters.
func (r *Runner) settle(ctx context.Context) (Stats, error) {
	const settleTimeout = 10 * time.Second
	deadline := time.Now().Add(settleTimeout)
	for {
		st, err := r.sinkStats(ctx)
		if err != nil || st.Gateway == nil || st.Gateway.TunnelsOpen == 0 || time.Now().After(deadline) {
			return st, err
		}
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *Runner) rung(ctx context.Context, s Scenario, target float64) (report.Rung, error) {
	opts := options{
		mode:                    s.mode,
		rate:                    target,
		duration:                r.Step,
		warmup:                  r.Warmup,
		workers:                 r.Workers,
		newConnectionPerRequest: s.newConnectionPerRequest,
	}
	var live Stats
	var liveErr error
	switch {
	case s.holdsConnections:
		// Open at a steady rate, stretching the rung if the count needs
		// it, then hold for at least half a step. Every open is measured.
		opts.connections = int(target)
		opts.rate = idleOpenRate
		opts.warmup = 0
		opts.duration = max(r.Step, time.Duration(target/idleOpenRate*float64(time.Second))+r.Step/2)
		opts.whileHolding = func(ctx context.Context) { live, liveErr = r.tunnel.liveStats(ctx) }
	case s.streams > 0:
		opts.connections = s.streams
		opts.payloadBytes = 64 << 10
		opts.rate = 0
	}

	type result struct {
		load report.Load
		err  error
	}
	// The load stops early if atunnel dies: against a replacement it would
	// only measure, or kill, that one.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	done := make(chan result, 1)
	go func() {
		l, err := r.execute(loadCtx, opts)
		done <- result{l, err}
	}()

	// The window opens when warmup ends.
	select {
	case <-ctx.Done():
		return report.Rung{}, ctx.Err()
	case <-time.After(opts.warmup):
	}
	tunnelWatch := startWatch(ctx, r.processStats(r.tunnel), sampleInterval, stopLoad)
	sinkWatch := startWatch(ctx, r.sinkProcessStats, sampleInterval, nil)
	generator := func(context.Context) (procstats.Snapshot, error) { return procstats.Read(), nil }
	if isEgress(opts.mode) {
		// Egress is generated by the egress actor in the sandbox.
		generator = r.processStats(unixAdmin(filepath.Join(r.SharedDir, "egress-actor.sock")))
	}
	selfWatch := startWatch(ctx, generator, sampleInterval, nil)

	var profiles []string
	profileDone := make(chan error, 1)
	if r.ProfileDir != "" {
		seconds := max(1, int((opts.duration - opts.warmup).Seconds()))
		path := filepath.Join(r.ProfileDir, profileName(s, target, "cpu"))
		profiles = append(profiles, path)
		go func() {
			profileDone <- r.tunnel.fetch(ctx, "/debug/pprof/profile?seconds="+strconv.Itoa(seconds), path)
		}()
	} else {
		profileDone <- nil
	}

	res := <-done
	tunnel, tunnelErr := tunnelWatch.finish(ctx, res.load.Completed)
	if res.err != nil {
		if ctx.Err() != nil || tunnelErr == nil {
			return report.Rung{}, res.err
		}
		// The load failed because atunnel died: stopped by the watch, or cut
		// off when the worker replaced the egress actor with atunnel.
		res.load = report.Load{Offered: opts.rate}
	}
	rung := report.Rung{Load: res.load}
	if s.holdsConnections {
		// The rung offers connections, and every open is a unit.
		rung.Load.Offered = target
		rung.Load.Achieved = float64(res.load.Completed)
	}
	sinkRes, sinkErr := sinkWatch.finish(ctx, res.load.Completed)
	generatorRes, generatorErr := selfWatch.finish(ctx, res.load.Completed)
	// When atunnel dies, the worker replaces the egress actor and ingress
	// actors with it, so their readings fail too; only atunnel's failure is
	// the result.
	if tunnelErr == nil {
		if sinkErr != nil {
			return report.Rung{}, fmt.Errorf("reading sink stats: %w", sinkErr)
		}
		if generatorErr != nil {
			return report.Rung{}, fmt.Errorf("reading generator stats: %w", generatorErr)
		}
	}
	rung.Sink, rung.Generator = sinkRes, generatorRes
	profileErr := <-profileDone
	if s.holdsConnections && tunnelErr == nil {
		tunnelErr = liveErr
	}
	if tunnelErr == nil {
		tunnelErr = profileErr
	}
	if tunnelErr != nil {
		// atunnel died or hung during the rung, as when it is killed for
		// going over its memory limit. That is a result. The ingress actors and
		// egress actor were replaced with it, so their readings are dropped too.
		rung.Sink, rung.Generator = report.Resources{}, report.Resources{}
		rung.Verdict = report.VerdictDown
		rung.Reason = "atunnel stopped answering: " + tunnelErr.Error()
		return rung, nil
	}
	rung.Tunnel = tunnel
	if s.holdsConnections {
		rung.Tunnel.HeapLiveBytes = live.Process.HeapInUseBytes
		rung.Tunnel.GoroutinesLive = live.Process.Goroutines
	}
	if r.ProfileDir != "" {
		path := filepath.Join(r.ProfileDir, profileName(s, target, "heap"))
		if err := r.tunnel.fetch(ctx, "/debug/pprof/heap", path); err != nil {
			return report.Rung{}, fmt.Errorf("fetching heap profile: %w", err)
		}
		profiles = append(profiles, path)
	}
	rung.Profiles = profiles
	rung.Verdict, rung.Reason = verdict(s, rung)
	return rung, nil
}

// execute runs opts here, or on the egress actor for egress modes.
func (r *Runner) execute(ctx context.Context, opts options) (report.Load, error) {
	if isEgress(opts.mode) {
		if r.SharedDir == "" {
			return report.Load{}, fmt.Errorf("%s needs the egress actor in the shared directory", opts.mode)
		}
		return executeOnEgressActor(ctx, filepath.Join(r.SharedDir, "egress-actor.sock"), opts)
	}
	return execute(ctx, r.Targets, opts)
}

func (r *Runner) processStats(a *admin) func(context.Context) (procstats.Snapshot, error) {
	return func(ctx context.Context) (procstats.Snapshot, error) {
		st, err := a.stats(ctx)
		return st.Process, err
	}
}

// sinkProcessStats is the CPU and memory of everything at the far end of
// atunnel: the fake gateway and the ingress actors, summed.
func (r *Runner) sinkProcessStats(ctx context.Context) (procstats.Snapshot, error) {
	st, err := r.sink.stats(ctx)
	if err != nil || r.SharedDir == "" {
		return st.Process, err
	}
	sum := st.Process
	sockets, err := filepath.Glob(filepath.Join(r.SharedDir, "ingress-actor-*.sock"))
	if err != nil {
		return sum, err
	}
	for _, socket := range sockets {
		w, err := unixAdmin(socket).stats(ctx)
		if err != nil {
			continue
		}
		sum.CPUSeconds += w.Process.CPUSeconds
		sum.RSSBytes += w.Process.RSSBytes
		sum.HeapInUseBytes += w.Process.HeapInUseBytes
		sum.Goroutines += w.Process.Goroutines
	}
	return sum, nil
}

func profileName(s Scenario, target float64, kind string) string {
	return fmt.Sprintf("%s-%.0f.%s.pb.gz", s.Name, target, kind)
}

// Shares of a process's CPU past which it counts as saturated: atunnel, and
// the generator or sink, which then set the pace instead of atunnel.
const (
	tunnelSaturated = 0.95
	rigSaturated    = 0.85
)

// verdict classifies a rung.
func verdict(s Scenario, g report.Rung) (string, string) {
	l := g.Load
	if l.Errors > 0 {
		return report.VerdictErrors, fmt.Sprintf("%d errors: %s", l.Errors, strings.Join(l.ErrorSamples, "; "))
	}
	if limit := cpuLimit(g.Tunnel); g.Tunnel.Cores >= tunnelSaturated*limit {
		return report.VerdictSaturated, fmt.Sprintf("atunnel at %.2f of %g cores", g.Tunnel.Cores, limit)
	}
	for _, p := range []struct {
		name string
		res  report.Resources
	}{{"generator", g.Generator}, {"sink", g.Sink}} {
		if limit := cpuLimit(p.res); p.res.Cores > rigSaturated*limit {
			return report.VerdictRigLimited, fmt.Sprintf("%s at %.1f of %g cores", p.name, p.res.Cores, limit)
		}
	}
	// With the generator and sink idle enough, a backed-up queue means the
	// generator's workers were all waiting on atunnel.
	if l.Skipped > 0 {
		return report.VerdictShortfall, fmt.Sprintf("%d units found every worker busy", l.Skipped)
	}
	if l.SendLag.P99 > 10 {
		return report.VerdictShortfall, fmt.Sprintf("units waited for a worker, p99 %.1fms", l.SendLag.P99)
	}
	if !s.holdsConnections && s.streams == 0 && l.Achieved < 0.97*l.Offered {
		return report.VerdictShortfall, fmt.Sprintf("achieved %.0f of %.0f", l.Achieved, l.Offered)
	}
	return report.VerdictOK, ""
}

// cpuLimit is the cores a process may use: its cgroup's limit when it has
// one, else its GOMAXPROCS.
func cpuLimit(r report.Resources) float64 {
	if r.CgroupCPULimitCore > 0 {
		return r.CgroupCPULimitCore
	}
	return float64(max(1, r.GOMAXPROCS))
}

// sinkDelta is how much the sink's counters moved over a scenario. A counter
// that went down was reset, by a replaced ingress actor, and is left out.
func sinkDelta(before, after Stats) map[string]any {
	out := map[string]any{}
	delta := func(name string, b, a uint64) {
		if a >= b {
			out[name] = a - b
		}
	}
	if before.IngressActor != nil && after.IngressActor != nil {
		delta("ingress_actor_requests", before.IngressActor.Requests, after.IngressActor.Requests)
		delta("ingress_actor_connections", before.IngressActor.ConnectionsSeen, after.IngressActor.ConnectionsSeen)
	}
	if before.Gateway != nil && after.Gateway != nil {
		delta("gateway_connections", before.Gateway.Connections, after.Gateway.Connections)
		delta("gateway_tunnels", before.Gateway.Tunnels, after.Gateway.Tunnels)
		delta("gateway_tunnels_h2", before.Gateway.TunnelsH2, after.Gateway.TunnelsH2)
		delta("gateway_requests", before.Gateway.Requests, after.Gateway.Requests)
		delta("gateway_bytes_in", before.Gateway.BytesIn, after.Gateway.BytesIn)
		delta("gateway_rejected", before.Gateway.Rejected, after.Gateway.Rejected)
	}
	return out
}
