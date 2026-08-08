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

// The guards that decide whether a window measured the router or measured the rig,
// and which of those failures are bad enough to abort the run.

package routercap

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

// Each guard watches one way the harness can become the bottleneck, so a
// capacity figure is never really a statement about the load generator.
// Envoy's port-exhaustion counters are deliberately absent: that cliff is what
// the run came to measure.

// ClientStats is the generator's view of its own transport over one window.
type ClientStats struct {
	// NewConnections is TCP connections the generator opened to the router.
	// With keep-alive intact this stays near zero after the pool warms.
	NewConnections float64 `json:"new_connections"`
	// RequestsPerConnection is requests dispatched per new connection over the
	// window. Near 1 means a socket per request, which hits the generator's own
	// ephemeral-port ceiling long before the router hits anything.
	RequestsPerConnection float64 `json:"requests_per_connection"`
	// ConnectionsInUse is the pool's live connection count.
	ConnectionsInUse int64 `json:"connections_in_use"`
}

// GuardName identifies a rig guard.
type GuardName string

// One guard per way the harness can become the bottleneck. A fatal trip marks
// the arm rig-limited from that rung on: the windows before it stand, the
// windows after it describe the harness.
const (
	// GuardLoadgenCPU: the generator container ran out of its own CPU, so the
	// reported offered rate is no longer real.
	GuardLoadgenCPU GuardName = "loadgen_cpu"
	// GuardControlPlaneThrottle: ate-api-server (or anything else in the
	// router's namespace) was CFS-throttled. Every request resumes through the
	// control plane, so this is indistinguishable from a slow router.
	GuardControlPlaneThrottle GuardName = "control_plane_throttle"
	// GuardWorkerConnRate: new connections per worker pod exceeded what a
	// worker's 60s-TIME_WAIT source-port pool can absorb (~470/s; the guard
	// trips at 400), so the wall belongs to the workers, not the router.
	GuardWorkerConnRate GuardName = "worker_conn_rate"
	// GuardClientKeepAlive: requests per new connection fell below the floor,
	// meaning keep-alive stopped holding and the generator is in a dial storm,
	// racing its own port ceiling.
	GuardClientKeepAlive GuardName = "client_keepalive"
	// GuardClientPorts: the generator's live connection count neared its own
	// source-port budget (80% of its measured ip_local_port_range — see
	// ResolveClientCeiling). The next cliff would be the rig's, not the
	// router's.
	GuardClientPorts GuardName = "client_ports"
	// GuardDispatchLag: the pacer fell behind its own schedule, so the offered
	// rate on the x-axis was not the rate actually offered. Suspended while the
	// system under test is demonstrably saturated.
	GuardDispatchLag GuardName = "dispatch_lag"
)

// defaultEphemeralPorts is the size of the Linux default source-port range
// (32768-60999). Only a fallback for the generator's ceiling; the router's
// range is always read from the live pod.
const defaultEphemeralPorts = 60999 - 32768 + 1

// GuardTrip is one guard firing on one window.
type GuardTrip struct {
	Guard     GuardName `json:"guard"`
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	// Fatal says whether the run should stop. A trip can be recorded without
	// being fatal — see the saturation exception on dispatch lag.
	Fatal  bool   `json:"fatal"`
	Detail string `json:"detail"`
}

// GuardConfig holds the thresholds, each settable by a flag on the binary. It
// is serialized into the run header, because a loosened threshold changes what
// the run's silence means.
type GuardConfig struct {
	// LoadgenCPUUtilization is the generator container's use against its own
	// limit. Past this the offered rate stops being real.
	LoadgenCPUUtilization float64 `json:"loadgen_cpu_utilization"`
	// WorkerNewConnsPerSec is the per-worker-pod new-connection rate ceiling.
	// A worker pod's source-port pool recycles on a 60s TIME_WAIT, giving
	// ~470/s; the default sits under it.
	WorkerNewConnsPerSec float64 `json:"worker_new_conns_per_sec"`
	// MinRequestsPerConnection is the generator-side keep-alive check.
	MinRequestsPerConnection float64 `json:"min_requests_per_connection"`
	// ClientConnectionCeiling is how many simultaneous connections the
	// generator may hold to the router before its own source ports bind first;
	// zero disables the check. The default is 80% of the generator's measured
	// ip_local_port_range (ResolveClientCeiling), with the Linux-default
	// assumption standing only until that read happens or when it fails.
	ClientConnectionCeiling int `json:"client_connection_ceiling"`
	// DispatchLagP95Ms is how far behind its own schedule the generator may
	// fall before the offered rate is fiction. This and SaturationLatencyP95Ms
	// key on p95 because the run only keeps p50 and p95 (see LatencyStats),
	// while the thresholds kept their old p99-era values — deliberately harder
	// to trip.
	DispatchLagP95Ms float64 `json:"dispatch_lag_p95_ms"`

	// SaturationLatencyP95Ms and SaturationAchievedRatio define "the system is
	// demonstrably saturated", which suspends the dispatch-lag guard.
	SaturationLatencyP95Ms  float64 `json:"saturation_latency_p95_ms"`
	SaturationAchievedRatio float64 `json:"saturation_achieved_ratio"`

	// WorkerPods is how many worker pods the load is spread across, used to
	// turn the cluster-wide connection rate into a per-pod one.
	WorkerPods int `json:"worker_pods"`
}

// DefaultGuardConfig returns the thresholds argued for in the design.
func DefaultGuardConfig() GuardConfig {
	return GuardConfig{
		LoadgenCPUUtilization:    0.80,
		WorkerNewConnsPerSec:     400,
		MinRequestsPerConnection: 10,
		ClientConnectionCeiling:  defaultEphemeralPorts * 8 / 10,
		DispatchLagP95Ms:         50,
		SaturationLatencyP95Ms:   200,
		SaturationAchievedRatio:  0.95,
	}
}

// ResolveClientCeiling replaces a default client-connection ceiling with 80%
// of this process's own measured source-port range, read once at startup from
// inside the generator pod so /proc answers for the right network namespace.
// A ceiling that is neither zero (disabled) nor the default is a flag override
// and is left alone; a failed read keeps the conservative default.
func (g *GuardConfig) ResolveClientCeiling(readFile func(string) ([]byte, error), log *slog.Logger) {
	if g.ClientConnectionCeiling != defaultEphemeralPorts*8/10 {
		return
	}
	b, err := readFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		log.Warn("could not read own ip_local_port_range; the client-connection ceiling keeps the Linux-default assumption", "err", err)
		return
	}
	f := strings.Fields(string(b))
	if len(f) != 2 {
		log.Warn("unparseable ip_local_port_range; keeping the default ceiling", "content", string(b))
		return
	}
	lo, err1 := strconv.Atoi(f[0])
	hi, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil || hi < lo {
		log.Warn("unparseable ip_local_port_range; keeping the default ceiling", "content", string(b))
		return
	}
	g.ClientConnectionCeiling = (hi - lo + 1) * 8 / 10
	log.Info("client-connection ceiling from this pod's own port range",
		"range", fmt.Sprintf("%d-%d", lo, hi), "ceiling", g.ClientConnectionCeiling)
}

// saturated reports whether the system is visibly failing to keep up, in which
// case the generator being slow is a symptom rather than a cause. Without this
// exception the dispatch-lag guard would abort the ladder at exactly the
// saturated rungs the run exists to measure.
func (g GuardConfig) saturated(s *Sample) bool {
	if g.SaturationLatencyP95Ms > 0 && s.Load.Latency.P95Ms >= g.SaturationLatencyP95Ms {
		return true
	}
	if g.SaturationAchievedRatio > 0 && s.Load.OfferedQPS > 0 &&
		s.Load.AchievedQPS < g.SaturationAchievedRatio*s.Load.OfferedQPS {
		return true
	}
	return false
}

// Check evaluates every guard against one window and returns the trips. It
// never mutates the sample; the caller decides what a fatal trip means.
func (g GuardConfig) Check(s *Sample) []GuardTrip {
	var trips []GuardTrip

	if lg, ok := s.Containers[RoleLoadgen]; ok && g.LoadgenCPUUtilization > 0 {
		if lg.CPULimitCores <= 0 {
			// An unlimited generator container cannot be checked against a
			// utilization threshold, and skipping silently would leave the
			// run's most important guard quietly disabled.
			trips = append(trips, GuardTrip{
				Guard: GuardLoadgenCPU, Value: lg.CPUCores, Fatal: true,
				Detail: "loadgen container has no CPU limit, so its headroom cannot be checked; give it requests == limits",
			})
		} else if lg.CPUUtilization > g.LoadgenCPUUtilization {
			trips = append(trips, GuardTrip{
				Guard: GuardLoadgenCPU, Value: lg.CPUUtilization, Threshold: g.LoadgenCPUUtilization, Fatal: true,
				Detail: fmt.Sprintf("load generator at %.0f%% of its %.1f-core limit; the offered rate is no longer reliable",
					lg.CPUUtilization*100, lg.CPULimitCores),
			})
		}
	}

	if cp, ok := s.Groups[RoleControlPlane]; ok && cp.ThrottledPeriods > 0 {
		// Any throttling at all, because a throttled ate-api-server is
		// indistinguishable from a slow router when seen from the client.
		trips = append(trips, GuardTrip{
			Guard: GuardControlPlaneThrottle, Value: cp.ThrottledPeriods, Threshold: 0, Fatal: true,
			Detail: fmt.Sprintf("%s throttled for %.0f periods (%.3fs), worst container %q; router latency would be partly the control plane's",
				RoleControlPlane, cp.ThrottledPeriods, cp.ThrottledSeconds, cp.ThrottledMaxOf),
		})
	}

	if s.Envoy != nil && g.WorkerPods > 0 && g.WorkerNewConnsPerSec > 0 {
		if actor, ok := s.Envoy.Clusters[ActorClusterName]; ok {
			// Cluster-wide mean divided by pod count, a fair stand-in for the
			// per-pod maximum only because the run places exactly one actor per
			// worker pod and dispatches uniformly. If either changes, so must
			// this.
			perPod := actor.NewConnectionsPerSec / float64(g.WorkerPods)
			if perPod > g.WorkerNewConnsPerSec {
				trips = append(trips, GuardTrip{
					Guard: GuardWorkerConnRate, Value: perPod, Threshold: g.WorkerNewConnsPerSec, Fatal: true,
					Detail: fmt.Sprintf("mean %.0f new connections/sec per worker pod across %d pods; a pod's source ports recycle on a 60s TIME_WAIT",
						perPod, g.WorkerPods),
				})
			}
		}
	}

	// Skipped when the generator opened nothing (perfect reuse divides by
	// zero) and when the window carried no scheduled load (the pre-warm window
	// races the ladder start). The ratio is only meaningful over traffic the
	// ladder asked for.
	if g.MinRequestsPerConnection > 0 && s.Load.OfferedQPS > 0 && s.Client.NewConnections > 0 &&
		s.Client.RequestsPerConnection < g.MinRequestsPerConnection {
		trips = append(trips, GuardTrip{
			Guard: GuardClientKeepAlive, Value: s.Client.RequestsPerConnection, Threshold: g.MinRequestsPerConnection, Fatal: true,
			Detail: fmt.Sprintf("generator averaged %.1f requests per connection over %.0f new connections; keep-alive to the router is not holding, so the client will hit its own port ceiling first",
				s.Client.RequestsPerConnection, s.Client.NewConnections),
		})
	}

	if g.ClientConnectionCeiling > 0 && s.Client.ConnectionsInUse > int64(g.ClientConnectionCeiling) {
		trips = append(trips, GuardTrip{
			Guard: GuardClientPorts, Value: float64(s.Client.ConnectionsInUse), Threshold: float64(g.ClientConnectionCeiling), Fatal: true,
			Detail: fmt.Sprintf("generator holds %d connections to the router, past its own source-port headroom; the next cliff would be the load generator's, not the router's",
				s.Client.ConnectionsInUse),
		})
	}

	if g.DispatchLagP95Ms > 0 && s.Load.DispatchLag.P95Ms > g.DispatchLagP95Ms {
		sat := g.saturated(s)
		detail := fmt.Sprintf("generator was %.1fms behind its own schedule at p95; offered load for this window is not what the x-axis says",
			s.Load.DispatchLag.P95Ms)
		if sat {
			detail = fmt.Sprintf("generator was %.1fms behind its own schedule at p95, but the system is saturated (p95 latency %.1fms, achieved %.0f of %.0f offered QPS): recorded, not fatal",
				s.Load.DispatchLag.P95Ms, s.Load.Latency.P95Ms, s.Load.AchievedQPS, s.Load.OfferedQPS)
		}
		trips = append(trips, GuardTrip{
			Guard: GuardDispatchLag, Value: s.Load.DispatchLag.P95Ms, Threshold: g.DispatchLagP95Ms,
			Fatal: !sat, Detail: detail,
		})
	}

	return trips
}

// AnyFatal reports whether any trip should stop the run.
func AnyFatal(trips []GuardTrip) bool { return len(FatalTrips(trips)) > 0 }

// FatalTrips returns only the trips that should stop the run.
func FatalTrips(trips []GuardTrip) []GuardTrip {
	var out []GuardTrip
	for _, t := range trips {
		if t.Fatal {
			out = append(out, t)
		}
	}
	return out
}

// RigLimitedError is returned when a guard stops the run. It maps to exit code
// 3, distinct from a system failure, so automation can tell "we could not
// measure this" from "the router fell over".
type RigLimitedError struct {
	Trips []GuardTrip
}

func (e *RigLimitedError) Error() string {
	if len(e.Trips) == 0 {
		return "rig-limited"
	}
	msg := "rig-limited: "
	for i, t := range e.Trips {
		if !t.Fatal {
			continue
		}
		if i > 0 {
			msg += "; "
		}
		msg += string(t.Guard) + ": " + t.Detail
	}
	return msg
}
