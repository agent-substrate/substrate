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

// The Sample record — one window of load, latency, CPU and memory, aligned — and the
// ephemeral-port budget arithmetic that goes into it.

package routercap

import (
	"sort"
	"time"
)

// Roles name the containers the run watches, so the output is keyed by what a
// container is rather than by a pod name that changes on every arm rollout.
const (
	RoleEnvoy        = "envoy"
	RoleSidecar      = "atenet-router"
	RoleLoadgen      = "loadgen"
	RoleControlPlane = "ate-system"
	RoleWorker       = "worker"
)

// Target is one container the sampler watches and the role it plays.
type Target struct {
	Role string
	Key  ContainerKey
}

// GroupUsage aggregates a role that has many containers — the control plane and
// the worker pods. Sums for the resources, maxima for throttling: averaging a
// single throttled container across a dozen idle ones would hide it.
type GroupUsage struct {
	Containers int     `json:"containers"`
	CPUCores   float64 `json:"cpu_cores"`
	// CPUUtilizationMax is the worst single container's use against its own
	// limit. The sum would be meaningless across differently-sized containers.
	CPUUtilizationMax     float64 `json:"cpu_utilization_max"`
	CPUUtilizationMaxOf   string  `json:"cpu_utilization_max_of,omitempty"`
	MemoryWorkingSetBytes float64 `json:"memory_working_set_bytes"`
	ThrottledPeriods      float64 `json:"throttled_periods"`
	ThrottledSeconds      float64 `json:"throttled_seconds"`
	ThrottledFractionMax  float64 `json:"throttled_fraction_max"`
	ThrottledMaxOf        string  `json:"throttled_max_of,omitempty"`
}

func aggregate(us []ContainerUsage) GroupUsage {
	g := GroupUsage{Containers: len(us)}
	for _, u := range us {
		g.CPUCores += u.CPUCores
		g.MemoryWorkingSetBytes += u.MemoryWorkingSetBytes
		g.ThrottledPeriods += u.ThrottledPeriods
		g.ThrottledSeconds += u.ThrottledSeconds
		if u.CPUUtilization > g.CPUUtilizationMax {
			g.CPUUtilizationMax, g.CPUUtilizationMaxOf = u.CPUUtilization, u.Container
		}
		if u.ThrottledFraction > g.ThrottledFractionMax {
			g.ThrottledFractionMax, g.ThrottledMaxOf = u.ThrottledFraction, u.Container
		}
	}
	return g
}

// PortBudget is the ephemeral-port picture for the router pod. The range is
// read out of the live pod at setup, not assumed to be the Linux default.
type PortBudget struct {
	RangeLow  int `json:"range_low"`
	RangeHigh int `json:"range_high"`
	// Available is the size of the range: the hard ceiling on simultaneously
	// held source ports for a given destination-less estimate.
	Available int `json:"available"`

	// ActiveConnections is Envoy's upstream_cx_active on the actor cluster.
	// The upstream hop is HTTP/1.1, so this is also the count of source ports
	// currently bound.
	ActiveConnections float64 `json:"active_connections"`
	// TimeWaitEstimate is new connections over the last minute, the TIME_WAIT
	// linger for a closed port. Estimated from the window's connection rate
	// because nothing in the pod exports a TIME_WAIT count.
	TimeWaitEstimate float64 `json:"time_wait_estimate"`
	EstimatedInUse   float64 `json:"estimated_in_use"`
	Headroom         float64 `json:"headroom"`
	Utilization      float64 `json:"utilization"`

	// CircuitBreakerLimit is the configured concurrency cap, deliberately below
	// Available so Envoy's counted overflow trips before the kernel's opaque
	// EADDRNOTAVAIL. Carrying both lets a reader confirm that ordering held.
	CircuitBreakerLimit int `json:"circuit_breaker_limit"`
}

// timeWaitSeconds is the kernel's fixed 2*MSL linger for a closed connection.
// Not tunable without a kernel rebuild, so it is a constant rather than a knob.
const timeWaitSeconds = 60

func portBudget(rangeLow, rangeHigh, breakerLimit int, cxActive, newConnsPerSec float64) PortBudget {
	p := PortBudget{
		RangeLow:            rangeLow,
		RangeHigh:           rangeHigh,
		ActiveConnections:   cxActive,
		TimeWaitEstimate:    newConnsPerSec * timeWaitSeconds,
		CircuitBreakerLimit: breakerLimit,
	}
	if rangeHigh >= rangeLow && rangeLow > 0 {
		p.Available = rangeHigh - rangeLow + 1
	}
	p.EstimatedInUse = p.ActiveConnections + p.TimeWaitEstimate
	if p.Available > 0 {
		p.Headroom = float64(p.Available) - p.EstimatedInUse
		p.Utilization = p.EstimatedInUse / float64(p.Available)
	}
	return p
}

// Sample is one line of samples.jsonl: everything true of one interval, from
// every source, over the same [T0, T1). AlignmentSpreadMs is the largest
// disagreement between any contributing container's own cAdvisor interval and
// that pair, so the alignment claim can be checked against the data.
type Sample struct {
	Arm  int `json:"arm_cores"`
	Pass int `json:"pass"`
	Rung int `json:"rung"`
	// RungQPS is the rung's nominal rate. Load.OfferedQPS is what the schedule
	// actually asked for over this window, which differs at a rung boundary.
	RungQPS float64 `json:"rung_qps"`
	// Warmup marks a window inside a rung's discarded head, kept in the file
	// so a chart can show the settling and an analysis can exclude it.
	Warmup bool `json:"warmup"`

	// T is the interval midpoint: the x value when a chart must draw an
	// interval as a point. T0 and T1 are carried so it need not.
	T             time.Time `json:"t"`
	T0            time.Time `json:"t0"`
	T1            time.Time `json:"t1"`
	WindowSeconds float64   `json:"window_seconds"`
	// WindowPolls is how many cAdvisor fetches the window took. One means the
	// kubelet moved faster than the poll interval, so the resolution here is
	// poll-limited rather than kubelet-limited.
	WindowPolls       int     `json:"window_polls"`
	AlignmentSpreadMs float64 `json:"alignment_spread_ms"`

	Load GenStats `json:"load"`
	// Client is the generator measuring its own transport. A generator
	// churning connections is heading for its own port wall, and that cliff
	// would be the rig's rather than the router's.
	Client ClientStats `json:"client"`

	// Containers holds the single-container roles, keyed by role.
	Containers map[string]ContainerUsage `json:"containers"`
	// Groups holds the many-container roles, keyed by role.
	Groups map[string]GroupUsage `json:"groups,omitempty"`

	Envoy  *EnvoyDelta  `json:"envoy,omitempty"`
	Router *RouterDelta `json:"router,omitempty"`
	Ports  PortBudget   `json:"ports"`
	// Spans divides the mean request across the hops of the request path. Nil
	// when Envoy reported no request-time samples for the window.
	Spans *LatencySpans `json:"spans,omitempty"`

	Guards []GuardTrip `json:"guards,omitempty"`
	// Missing names containers cAdvisor did not report this window. Present so
	// "the router used no CPU" and "we could not see the router" never look the
	// same in the output.
	Missing []string `json:"missing,omitempty"`
	// Errors are non-fatal problems encountered building this record.
	Errors []string `json:"errors,omitempty"`
}

// FineSample is one line of fine.jsonl: the generator's own series at 1s,
// which resolves cliffs faster than the kubelet's ~10s housekeeping. It
// deliberately carries no resource fields, so it cannot be plotted against a
// resource panel and imply an alignment that does not exist.
type FineSample struct {
	Arm     int       `json:"arm_cores"`
	Pass    int       `json:"pass"`
	Rung    int       `json:"rung"`
	RungQPS float64   `json:"rung_qps"`
	Warmup  bool      `json:"warmup"`
	T       time.Time `json:"t"`
	T0      time.Time `json:"t0"`
	T1      time.Time `json:"t1"`
	Load    GenStats  `json:"load"`
}

// buildSample assembles a record from a window and the sources sampled around
// it. Container usage is split into single roles and aggregated groups, and
// anything missing or unreadable is recorded on the sample rather than dropped.
func buildSample(w Window, targets []Target) (containers map[string]ContainerUsage, groups map[string]GroupUsage, spread time.Duration, missing []string, errs []string) {
	keys := make([]ContainerKey, 0, len(targets))
	byKey := make(map[ContainerKey]string, len(targets))
	for _, t := range targets {
		keys = append(keys, t.Key)
		byKey[t.Key] = t.Role
	}
	usage, spread, missingKeys, uerrs := w.Usage(keys)

	for _, k := range missingKeys {
		missing = append(missing, byKey[k]+"="+k.String())
	}
	sort.Strings(missing)
	for _, e := range uerrs {
		errs = append(errs, e.Error())
	}
	sort.Strings(errs)

	containers = map[string]ContainerUsage{}
	grouped := map[string][]ContainerUsage{}
	for k, u := range usage {
		role := byKey[k]
		switch role {
		case RoleControlPlane, RoleWorker:
			grouped[role] = append(grouped[role], u)
		default:
			containers[role] = u
		}
	}
	if len(grouped) > 0 {
		groups = map[string]GroupUsage{}
		for role, us := range grouped {
			groups[role] = aggregate(us)
		}
	}
	return containers, groups, spread, missing, errs
}
