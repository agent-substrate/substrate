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

// Scraping the kubelet's cAdvisor endpoint, and turning two scrapes of a container
// into the CPU and memory it used over the interval between them.

package routercap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// cAdvisor, reached through the kubelet, is the only source that reports raw
// cumulative counters, CFS accounting and a per-container timestamp together.
// metrics.k8s.io serves pre-averaged rates with one timestamp per pod and no
// CFS data, which loses everything the windowing here depends on.
const (
	metricCPUUsageSeconds     = "container_cpu_usage_seconds_total"
	metricMemoryWorkingSet    = "container_memory_working_set_bytes"
	metricMemoryRSS           = "container_memory_rss"
	metricCFSPeriods          = "container_cpu_cfs_periods_total"
	metricCFSThrottledPeriods = "container_cpu_cfs_throttled_periods_total"
	metricCFSThrottledSeconds = "container_cpu_cfs_throttled_seconds_total"
	metricSpecCPUQuota        = "container_spec_cpu_quota"
	metricSpecCPUPeriod       = "container_spec_cpu_period"
)

var cadvisorMetrics = map[string]bool{
	metricCPUUsageSeconds:     true,
	metricMemoryWorkingSet:    true,
	metricMemoryRSS:           true,
	metricCFSPeriods:          true,
	metricCFSThrottledPeriods: true,
	metricCFSThrottledSeconds: true,
	metricSpecCPUQuota:        true,
	metricSpecCPUPeriod:       true,
}

// ContainerKey identifies one container across scrapes. Pod name is included
// because an arm change restarts the router pod, and silently carrying a
// counter across that restart would show up as a large negative CPU rate.
type ContainerKey struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
}

func (k ContainerKey) String() string {
	return k.Namespace + "/" + k.Pod + "/" + k.Container
}

// ContainerSample is one container's cumulative counters at one cAdvisor
// housekeeping instant.
type ContainerSample struct {
	Key ContainerKey
	// At is cAdvisor's own timestamp for the sample, not the time we fetched
	// it. The kubelet refreshes on its own ~10s cadence, so a fetch says
	// nothing about when the numbers were true.
	At time.Time

	CPUSecondsTotal       float64
	MemoryWorkingSetBytes float64
	MemoryRSSBytes        float64
	CFSPeriods            float64
	CFSThrottledPeriods   float64
	CFSThrottledSeconds   float64

	// CPUQuota and CPUPeriod are the cgroup's CFS settings, from which the
	// effective core limit is derived. A quota of -1 means unlimited.
	CPUQuota  float64
	CPUPeriod float64
}

// LimitCores is the container's CPU limit in cores, or 0 if it has none. A
// BestEffort or Burstable container reports 0 here, which is itself worth
// seeing: it means the arm's x-axis value was not enforced.
func (s ContainerSample) LimitCores() float64 {
	if s.CPUQuota <= 0 || s.CPUPeriod <= 0 {
		return 0
	}
	return s.CPUQuota / s.CPUPeriod
}

// CadvisorScrape is one fetch of the kubelet's cAdvisor endpoint.
type CadvisorScrape struct {
	// FetchedAt is local wall clock at the moment the response was read.
	FetchedAt time.Time
	// Containers is keyed by namespace/pod/container. Pod-level and
	// node-level cgroup rows are dropped.
	Containers map[ContainerKey]ContainerSample
}

// SkewAgainst reports how far the sample for key lags local wall clock at
// fetch time. It conflates real clock skew between this pod and the node with
// cAdvisor housekeeping age, and cannot separate them — which is exactly why
// the run header records the measured value instead of assuming zero.
func (s CadvisorScrape) SkewAgainst(key ContainerKey) (time.Duration, bool) {
	c, ok := s.Containers[key]
	if !ok {
		return 0, false
	}
	return s.FetchedAt.Sub(c.At), true
}

// parseCadvisor reads a kubelet cAdvisor exposition into a scrape.
func parseCadvisor(r io.Reader, fetchedAt time.Time) (CadvisorScrape, error) {
	out := CadvisorScrape{FetchedAt: fetchedAt, Containers: map[ContainerKey]ContainerSample{}}
	err := scanPromText(r, cadvisorMetrics, func(s promSample) {
		name := s.Labels["container"]
		// cAdvisor also emits rows for the pod sandbox ("POD") and the
		// pod-level cgroup (empty container); both double-count the real
		// containers.
		if name == "" || name == "POD" {
			return
		}
		key := ContainerKey{
			Namespace: s.Labels["namespace"],
			Pod:       s.Labels["pod"],
			Container: name,
		}
		cs, ok := out.Containers[key]
		if !ok {
			cs = ContainerSample{Key: key}
		}
		if s.TimestampMs != 0 {
			at := time.UnixMilli(s.TimestampMs)
			// Metric families for one container can carry slightly different
			// housekeeping timestamps; keep the newest so the interval is never
			// credited to an instant before its data.
			if at.After(cs.At) {
				cs.At = at
			}
		}
		switch s.Name {
		case metricCPUUsageSeconds:
			cs.CPUSecondsTotal = s.Value
		case metricMemoryWorkingSet:
			cs.MemoryWorkingSetBytes = s.Value
		case metricMemoryRSS:
			cs.MemoryRSSBytes = s.Value
		case metricCFSPeriods:
			cs.CFSPeriods = s.Value
		case metricCFSThrottledPeriods:
			cs.CFSThrottledPeriods = s.Value
		case metricCFSThrottledSeconds:
			cs.CFSThrottledSeconds = s.Value
		case metricSpecCPUQuota:
			cs.CPUQuota = s.Value
		case metricSpecCPUPeriod:
			cs.CPUPeriod = s.Value
		}
		out.Containers[key] = cs
	})
	if err != nil {
		return CadvisorScrape{}, err
	}
	return out, nil
}

// ContainerUsage is a container's resource use over one interval. CPU is a
// rate derived from two cumulative samples; memory is a level read at the end
// of the interval, because a working set has no meaningful rate.
type ContainerUsage struct {
	Container string `json:"container"`
	// CPUCores is mean cores consumed over the interval: seconds of CPU time
	// per second of wall clock.
	CPUCores float64 `json:"cpu_cores"`
	// CPULimitCores is the cgroup quota, so a reader can see utilization
	// against the limit rather than against the node.
	CPULimitCores float64 `json:"cpu_limit_cores"`
	// CPUUtilization is CPUCores/CPULimitCores, or 0 when unlimited.
	CPUUtilization float64 `json:"cpu_utilization"`

	MemoryWorkingSetBytes float64 `json:"memory_working_set_bytes"`
	MemoryRSSBytes        float64 `json:"memory_rss_bytes"`

	// ThrottledPeriods and ThrottledSeconds are deltas over the interval. Any
	// throttling on a control-plane container invalidates the arm.
	ThrottledPeriods float64 `json:"throttled_periods"`
	Periods          float64 `json:"periods"`
	ThrottledSeconds float64 `json:"throttled_seconds"`
	// ThrottledFraction is ThrottledPeriods/Periods.
	ThrottledFraction float64 `json:"throttled_fraction"`
}

// usageBetween derives an interval's usage from two samples of the same
// container. It errors when the samples cannot describe an interval: a counter
// that went backwards means the container restarted.
func usageBetween(prev, cur ContainerSample) (ContainerUsage, error) {
	if prev.Key != cur.Key {
		return ContainerUsage{}, fmt.Errorf("samples are for different containers: %s and %s", prev.Key, cur.Key)
	}
	secs := cur.At.Sub(prev.At).Seconds()
	if secs <= 0 {
		return ContainerUsage{}, fmt.Errorf("%s: non-advancing cAdvisor timestamps (%s to %s)", cur.Key, prev.At, cur.At)
	}
	if cur.CPUSecondsTotal < prev.CPUSecondsTotal {
		return ContainerUsage{}, fmt.Errorf("%s: cpu counter went backwards (%.3f to %.3f): the container restarted mid-interval",
			cur.Key, prev.CPUSecondsTotal, cur.CPUSecondsTotal)
	}

	u := ContainerUsage{
		Container:             cur.Key.Container,
		CPUCores:              (cur.CPUSecondsTotal - prev.CPUSecondsTotal) / secs,
		CPULimitCores:         cur.LimitCores(),
		MemoryWorkingSetBytes: cur.MemoryWorkingSetBytes,
		MemoryRSSBytes:        cur.MemoryRSSBytes,
		ThrottledPeriods:      cur.CFSThrottledPeriods - prev.CFSThrottledPeriods,
		Periods:               cur.CFSPeriods - prev.CFSPeriods,
		ThrottledSeconds:      cur.CFSThrottledSeconds - prev.CFSThrottledSeconds,
	}
	if u.CPULimitCores > 0 {
		u.CPUUtilization = u.CPUCores / u.CPULimitCores
	}
	if u.Periods > 0 {
		u.ThrottledFraction = u.ThrottledPeriods / u.Periods
	}
	return u, nil
}

// CadvisorClient fetches and parses the kubelet's cAdvisor endpoint for one
// node.
type CadvisorClient struct {
	// Fetch returns the raw exposition. Injected so the parsing and windowing
	// logic can be tested without a cluster.
	Fetch func(ctx context.Context) (io.ReadCloser, error)
}

// Scrape fetches and parses one sample set.
func (c *CadvisorClient) Scrape(ctx context.Context) (CadvisorScrape, error) {
	rc, err := c.Fetch(ctx)
	if err != nil {
		return CadvisorScrape{}, fmt.Errorf("fetch cadvisor: %w", err)
	}
	defer rc.Close()
	return parseCadvisor(rc, time.Now())
}

// Scraper is the window driver's view of cAdvisor. An interface because the
// run watches containers on four different nodes and one node's kubelet only
// reports its own.
type Scraper interface {
	Scrape(ctx context.Context) (CadvisorScrape, error)
}

// MultiNodeClient merges several nodes' cAdvisor surfaces into one scrape.
// Window boundaries still come from the anchor node's clock and each
// container's rate from its own timestamp pair; the cost — other nodes
// housekeeping on their own schedules — is reported as alignment spread.
type MultiNodeClient struct {
	Clients []*CadvisorClient
}

// Scrape fetches every node concurrently — sequential fetches would widen the
// alignment spread — and merges the results. Any node failing fails the
// scrape: a partial merge would silently blind the control-plane guard.
func (m *MultiNodeClient) Scrape(ctx context.Context) (CadvisorScrape, error) {
	if len(m.Clients) == 0 {
		return CadvisorScrape{}, fmt.Errorf("no cadvisor clients")
	}
	if len(m.Clients) == 1 {
		return m.Clients[0].Scrape(ctx)
	}

	scrapes := make([]CadvisorScrape, len(m.Clients))
	errs := make([]error, len(m.Clients))
	var wg sync.WaitGroup
	for i, c := range m.Clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scrapes[i], errs[i] = c.Scrape(ctx)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return CadvisorScrape{}, err
	}

	out := CadvisorScrape{Containers: map[ContainerKey]ContainerSample{}}
	for _, s := range scrapes {
		// Latest fetch time of the set, so the reported skew is an upper bound
		// across every node.
		if s.FetchedAt.After(out.FetchedAt) {
			out.FetchedAt = s.FetchedAt
		}
		for k, v := range s.Containers {
			out.Containers[k] = v
		}
	}
	return out, nil
}
