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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/procstats"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/report"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/sink"
)

// Stats is what a benchmark process's /stats serves. Each process fills the
// fields that are its own.
type Stats struct {
	Process procstats.Snapshot `json:"process"`
	// Tunnel is set by atunnel (atunnel-bench tunnel).
	Tunnel *TunnelInfo `json:"tunnel,omitempty"`
	// IngressActor is set by the ingress actors, Gateway by the sink.
	IngressActor *sink.IngressActorStats `json:"ingress_actor,omitempty"`
	Gateway      *sink.GatewayStats      `json:"gateway,omitempty"`
}

// sinkStats reads the gateway's counters and the ingress actors', summed.
func (r *Runner) sinkStats(ctx context.Context) (Stats, error) {
	st, err := r.sink.stats(ctx)
	if err != nil || r.SharedDir == "" {
		return st, err
	}
	sockets, err := filepath.Glob(filepath.Join(r.SharedDir, "ingress-actor-*.sock"))
	if err != nil {
		return st, err
	}
	total := sink.IngressActorStats{}
	for _, socket := range sockets {
		w, err := unixAdmin(socket).stats(ctx)
		if err != nil || w.IngressActor == nil {
			// An ingress actor of a replaced atunnel; its successor's are
			// read under the same names.
			continue
		}
		total.Requests += w.IngressActor.Requests
		total.ConnectionsSeen += w.IngressActor.ConnectionsSeen
		total.ConnectionsOpen += w.IngressActor.ConnectionsOpen
	}
	st.IngressActor = &total
	return st, nil
}

// unixAdmin reads /stats served on a Unix socket.
func unixAdmin(socket string) *admin {
	return &admin{base: "http://unix", client: &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}},
	}}
}

// TunnelInfo is the part of atunnel's hosted actors the generator uses.
type TunnelInfo struct {
	Sandboxes []struct {
		Actor string `json:"actor"`
	} `json:"sandboxes"`
	ActorPort int `json:"actor_port"`
}

// admin reads a benchmark process's admin endpoint.
type admin struct {
	base   string
	client *http.Client
}

func newAdmin(base string) *admin {
	return &admin{base: base, client: &http.Client{Timeout: 5 * time.Second}}
}

func (a *admin) stats(ctx context.Context) (Stats, error) {
	return a.get(ctx, "/stats")
}

// liveStats is stats after a garbage collection in the process.
func (a *admin) liveStats(ctx context.Context) (Stats, error) {
	return a.get(ctx, "/stats?gc=1")
}

func (a *admin) get(ctx context.Context, path string) (Stats, error) {
	var s Stats
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+path, nil)
	if err != nil {
		return s, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return s, fmt.Errorf("%s/stats: HTTP %d", a.base, resp.StatusCode)
	}
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// waitReady polls /healthz until it answers or ctx ends.
func (a *admin) waitReady(ctx context.Context) error {
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+"/healthz", nil)
		if err != nil {
			return err
		}
		resp, err := a.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not ready: %w", a.base, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// fetch saves a pprof endpoint's response to path.
func (a *admin) fetch(ctx context.Context, endpoint, path string) error {
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s%s: HTTP %d", a.base, endpoint, resp.StatusCode)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// watch samples a process over a measured window: a snapshot at the start, at
// the end, and every interval between, for peaks.
type watch struct {
	read  func(context.Context) (procstats.Snapshot, error)
	start procstats.Snapshot

	mu   sync.Mutex
	peak procstats.Snapshot
	err  error
	stop chan struct{}
	done chan struct{}
}

// failuresToGiveUp is how many reads in a row must fail before a watched
// process counts as gone.
const failuresToGiveUp = 2

// startWatch takes the first snapshot and samples every interval after. If
// the process stops answering, it calls onGone, when set, once.
func startWatch(ctx context.Context, read func(context.Context) (procstats.Snapshot, error), interval time.Duration, onGone func()) *watch {
	w := &watch{read: read, stop: make(chan struct{}), done: make(chan struct{})}
	w.start, w.err = read(ctx)
	w.peak = w.start
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		failures := 0
		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				s, err := read(ctx)
				if err == nil && !s.StartedAt.Equal(w.start.StartedAt) {
					err = fmt.Errorf("process restarted at %s", s.StartedAt.Format(time.TimeOnly))
					failures = failuresToGiveUp - 1
				}
				w.mu.Lock()
				if err != nil {
					w.err = err
					failures++
				} else {
					w.observe(s)
					failures = 0
				}
				w.mu.Unlock()
				if failures == failuresToGiveUp && onGone != nil {
					onGone()
				}
			}
		}
	}()
	return w
}

func (w *watch) observe(s procstats.Snapshot) {
	w.peak.RSSBytes = max(w.peak.RSSBytes, s.RSSBytes)
	w.peak.HeapInUseBytes = max(w.peak.HeapInUseBytes, s.HeapInUseBytes)
	w.peak.Goroutines = max(w.peak.Goroutines, s.Goroutines)
	if s.Cgroup != nil {
		if w.peak.Cgroup == nil {
			c := *s.Cgroup
			w.peak.Cgroup = &c
		}
		w.peak.Cgroup.MemoryCurrentBytes = max(w.peak.Cgroup.MemoryCurrentBytes, s.Cgroup.MemoryCurrentBytes)
	}
}

// finish ends the window and returns the process's cost over it, with CPU
// spread over units.
func (w *watch) finish(ctx context.Context, units uint64) (report.Resources, error) {
	close(w.stop)
	<-w.done
	end, err := w.read(ctx)
	if err == nil && !end.StartedAt.Equal(w.start.StartedAt) {
		err = fmt.Errorf("process restarted at %s", end.StartedAt.Format(time.TimeOnly))
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		return report.Resources{}, err
	}
	if w.err != nil {
		return report.Resources{}, w.err
	}
	w.observe(end)
	elapsed := end.Time.Sub(w.start.Time).Seconds()
	res := report.Resources{
		CPUSeconds:    end.CPUSeconds - w.start.CPUSeconds,
		GOMAXPROCS:    end.GOMAXPROCS,
		RSSEndBytes:   end.RSSBytes,
		RSSMaxBytes:   w.peak.RSSBytes,
		RSSIsPeak:     end.RSSIsPeak,
		HeapEndBytes:  end.HeapInUseBytes,
		HeapMaxBytes:  w.peak.HeapInUseBytes,
		GoroutinesMax: w.peak.Goroutines,
	}
	if elapsed > 0 {
		res.Cores = res.CPUSeconds / elapsed
	}
	if units > 0 {
		res.MicrosPerUnit = res.CPUSeconds / float64(units) * 1e6
	}
	if end.Cgroup != nil && w.start.Cgroup != nil {
		if elapsed > 0 {
			res.CgroupCores = (end.Cgroup.CPUSeconds - w.start.Cgroup.CPUSeconds) / elapsed
		}
		res.CgroupMemEndBytes = end.Cgroup.MemoryCurrentBytes
		res.CgroupMemMaxBytes = w.peak.Cgroup.MemoryCurrentBytes
		res.CgroupCPULimitCore = end.Cgroup.CPULimitCores
	}
	return res, nil
}
