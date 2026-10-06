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

package glutton

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// UseEgress replaces the running egress loop. An interval_ms of 0 stops it
// and keeps the results for the next DrainEgress.
func (s *Service) UseEgress(_ context.Context, req *gluttonpb.UseEgressRequest) (*gluttonpb.UseEgressResponse, error) {
	if req.GetIntervalMs() < 0 {
		return nil, status.Error(codes.InvalidArgument, "interval_ms must be non-negative")
	}
	if req.GetIntervalMs() == 0 {
		s.egress.Stop()
		return &gluttonpb.UseEgressResponse{}, nil
	}
	u, err := url.Parse(req.GetUrl())
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, status.Errorf(codes.InvalidArgument, "url %q must be an absolute http or https URL", req.GetUrl())
	}
	interval := time.Duration(req.GetIntervalMs()) * time.Millisecond
	s.egress.Set(req.GetUrl(), interval, req.GetDisableKeepAlives())
	return &gluttonpb.UseEgressResponse{}, nil
}

// DrainEgress returns the results recorded since the last drain and forgets
// them. With stop set, the loop is stopped first, so no call is left out.
func (s *Service) DrainEgress(_ context.Context, req *gluttonpb.DrainEgressRequest) (*gluttonpb.DrainEgressResponse, error) {
	if req.GetStop() {
		s.egress.Stop()
	}
	samples, dropped := s.egress.Drain()
	return &gluttonpb.DrainEgressResponse{Samples: samples, Dropped: dropped}, nil
}

// egressBufferCap bounds the results kept between drains. A variable so tests
// can shrink it.
var egressBufferCap = 4096

// egressLoad runs one loop that GETs a URL at a fixed interval and keeps the
// result of each call until Drain. Set replaces the loop; Stop ends it.
type egressLoad struct {
	// mu guards the loop handle. Set and Stop hold it while they wait for
	// the loop to exit, so the loop itself never takes it.
	mu     sync.Mutex
	stop   chan struct{}
	done   chan struct{}
	client *http.Client

	resultsMu sync.Mutex
	samples   []*gluttonpb.EgressSample
	dropped   int64
}

// Set stops any running loop, forgets its results, and starts a new loop that
// calls target now and then once per interval. Each call times out after one
// interval, so calls never overlap.
func (e *egressLoad) Set(target string, interval time.Duration, disableKeepAlives bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.stopLocked()
	e.resultsMu.Lock()
	e.samples, e.dropped = nil, 0
	e.resultsMu.Unlock()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = disableKeepAlives
	client := &http.Client{Timeout: interval, Transport: transport}
	stop, done := make(chan struct{}), make(chan struct{})
	e.stop, e.done, e.client = stop, done, client
	go e.run(client, target, interval, stop, done)
}

// Stop ends the running loop, if any, after the call in flight completes.
func (e *egressLoad) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stopLocked()
}

// stopLocked signals the loop, waits for it to exit, and closes the idle
// connections it kept alive: after a suspend they are dead, so the next loop
// must not reuse them. e.mu must be held.
func (e *egressLoad) stopLocked() {
	if e.stop == nil {
		return
	}
	close(e.stop)
	<-e.done
	e.client.CloseIdleConnections()
	e.stop, e.done, e.client = nil, nil, nil
}

// Drain returns the results recorded since the last drain, oldest first, and
// the number discarded because the buffer was full, then resets both.
func (e *egressLoad) Drain() ([]*gluttonpb.EgressSample, int64) {
	e.resultsMu.Lock()
	defer e.resultsMu.Unlock()
	samples, dropped := e.samples, e.dropped
	e.samples, e.dropped = nil, 0
	return samples, dropped
}

// run makes the first call immediately and then one per tick. The stop
// signal is checked only between calls, so a call in flight always completes
// and is recorded.
func (e *egressLoad) run(client *http.Client, target string, interval time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for first := true; ; first = false {
		e.record(egressCall(client, target, first))
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
	}
}

func (e *egressLoad) record(sample *gluttonpb.EgressSample) {
	e.resultsMu.Lock()
	defer e.resultsMu.Unlock()
	if len(e.samples) >= egressBufferCap {
		e.dropped++
		return
	}
	e.samples = append(e.samples, sample)
}

// egressCall GETs target once and reads the whole body. The latency covers
// connecting, the request, and the body; the client's timeout bounds it.
func egressCall(client *http.Client, target string, first bool) *gluttonpb.EgressSample {
	sample := &gluttonpb.EgressSample{First: first}
	trace := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { sample.ReusedConn = info.Reused },
	}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodGet, target, nil)
	if err != nil {
		sample.Error = err.Error()
		return sample
	}
	start := time.Now()
	resp, err := client.Do(req)
	if err == nil {
		sample.StatusCode = int32(resp.StatusCode)
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	sample.LatencyUs = time.Since(start).Microseconds()
	if err != nil {
		sample.Error = err.Error()
	}
	return sample
}
