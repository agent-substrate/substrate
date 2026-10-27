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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/procstats"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/report"
)

// Egress has to start inside an actor's sandbox, where the sandbox's nftables
// redirect hands every outbound connection to atunnel. The egress actor runs
// there and executes egress modes on the load generator's behalf; the two
// talk over a Unix socket, which reaches across network namespaces.

// The destinations the egress actor dials. Nothing answers them: atunnel takes
// every connection, and the fake gateway answers or discards by port.
const (
	egressAnswerDestination  = "192.0.2.10:80"
	egressDiscardDestination = "192.0.2.10:9"
)

// egressActorHolding is the line the egress actor writes when EgressIdle has
// opened its connections and holds them, so the generator can read atunnel
// then.
const egressActorHolding = "holding"

// egressActorRequest is options on the wire.
type egressActorRequest struct {
	Mode                    Mode          `json:"mode"`
	Rate                    float64       `json:"rate"`
	Duration                time.Duration `json:"duration"`
	Warmup                  time.Duration `json:"warmup"`
	Workers                 int           `json:"workers"`
	Connections             int           `json:"connections"`
	NewConnectionPerRequest bool          `json:"new_connection_per_request"`
	PayloadBytes            int           `json:"payload_bytes"`
}

type egressActorResult struct {
	Load  report.Load `json:"load"`
	Error string      `json:"error,omitempty"`
}

// isEgress reports whether a mode's traffic starts inside the sandbox.
func isEgress(m Mode) bool {
	return m == EgressHTTP || m == EgressIdle || m == EgressStream
}

// ServeEgressActor serves egress runs on lis until ctx ends.
func ServeEgressActor(ctx context.Context, lis net.Listener) error {
	return serveEgressActor(ctx, lis, Targets{Egress: egressAnswerDestination, EgressDiscard: egressDiscardDestination})
}

func serveEgressActor(ctx context.Context, lis net.Listener, targets Targets) error {
	mux := http.NewServeMux()
	// The egress actor generates the egress load, so its CPU is the generator's.
	mux.Handle("GET /stats", procstats.Handler(nil))
	mux.HandleFunc("POST /run", func(w http.ResponseWriter, r *http.Request) {
		var req egressActorRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rc := http.NewResponseController(w)
		opts := options{
			mode: req.Mode, rate: req.Rate, duration: req.Duration, warmup: req.Warmup,
			workers: req.Workers, connections: req.Connections,
			newConnectionPerRequest: req.NewConnectionPerRequest, payloadBytes: req.PayloadBytes,
			whileHolding: func(context.Context) {
				_, _ = fmt.Fprintln(w, egressActorHolding)
				_ = rc.Flush()
			},
		}
		var res egressActorResult
		l, err := execute(r.Context(), targets, opts)
		if err != nil {
			res.Error = err.Error()
		}
		res.Load = l
		_ = json.NewEncoder(w).Encode(res)
	})
	server := &http.Server{Handler: mux}
	context.AfterFunc(ctx, func() { _ = server.Close() })
	if err := server.Serve(lis); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// executeOnEgressActor runs opts on the egress actor at socket.
func executeOnEgressActor(ctx context.Context, socket string, opts options) (report.Load, error) {
	body, err := json.Marshal(egressActorRequest{
		Mode: opts.mode, Rate: opts.rate, Duration: opts.duration, Warmup: opts.warmup,
		Workers: opts.workers, Connections: opts.connections,
		NewConnectionPerRequest: opts.newConnectionPerRequest, PayloadBytes: opts.payloadBytes,
	})
	if err != nil {
		return report.Load{}, err
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://egress-actor/run", bytes.NewReader(body))
	if err != nil {
		return report.Load{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return report.Load{}, fmt.Errorf("egress actor: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return report.Load{}, fmt.Errorf("egress actor: HTTP %d", resp.StatusCode)
	}
	lines := bufio.NewScanner(resp.Body)
	lines.Buffer(make([]byte, 64<<10), 1<<20)
	for lines.Scan() {
		if lines.Text() == egressActorHolding {
			if opts.whileHolding != nil {
				opts.whileHolding(ctx)
			}
			continue
		}
		var res egressActorResult
		if err := json.Unmarshal(lines.Bytes(), &res); err != nil {
			return report.Load{}, fmt.Errorf("egress actor: %w", err)
		}
		if res.Error != "" {
			return report.Load{}, fmt.Errorf("egress actor: %s", res.Error)
		}
		return res.Load, nil
	}
	if err := lines.Err(); err != nil {
		return report.Load{}, fmt.Errorf("egress actor: %w", err)
	}
	return report.Load{}, fmt.Errorf("egress actor: no result")
}

// executeOnEgressActors splits an EgressIdle run evenly across the egress
// actors at sockets, one per sandbox. A sandbox has one address, so it can hold
// at most ~64k egress connections at a time: atunnel's redirect makes every one
// of them look the same but for the source port. whileHolding runs once, after
// every actor holds its connections.
func executeOnEgressActors(ctx context.Context, sockets []string, opts options) (report.Load, error) {
	n := len(sockets)
	var waiting atomic.Int64
	waiting.Store(int64(n))
	loads := make([]report.Load, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i, socket := range sockets {
		sub := opts
		sub.rate = opts.rate / float64(n)
		sub.connections = opts.connections / n
		if i < opts.connections%n {
			sub.connections++
		}
		sub.whileHolding = func(ctx context.Context) {
			if waiting.Add(-1) == 0 && opts.whileHolding != nil {
				opts.whileHolding(ctx)
			}
		}
		wg.Go(func() { loads[i], errs[i] = executeOnEgressActor(ctx, socket, sub) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return report.Load{}, err
	}
	return mergeLoads(loads), nil
}

// mergeLoads adds up loads that ran side by side. Latency percentiles are the
// worst of them.
func mergeLoads(loads []report.Load) report.Load {
	var m report.Load
	worst := func(a, b report.Percentiles) report.Percentiles {
		return report.Percentiles{P50: max(a.P50, b.P50), P90: max(a.P90, b.P90), P99: max(a.P99, b.P99), Max: max(a.Max, b.Max), Mean: max(a.Mean, b.Mean)}
	}
	for _, l := range loads {
		m.Offered += l.Offered
		m.MeasuredSeconds = max(m.MeasuredSeconds, l.MeasuredSeconds)
		m.Scheduled += l.Scheduled
		m.Skipped += l.Skipped
		m.Completed += l.Completed
		m.Errors += l.Errors
		m.Bytes += l.Bytes
		m.Latency = worst(m.Latency, l.Latency)
		m.SendLag = worst(m.SendLag, l.SendLag)
		m.ErrorSamples = append(m.ErrorSamples, l.ErrorSamples...)
	}
	if len(m.ErrorSamples) > maxErrorSamples {
		m.ErrorSamples = m.ErrorSamples[:maxErrorSamples]
	}
	if m.MeasuredSeconds > 0 {
		m.Achieved = float64(m.Completed) / m.MeasuredSeconds
		m.MBytesPerSec = float64(m.Bytes) / m.MeasuredSeconds / 1e6
	}
	return m
}
