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

// loadgen drives one arm of the demo and reports the end-to-end latency
// distribution.
//
// It spreads requests over a fixed set of actors, addressing each by the actor
// DNS name the router parses, which is the shape of traffic the comparison is
// about: a working set of already-running actors, hit repeatedly. That is
// exactly the case the current router spends one ResumeActor RPC on per
// request and the Rust module serves from cache.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	target      = flag.String("target", "http://localhost:10000", "base URL of the gateway under test")
	actors      = flag.Int("actors", 50, "size of the hot actor working set")
	atespace    = flag.String("atespace", "demo", "atespace the actors live in")
	suffix      = flag.String("suffix", "actors.resources.substrate.ate.dev", "actor DNS suffix")
	concurrency = flag.Int("concurrency", 32, "number of concurrent connections")
	duration    = flag.Duration("duration", 20*time.Second, "measurement window")
	warmup      = flag.Duration("warmup", 3*time.Second, "warmup window, excluded from the reported numbers")
	label       = flag.String("label", "arm", "label for this run")
	jsonOut     = flag.String("json-out", "", "if set, write the result as JSON to this path")
)

type result struct {
	Label       string  `json:"label"`
	Requests    int64   `json:"requests"`
	Errors      int64   `json:"errors"`
	NonOK       int64   `json:"non_2xx"`
	RPS         float64 `json:"rps"`
	P50ms       float64 `json:"p50_ms"`
	P90ms       float64 `json:"p90_ms"`
	P95ms       float64 `json:"p95_ms"`
	P99ms       float64 `json:"p99_ms"`
	MaxMs       float64 `json:"max_ms"`
	MeanMs      float64 `json:"mean_ms"`
	Concurrency int     `json:"concurrency"`
	Actors      int     `json:"actors"`
}

func main() {
	flag.Parse()

	// One transport shared by every worker, with a connection pool at least as
	// large as the concurrency: otherwise the client, not the gateway, becomes
	// the bottleneck and both arms measure the same thing.
	transport := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		MaxConnsPerHost:     *concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	var (
		mu        sync.Mutex
		latencies []time.Duration
		errs      atomic.Int64
		nonOK     atomic.Int64
		measuring atomic.Bool
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			local := make([]time.Duration, 0, 4096)
			for ctx.Err() == nil {
				actor := fmt.Sprintf("actor-%03d", rng.Intn(*actors))
				host := fmt.Sprintf("%s.%s.%s", actor, *atespace, *suffix)

				req, err := http.NewRequestWithContext(ctx, http.MethodGet, *target+"/", nil)
				if err != nil {
					continue
				}
				req.Host = host

				start := time.Now()
				resp, err := client.Do(req)
				elapsed := time.Since(start)
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					nonOK.Add(1)
				}
				if measuring.Load() {
					local = append(local, elapsed)
				}
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}(i)
	}

	time.Sleep(*warmup)
	measuring.Store(true)
	measureStart := time.Now()
	time.Sleep(*duration)
	measured := time.Since(measureStart)
	measuring.Store(false)
	cancel()
	wg.Wait()

	if len(latencies) == 0 {
		log.Fatalf("%s: no successful requests were recorded", *label)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	ms := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }
	pct := func(p float64) float64 {
		idx := int(p / 100 * float64(len(latencies)))
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return ms(latencies[idx])
	}
	var total time.Duration
	for _, d := range latencies {
		total += d
	}

	r := result{
		Label:       *label,
		Requests:    int64(len(latencies)),
		Errors:      errs.Load(),
		NonOK:       nonOK.Load(),
		RPS:         float64(len(latencies)) / measured.Seconds(),
		P50ms:       pct(50),
		P90ms:       pct(90),
		P95ms:       pct(95),
		P99ms:       pct(99),
		MaxMs:       ms(latencies[len(latencies)-1]),
		MeanMs:      ms(total / time.Duration(len(latencies))),
		Concurrency: *concurrency,
		Actors:      *actors,
	}

	fmt.Printf("%-22s reqs=%-8d rps=%-9.0f p50=%-7.2f p95=%-7.2f p99=%-7.2f max=%-8.2f err=%d non2xx=%d\n",
		r.Label, r.Requests, r.RPS, r.P50ms, r.P95ms, r.P99ms, r.MaxMs, r.Errors, r.NonOK)

	if *jsonOut != "" {
		f, err := os.Create(*jsonOut)
		if err != nil {
			log.Fatalf("creating %s: %v", *jsonOut, err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			log.Fatalf("writing result: %v", err)
		}
	}
}
