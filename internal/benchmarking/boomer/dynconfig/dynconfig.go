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

// Package dynconfig fetches and holds the boomer worker's runtime-mutable
// settings — the subset of locust flags the operator can change in the web
// UI form. The boomer wire protocol only carries num_users + spawn_rate, so
// these come over an HTTP side channel from the master's /boomer-config
// endpoint (common/boomer_config.py).
package dynconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/myzhan/boomer"
)

// Config is the dynamic-mutable subset of boomer's behavior. Holder swaps
// it atomically so task goroutines read a consistent snapshot.
type Config struct {
	MinWait          time.Duration
	MaxWait          time.Duration
	TraceProbability float64
}

// Holder lets readers Load() the current Config and writers Store() a new
// one. Backed by atomic.Pointer for lock-free reads on the hot path.
type Holder struct {
	v atomic.Pointer[Config]
}

func NewHolder(initial Config) *Holder {
	h := &Holder{}
	h.v.Store(&initial)
	return h
}

func (h *Holder) Load() Config { return *h.v.Load() }

func (h *Holder) Store(c Config) { h.v.Store(&c) }

// ProbabilityUpdater is the subset of trace.UpdatableSampler we touch here;
// kept as an interface so this package doesn't depend on the trace package.
type ProbabilityUpdater interface {
	UpdateProbability(p float64)
}

// payload mirrors the master's /boomer-config JSON. Fields are pointers so
// we can distinguish "absent" (leave current value) from "explicitly zero".
// The same shape is used for static --config-json input and the live
// /boomer-config endpoint, so master + Python runner + Go worker share one
// vocabulary for the boomer's runtime-tunable knobs.
type payload struct {
	TraceProbability *float64 `json:"trace_probability"`
	MinWaitTime      *float64 `json:"min_wait_time"`
	MaxWaitTime      *float64 `json:"max_wait_time"`
}

// Parse decodes a JSON blob (typically from a CLI flag) and merges its
// fields into `current`. Returns the merged Config — unset fields preserve
// `current`'s existing values, matching Fetch's behavior.
func Parse(jsonBytes []byte, current Config) (Config, error) {
	if len(jsonBytes) == 0 {
		return current, nil
	}
	var p payload
	if err := json.Unmarshal(jsonBytes, &p); err != nil {
		return current, fmt.Errorf("decode config json: %w", err)
	}
	return p.merge(current), nil
}

// Fetch GETs `url` and merges any returned fields into `current`. Returns
// the merged Config (or current unchanged on a soft no-op response).
func Fetch(ctx context.Context, url string, current Config) (Config, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return current, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return current, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return current, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	var p payload
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return current, fmt.Errorf("decode %s: %w", url, err)
	}
	return p.merge(current), nil
}

// merge folds the payload's set fields into `current`, leaving unset fields
// at their existing values. Used by both Parse (CLI input) and Fetch (HTTP
// pull) so the merge semantics are identical.
func (p payload) merge(current Config) Config {
	out := current
	if p.TraceProbability != nil {
		out.TraceProbability = *p.TraceProbability
	}
	if p.MinWaitTime != nil {
		out.MinWait = time.Duration(*p.MinWaitTime * float64(time.Second))
	}
	if p.MaxWaitTime != nil {
		out.MaxWait = time.Duration(*p.MaxWaitTime * float64(time.Second))
	}
	return out
}

// SubscribeSpawn registers a boomer Events handler that fetches `url` on
// each spawn message (≈ once per test start) and applies the result to
// `holder` + `sampler`. `onError` is invoked when a fetch fails; production
// callers typically exit the process there per the "treat as fatal" design.
// Returns an error if the event subscription itself fails (handler signature
// mismatch), which is a programmer error and should be treated as fatal too.
func SubscribeSpawn(url string, holder *Holder, sampler ProbabilityUpdater, fetchTimeout time.Duration, onError func(error)) error {
	return boomer.Events.Subscribe("boomer:spawn", func(spawnCount int, spawnRate float64) {
		ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		defer cancel()
		next, err := Fetch(ctx, url, holder.Load())
		if err != nil {
			slog.Error("dynconfig fetch failed",
				slog.String("url", url), slog.String("err", err.Error()))
			onError(err)
			return
		}
		holder.Store(next)
		sampler.UpdateProbability(next.TraceProbability)
		slog.Info("dynconfig applied",
			slog.Float64("trace_probability", next.TraceProbability),
			slog.Duration("min_wait", next.MinWait),
			slog.Duration("max_wait", next.MaxWait),
		)
	})
}
