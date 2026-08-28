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
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/myzhan/boomer"
)

// Resume modes. Explicit issues a ResumeActor RPC before sending traffic.
// Implicit issues no wake request at all: the actor stays suspended until a
// request reaches the atenet router, which wakes it while the request is
// parked.
const (
	ResumeModeExplicit = "explicit"
	ResumeModeImplicit = "implicit"

	ReadModeData   = "data"
	ReadModeDigest = "digest"
)

// Config is the dynamic-mutable subset of boomer's behavior. Holder swaps
// it atomically so task goroutines read a consistent snapshot.
type Config struct {
	MinWait          time.Duration
	MaxWait          time.Duration
	TraceProbability float64
	DurDirFileSize   int64  // bytes
	ResumeMode       string // ResumeModeExplicit | ResumeModeImplicit
	DurDirReadMode   string // ReadModeData | ReadModeDigest
	DurDirTemplate   string // ActorTemplate name
	MemTarget        string // resident RAM the GluttonUser fills via WriteRAM, suffixed (e.g. "2Gi"); "" disables
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
	DurDirFileSize   *float64 `json:"durdir_file_size_bytes"`
	ResumeMode       *string  `json:"resume_mode"`
	DurDirReadMode   *string  `json:"durdir_read_mode"`
	DurDirTemplate   *string  `json:"durdir_template"`
	MemTarget        *string  `json:"mem_target"`
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
	merged := p.merge(current)
	if err := merged.Validate(); err != nil {
		return current, fmt.Errorf("validate config: %w", err)
	}
	return merged, nil
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
	merged := p.merge(current)
	if err := merged.Validate(); err != nil {
		return current, fmt.Errorf("validate %s: %w", url, err)
	}
	return merged, nil
}

// Validate checks that the config values are within legal ranges.
func (c Config) Validate() error {
	if c.MinWait < 0 {
		return fmt.Errorf("min_wait_time cannot be negative: %v", c.MinWait)
	}
	if c.MaxWait < 0 {
		return fmt.Errorf("max_wait_time cannot be negative: %v", c.MaxWait)
	}
	if c.MaxWait < c.MinWait {
		return fmt.Errorf("max_wait_time (%v) cannot be less than min_wait_time (%v)", c.MaxWait, c.MinWait)
	}
	if c.TraceProbability < 0 || c.TraceProbability > 1 {
		return fmt.Errorf("trace_probability must be between 0.0 and 1.0, got: %f", c.TraceProbability)
	}
	if c.DurDirFileSize < 0 {
		return fmt.Errorf("durdir_file_size_bytes cannot be negative: %d", c.DurDirFileSize)
	}
	if c.DurDirFileSize > math.MaxInt32 {
		return fmt.Errorf("durdir_file_size_bytes cannot exceed %d (2 GiB), got: %d", math.MaxInt32, c.DurDirFileSize)
	}
	if c.ResumeMode != "" && c.ResumeMode != ResumeModeExplicit && c.ResumeMode != ResumeModeImplicit {
		return fmt.Errorf("invalid resume_mode %q: must be %q or %q", c.ResumeMode, ResumeModeExplicit, ResumeModeImplicit)
	}
	if c.DurDirReadMode != "" && c.DurDirReadMode != ReadModeData && c.DurDirReadMode != ReadModeDigest {
		return fmt.Errorf("invalid durdir_read_mode %q: must be %q or %q", c.DurDirReadMode, ReadModeData, ReadModeDigest)
	}
	// MemTarget is passed to glutton verbatim, which owns the parse; an
	// invalid value fails loudly there as a GluttonFillRAM error.
	return nil
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
	if p.DurDirFileSize != nil {
		out.DurDirFileSize = int64(*p.DurDirFileSize)
	}
	if p.ResumeMode != nil {
		out.ResumeMode = *p.ResumeMode
	}
	if p.DurDirReadMode != nil {
		out.DurDirReadMode = *p.DurDirReadMode
	}
	if p.DurDirTemplate != nil {
		out.DurDirTemplate = *p.DurDirTemplate
	}
	if p.MemTarget != nil {
		out.MemTarget = *p.MemTarget
	}
	return out
}

// StartPoll fetches `url` every `interval` until `ctx` is done, and applies
// each change to `holder` + `sampler`. It returns at once; the loop is in a
// goroutine. An interval of zero or less starts no loop.
//
// SubscribeSpawn below is not sufficient by itself. Locust sends a spawn
// message only when the number of users or the spawn rate changes, thus a
// step of a load shape that changes the sample rate and holds the number of
// users gives no message, and the worker keeps the value of the step before
// it. The sample-rate sweep of benchmarking/observability.md is one such
// shape: each of its steps holds 10 users.
//
// `onError` gets each failed fetch. A caller must not exit the process there,
// as it does for a spawn: the worker holds the last good value, and one
// failed poll of a long run is not a reason to lose the run.
func StartPoll(
	ctx context.Context,
	url string,
	holder *Holder,
	sampler ProbabilityUpdater,
	interval, fetchTimeout time.Duration,
	onError func(error),
) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
				next, err := Fetch(fetchCtx, url, holder.Load())
				cancel()
				if err != nil {
					// The end of the run stops a fetch that is in
					// progress. That is not a failed fetch, thus it must
					// not go to onError.
					if ctx.Err() != nil {
						return
					}
					onError(err)
					continue
				}
				// Only a change goes to the log. A poll of each few seconds
				// for the length of a soak makes a log that hides the run.
				if next == holder.Load() {
					continue
				}
				holder.Store(next)
				sampler.UpdateProbability(next.TraceProbability)
				slog.Info("dynconfig applied",
					slog.String("trigger", "poll"),
					slog.Float64("trace_probability", next.TraceProbability),
					slog.Duration("min_wait", next.MinWait),
					slog.Duration("max_wait", next.MaxWait),
					slog.Int64("durdir_file_size_bytes", next.DurDirFileSize),
					slog.String("resume_mode", next.ResumeMode),
					slog.String("durdir_read_mode", next.DurDirReadMode),
					slog.String("durdir_template", next.DurDirTemplate),
					slog.String("mem_target", next.MemTarget),
				)
			}
		}
	}()
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
			slog.Int64("durdir_file_size_bytes", next.DurDirFileSize),
			slog.String("resume_mode", next.ResumeMode),
			slog.String("durdir_read_mode", next.DurDirReadMode),
			slog.String("durdir_template", next.DurDirTemplate),
			slog.String("mem_target", next.MemTarget),
		)
	})
}
