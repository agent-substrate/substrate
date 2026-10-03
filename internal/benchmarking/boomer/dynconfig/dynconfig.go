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
// settings: the locust flags the operator can change in the web UI form.
// The boomer wire protocol only carries num_users + spawn_rate, so these
// come over an HTTP side channel from the master's /boomer-config endpoint
// (common/boomer_config.py).
//
// The payload is a JSON object keyed by locust flag name in snake_case.
// This package does not know what the keys mean. A worker runs exactly one
// user class, known at launch, so the class hands the Holder its Codec at
// construction: the typed struct it reads its knobs from, with the
// defaults a key left unset falls back to and the rules a fetched value
// must pass. The Holder then stores that typed value, folds each payload
// into it, refuses one the class cannot run on in favor of the last good
// config, and hands the class its config back through Get. Keys the class
// does not name are ignored.
package dynconfig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/myzhan/boomer"
)

// Codec is how a user class's config comes to be: the value a worker
// starts from, and how a payload folds into the current value. Typed is
// the implementation a class declares over its knobs struct.
type Codec interface {
	// Initial is the config before any payload: the class's defaults.
	Initial() any
	// Merge folds update, a JSON object, into current and validates the
	// result. A key absent from update, or null in it, keeps current's
	// value: the master serves every flag it knows, null for the ones the
	// operator left blank.
	Merge(current any, update []byte) (any, error)
}

// Typed is the Codec over a class's knobs struct T, whose json tags name
// the keys it reads. Merge decodes the update over a copy of the current
// value, so untouched keys keep what they had, and runs Validate on the
// result.
type Typed[T any] struct {
	Defaults T
	Validate func(T) error
}

func (c Typed[T]) Initial() any { return c.Defaults }

func (c Typed[T]) Merge(current any, update []byte) (any, error) {
	next, ok := current.(T)
	if !ok {
		return nil, fmt.Errorf("dynconfig: current config is %T, codec expects %T", current, c.Defaults)
	}
	if len(bytes.TrimSpace(update)) > 0 {
		if err := json.Unmarshal(update, &next); err != nil {
			return nil, fmt.Errorf("decode config: %w", err)
		}
	}
	if c.Validate != nil {
		if err := c.Validate(next); err != nil {
			return nil, err
		}
	}
	return next, nil
}

// Common is the slice of the payload the worker itself reads, whatever the
// user class. Every Holder folds it alongside the class's config.
type Common struct {
	TraceProbability float64 `json:"trace_probability"`
}

var commonCodec = Typed[Common]{
	Validate: func(c Common) error {
		if c.TraceProbability < 0 || c.TraceProbability > 1 {
			return fmt.Errorf("trace_probability must be between 0.0 and 1.0, got: %f", c.TraceProbability)
		}
		return nil
	},
}

// Holder holds the running class's config and the worker's Common slice,
// swapped together atomically so task goroutines read a consistent pair.
type Holder struct {
	codec Codec
	v     atomic.Pointer[state]
}

type state struct {
	class  any
	common Common
}

// NewHolder returns a holder at codec's defaults. A nil codec is a class
// that reads no runtime config.
func NewHolder(codec Codec) *Holder {
	if codec == nil {
		codec = Typed[struct{}]{}
	}
	h := &Holder{codec: codec}
	h.v.Store(&state{class: codec.Initial()})
	return h
}

// Static is for tests: a holder fixed at cfg, with no validation.
func Static[T any](cfg T) *Holder {
	return NewHolder(Typed[T]{Defaults: cfg})
}

// Apply folds a payload into the current config. A payload the class's
// codec or Common refuses leaves the holder as it was and comes back as
// the error, naming the key. changed is false when the payload left every
// value as it was, so a caller can keep an unchanged poll out of the log.
func (h *Holder) Apply(update []byte) (changed bool, err error) {
	prev := h.v.Load()
	class, err := h.codec.Merge(prev.class, update)
	if err != nil {
		return false, err
	}
	common, err := commonCodec.Merge(prev.common, update)
	if err != nil {
		return false, err
	}
	next := &state{class: class, common: common.(Common)}
	if next.common == prev.common && reflect.DeepEqual(next.class, prev.class) {
		return false, nil
	}
	h.v.Store(next)
	return true, nil
}

// Load returns the class's current config as the codec's type; Get is the
// typed form.
func (h *Holder) Load() any { return h.v.Load().class }

// Common returns the worker's current Common slice.
func (h *Holder) Common() Common { return h.v.Load().common }

// Get is the class's current config. A nil holder reads as T's zero
// value; a holder built for another type is a programming error and
// panics with both types named.
func Get[T any](h *Holder) T {
	var zero T
	if h == nil {
		return zero
	}
	cfg, ok := h.Load().(T)
	if !ok {
		panic(fmt.Sprintf("dynconfig: holder carries %T, Get asked for %T", h.Load(), zero))
	}
	return cfg
}

// Seconds is a duration carried in the payload as a JSON number of seconds,
// which is how the locust flags spell every time value.
type Seconds time.Duration

// Duration converts to the time package's unit.
func (s Seconds) Duration() time.Duration { return time.Duration(s) }

func (s Seconds) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(s).Seconds())
}

func (s *Seconds) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	var secs float64
	if err := json.Unmarshal(data, &secs); err != nil {
		return fmt.Errorf("seconds: %w", err)
	}
	*s = Seconds(secs * float64(time.Second))
	return nil
}

// WaitTime is the gap between one iteration of a user and the next, drawn
// uniformly from [MinWait, MaxWait]. Embed it in a class's knobs struct
// and call its Validate from the class's.
type WaitTime struct {
	MinWait Seconds `json:"min_wait_time"`
	MaxWait Seconds `json:"max_wait_time"`
}

func (w WaitTime) Validate() error {
	if w.MinWait < 0 {
		return fmt.Errorf("min_wait_time cannot be negative: %v", w.MinWait.Duration())
	}
	if w.MaxWait < 0 {
		return fmt.Errorf("max_wait_time cannot be negative: %v", w.MaxWait.Duration())
	}
	if w.MaxWait < w.MinWait {
		return fmt.Errorf("max_wait_time (%v) cannot be less than min_wait_time (%v)", w.MaxWait.Duration(), w.MinWait.Duration())
	}
	return nil
}

// Draw returns a wait from [MinWait, MaxWait]; an inverted or empty range
// yields MinWait.
func (w WaitTime) Draw() time.Duration {
	return Uniform(w.MinWait.Duration(), w.MaxWait.Duration())
}

// Uniform draws from [lo, hi]; an inverted or empty range yields lo.
func Uniform(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Float64()*float64(hi-lo))
}

// Resume and lifecycle modes. Explicit issues a ResumeActor RPC before
// sending traffic; implicit issues no wake request at all: the actor stays
// suspended until a request reaches the atenet router, which wakes it
// while the request is parked. Suspend writes the snapshot to durable
// storage; pause keeps it on the node.
const (
	ResumeModeExplicit = "explicit"
	ResumeModeImplicit = "implicit"

	LifecycleModeSuspend = "suspend"
	LifecycleModePause   = "pause"
)

// Lifecycle is how a class takes its actors off a worker and brings them
// back. The empty string is each class's own default; see the class.
type Lifecycle struct {
	ResumeMode    string `json:"resume_mode"`
	LifecycleMode string `json:"lifecycle_mode"`
}

func (l Lifecycle) Validate() error {
	if l.ResumeMode != "" && l.ResumeMode != ResumeModeExplicit && l.ResumeMode != ResumeModeImplicit {
		return fmt.Errorf("invalid resume_mode %q: must be %q or %q", l.ResumeMode, ResumeModeExplicit, ResumeModeImplicit)
	}
	if l.LifecycleMode != "" && l.LifecycleMode != LifecycleModeSuspend && l.LifecycleMode != LifecycleModePause {
		return fmt.Errorf("invalid lifecycle_mode %q: must be %q or %q", l.LifecycleMode, LifecycleModeSuspend, LifecycleModePause)
	}
	return nil
}

// Fetch GETs url and returns the payload it serves.
func Fetch(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", url, err)
	}
	return body, nil
}

// ProbabilityUpdater is the subset of trace.UpdatableSampler we touch here;
// kept as an interface so this package doesn't depend on the trace package.
type ProbabilityUpdater interface {
	UpdateProbability(p float64)
}

// fetchAndApply fetches url into holder and, on a change, pushes the trace
// probability to the sampler and logs the config now in force. The error
// is a failed fetch or the holder's refusal, with the key it names.
func fetchAndApply(ctx context.Context, trigger, url string, holder *Holder, sampler ProbabilityUpdater) error {
	body, err := Fetch(ctx, url)
	if err != nil {
		return err
	}
	changed, err := holder.Apply(body)
	if err != nil {
		return fmt.Errorf("%s: %w", url, err)
	}
	if !changed {
		return nil
	}
	sampler.UpdateProbability(holder.Common().TraceProbability)
	slog.Info("dynconfig applied",
		slog.String("trigger", trigger),
		slog.Float64("trace_probability", holder.Common().TraceProbability),
		slog.String("config", fmt.Sprintf("%+v", holder.Load())))
	return nil
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
// `onError` gets each failed fetch and each refused payload. A caller must
// not exit the process there, as it does for a spawn: the worker holds the
// last good value, and one failed poll of a long run is not a reason to
// lose the run. Only a change goes to the log: a poll of each few seconds
// for the length of a soak would otherwise make a log that hides the run.
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
				err := fetchAndApply(fetchCtx, "poll", url, holder, sampler)
				cancel()
				if err != nil {
					// The end of the run stops a fetch that is in
					// progress. That is not a failed fetch, thus it must
					// not go to onError.
					if ctx.Err() != nil {
						return
					}
					onError(err)
				}
			}
		}
	}()
}

// SubscribeSpawn registers a boomer Events handler that fetches `url` on
// each spawn message and applies the result to `holder` + `sampler`. Locust
// sends a spawn message for every ramp step, so a long ramp fetches once per
// second. `onError` is invoked when a fetch fails or the holder refuses the
// payload, with `fetched` true if an earlier spawn fetch was applied: the
// holder then still has a value from the master, and the caller can keep
// running on it. With `fetched` false the worker has only its command-line
// values, and callers typically exit. Returns an error if the event
// subscription itself fails (handler signature mismatch), which is a
// programmer error and should be treated as fatal too.
func SubscribeSpawn(url string, holder *Holder, sampler ProbabilityUpdater, fetchTimeout time.Duration, onError func(err error, fetched bool)) error {
	var fetched atomic.Bool
	return boomer.Events.Subscribe("boomer:spawn", func(spawnCount int, spawnRate float64) {
		ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout)
		defer cancel()
		if err := fetchAndApply(ctx, "spawn", url, holder, sampler); err != nil {
			onError(err, fetched.Load())
			return
		}
		fetched.Store(true)
	})
}
