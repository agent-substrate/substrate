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

package dynconfig

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/myzhan/boomer"
)

// knobs is the kind of struct a user class declares: its slice of the
// payload with its own defaults and rules.
type knobs struct {
	WaitTime
	Lifecycle
	Template string `json:"test_template"`
	Workers  int    `json:"test_workers"`
}

var knobsCodec = Typed[knobs]{
	Defaults: knobs{Template: "stock", Workers: 1},
	Validate: func(k knobs) error {
		if err := k.WaitTime.Validate(); err != nil {
			return err
		}
		if err := k.Lifecycle.Validate(); err != nil {
			return err
		}
		if k.Workers < 1 {
			return fmt.Errorf("test_workers must be positive: %d", k.Workers)
		}
		return nil
	},
}

func mustApply(t *testing.T, h *Holder, payload string) bool {
	t.Helper()
	_, changed, err := h.Apply([]byte(payload))
	if err != nil {
		t.Fatalf("Apply(%s): %v", payload, err)
	}
	return changed
}

// A payload stands on its own: it sets the keys it carries, puts the ones
// it nulls or omits back at the class's defaults, converts seconds to
// durations, and ignores keys the class does not name.
func TestApplyDecodesOverDefaults(t *testing.T) {
	h := NewHolder(knobsCodec)
	if got := Get[knobs](h); got != knobsCodec.Defaults {
		t.Fatalf("fresh holder = %+v, want the defaults", got)
	}
	mustApply(t, h, `{
		"min_wait_time": 0.5, "max_wait_time": 2,
		"resume_mode": "implicit",
		"test_workers": 3,
		"someone_elses_key": "ignored"
	}`)
	want := knobs{
		WaitTime:  WaitTime{MinWait: Seconds(500 * time.Millisecond), MaxWait: Seconds(2 * time.Second)},
		Lifecycle: Lifecycle{ResumeMode: ResumeModeImplicit},
		Template:  "stock",
		Workers:   3,
	}
	if got := Get[knobs](h); got != want {
		t.Errorf("after first payload =\n %+v, want\n %+v", got, want)
	}
	// A cleared form field comes as null, and a key the master does not
	// know is absent; both are back at the default, not at the last value.
	mustApply(t, h, `{"test_template": "big", "test_workers": null, "max_wait_time": null}`)
	want = knobsCodec.Defaults
	want.Template = "big"
	if got := Get[knobs](h); got != want {
		t.Errorf("after nulls and omissions =\n %+v, want\n %+v", got, want)
	}
	if changed := mustApply(t, h, `{"test_template": "big"}`); changed {
		t.Error("a payload that changes nothing reported a change")
	}
	if changed := mustApply(t, h, `{}`); !changed {
		t.Error("an empty object did not report the return to the defaults")
	}
	if got := Get[knobs](h); got != knobsCodec.Defaults {
		t.Errorf("after an empty object = %+v, want the defaults", got)
	}
}

// Apply hands back the snapshot the payload left in the holder, so a log
// line written from it cannot show a concurrent Apply's values.
func TestApplyReturnsWhatItStored(t *testing.T) {
	h := NewHolder(knobsCodec)
	applied, changed, err := h.Apply([]byte(`{"test_workers": 4, "trace_probability": 0.5}`))
	if err != nil || !changed {
		t.Fatalf("Apply = changed %v, err %v; want a change", changed, err)
	}
	if applied.Class != h.Load() || applied.Common != h.Common() {
		t.Errorf("Apply returned %+v, holder has %+v / %+v", applied, h.Load(), h.Common())
	}
	if got := applied.Class.(knobs).Workers; got != 4 {
		t.Errorf("applied test_workers = %d, want 4", got)
	}
}

// A payload the class's rules or Common refuse leaves the holder as it was,
// so a bad value from the master cannot take a run down. An empty body is
// one such payload: a 200 with nothing in it must not read as every
// default.
func TestApplyRefuses(t *testing.T) {
	h := NewHolder(knobsCodec)
	mustApply(t, h, `{"test_workers": 2, "trace_probability": 0.5}`)
	before := Get[knobs](h)
	for name, payload := range map[string]string{
		"class rule":  `{"test_workers": 0}`,
		"embedded":    `{"max_wait_time": 1, "min_wait_time": 2}`,
		"mode":        `{"lifecycle_mode": "hibernate"}`,
		"wrong type":  `{"test_workers": "three"}`,
		"bad seconds": `{"min_wait_time": "soon"}`,
		"common rule": `{"trace_probability": 1.5}`,
		"not json":    `<html>`,
		"empty body":  ``,
	} {
		t.Run(name, func(t *testing.T) {
			_, changed, err := h.Apply([]byte(payload))
			if err == nil || changed {
				t.Fatalf("Apply(%s) = changed %v, err %v; want a refusal", payload, changed, err)
			}
			if got := Get[knobs](h); got != before {
				t.Errorf("a refused payload changed the config to %+v", got)
			}
			if got := h.Common().TraceProbability; got != 0.5 {
				t.Errorf("a refused payload changed trace_probability to %v", got)
			}
		})
	}
}

// Get on the wrong type is a programming error that must not pass
// silently; a nil holder is the zero value, for classes that run without
// one in tests.
func TestGetTypeChecks(t *testing.T) {
	if got := Get[knobs](nil); got != (knobs{}) {
		t.Errorf("Get on a nil holder = %+v, want zero", got)
	}
	defer func() {
		if recover() == nil {
			t.Error("Get with the wrong type did not panic")
		}
	}()
	Get[Common](NewHolder(knobsCodec))
}

func TestStaticAndNilCodec(t *testing.T) {
	h := Static(knobs{Template: "t", WaitTime: WaitTime{MaxWait: Seconds(3 * time.Second)}})
	if got := Get[knobs](h); got.Template != "t" || got.MaxWait != Seconds(3*time.Second) {
		t.Errorf("Static round trip = %+v", got)
	}
	// A class without knobs still gets Common.
	h = NewHolder(nil)
	mustApply(t, h, `{"trace_probability": 0.25, "anything": 1}`)
	if got := h.Common().TraceProbability; got != 0.25 {
		t.Errorf("trace_probability = %v, want 0.25", got)
	}
}

// A config printed with %+v, as the dynconfig applied log line does, shows
// its durations as durations, not as counts of nanoseconds.
func TestSecondsPrintsAsDuration(t *testing.T) {
	line := fmt.Sprintf("%+v", knobs{WaitTime: WaitTime{MaxWait: Seconds(500 * time.Millisecond)}})
	if !strings.Contains(line, "MaxWait:500ms") {
		t.Errorf("printed config %q, want MaxWait:500ms in it", line)
	}
}

func TestWaitTimeDraw(t *testing.T) {
	window := WaitTime{MinWait: Seconds(10 * time.Millisecond), MaxWait: Seconds(50 * time.Millisecond)}
	for i := 0; i < 100; i++ {
		if got := window.Draw(); got < 10*time.Millisecond || got > 50*time.Millisecond {
			t.Fatalf("Draw = %v, outside the window", got)
		}
	}
	inverted := WaitTime{MinWait: Seconds(100 * time.Millisecond), MaxWait: Seconds(50 * time.Millisecond)}
	if got := inverted.Draw(); got != 100*time.Millisecond {
		t.Errorf("inverted Draw = %v, want the lower bound", got)
	}
	if got := Uniform(0, 0); got != 0 {
		t.Errorf("Uniform(0, 0) = %v, want 0", got)
	}
	if got := Uniform(3*time.Second, 3*time.Second); got != 3*time.Second {
		t.Errorf("Uniform(3s, 3s) = %v, want 3s", got)
	}
}

func TestFetchReportsErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/valid", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"resume_mode": "implicit"}`))
	})
	mux.HandleFunc("/busy", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	body, err := Fetch(context.Background(), ts.URL+"/valid")
	if err != nil || string(body) != `{"resume_mode": "implicit"}` {
		t.Errorf("Fetch valid = %q, %v", body, err)
	}
	if _, err := Fetch(context.Background(), ts.URL+"/busy"); err == nil {
		t.Error("Fetch of a 503 returned no error")
	}
}

// fakeSampler records each probability that StartPoll applies.
type fakeSampler struct {
	mu   sync.Mutex
	seen []float64
}

func (f *fakeSampler) UpdateProbability(p float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, p)
}

func (f *fakeSampler) last() (float64, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.seen) == 0 {
		return -1, 0
	}
	return f.seen[len(f.seen)-1], len(f.seen)
}

// A step of a load shape can change the sample rate and hold the number of
// users. Locust sends no spawn message for such a step, thus the poll is the
// only way the new rate reaches the worker.
func TestStartPollAppliesAChange(t *testing.T) {
	var probability atomic.Value
	probability.Store(0.0)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"trace_probability": %v}`, probability.Load())
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	holder := NewHolder(knobsCodec)
	sampler := &fakeSampler{}
	StartPoll(ctx, ts.URL, holder, sampler, 10*time.Millisecond, time.Second,
		func(err error) { t.Errorf("unexpected poll error: %v", err) })

	probability.Store(0.5)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got, _ := sampler.last(); got == 0.5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := holder.Common().TraceProbability; got != 0.5 {
		t.Fatalf("holder trace_probability = %v, want 0.5", got)
	}

	// A value that does not change must not go to the sampler again.
	_, count := sampler.last()
	time.Sleep(100 * time.Millisecond)
	if _, after := sampler.last(); after != count {
		t.Errorf("sampler got %d more updates with no change", after-count)
	}
}

// A poll that brings a value the class refuses goes to onError and leaves
// the holder as it was.
func TestStartPollReportsRefusedConfig(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"test_workers": 0}`))
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	holder := NewHolder(knobsCodec)
	errs := make(chan error, 1)
	StartPoll(ctx, ts.URL, holder, &fakeSampler{}, 10*time.Millisecond, time.Second, func(err error) {
		select {
		case errs <- err:
		default:
		}
	})
	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "test_workers") {
			t.Errorf("onError got %v, want the refusing key named", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a refused poll never reached onError")
	}
	if got := Get[knobs](holder); got != knobsCodec.Defaults {
		t.Errorf("a refused poll changed the holder to %+v", got)
	}
}

func TestStartPollStopsWithTheContext(t *testing.T) {
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	StartPoll(ctx, ts.URL, NewHolder(nil), &fakeSampler{},
		10*time.Millisecond, time.Second, func(error) {})
	time.Sleep(60 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	stopped := hits.Load()
	time.Sleep(100 * time.Millisecond)
	if got := hits.Load(); got != stopped {
		t.Errorf("poll continued after the context stopped: %d -> %d", stopped, got)
	}
}

// A spawn fetch that fails after an earlier one succeeded reports
// fetched=true and leaves the last applied value in place, so the caller can
// keep the run going. A failure before any success reports fetched=false.
func TestSubscribeSpawnReportsPriorSuccess(t *testing.T) {
	var fail atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"trace_probability": 0.25}`))
	}))
	defer ts.Close()

	type call struct {
		err     error
		fetched bool
	}
	var calls []call
	holder := NewHolder(nil)
	// boomer.Events is a process-wide bus and SubscribeSpawn owns the handler
	// value, so the subscription outlives this test. No other test publishes
	// boomer:spawn, and each test uses its own holder, so that is harmless.
	if err := SubscribeSpawn(ts.URL, holder, &fakeSampler{}, time.Second, func(err error, fetched bool) {
		calls = append(calls, call{err: err, fetched: fetched})
	}); err != nil {
		t.Fatalf("SubscribeSpawn: %v", err)
	}

	fail.Store(true)
	boomer.Events.Publish("boomer:spawn", 1, 1.0)
	if len(calls) != 1 || calls[0].fetched || calls[0].err == nil {
		t.Fatalf("after a failure with no prior success: calls = %+v, want one error with fetched=false", calls)
	}

	fail.Store(false)
	boomer.Events.Publish("boomer:spawn", 2, 1.0)
	if len(calls) != 1 {
		t.Fatalf("a successful fetch invoked onError: %+v", calls)
	}
	if got := holder.Common().TraceProbability; got != 0.25 {
		t.Fatalf("holder trace_probability = %v, want 0.25", got)
	}

	fail.Store(true)
	boomer.Events.Publish("boomer:spawn", 3, 1.0)
	if len(calls) != 2 || !calls[1].fetched {
		t.Fatalf("after a failure with a prior success: calls = %+v, want a second with fetched=true", calls)
	}
	if got := holder.Common().TraceProbability; got != 0.25 {
		t.Fatalf("failed fetch changed holder trace_probability to %v", got)
	}
}

// An interval of zero starts no loop.
func TestStartPollZeroInterval(t *testing.T) {
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer ts.Close()

	StartPoll(context.Background(), ts.URL, NewHolder(nil), &fakeSampler{},
		0, time.Second, func(error) {})
	time.Sleep(50 * time.Millisecond)
	if hits.Load() != 0 {
		t.Errorf("StartPoll with interval 0 made %d requests", hits.Load())
	}
}
