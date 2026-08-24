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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseValid(t *testing.T) {
	jsonBlob := []byte(`{
		"trace_probability": 0.5,
		"min_wait_time": 0.1,
		"max_wait_time": 0.5,
		"durdir_file_size_bytes": 1048576,
		"resume_mode": "explicit",
		"durdir_read_mode": "data",
		"durdir_template": "glutton-durdir-data"
	}`)

	cfg, err := Parse(jsonBlob, Config{})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if cfg.TraceProbability != 0.5 {
		t.Errorf("TraceProbability: got %f, want 0.5", cfg.TraceProbability)
	}
	if cfg.MinWait != 100*time.Millisecond {
		t.Errorf("MinWait: got %v, want 100ms", cfg.MinWait)
	}
	if cfg.MaxWait != 500*time.Millisecond {
		t.Errorf("MaxWait: got %v, want 500ms", cfg.MaxWait)
	}
	if cfg.DurDirFileSize != 1048576 {
		t.Errorf("DurDirFileSize: got %d, want 1048576", cfg.DurDirFileSize)
	}
	if cfg.ResumeMode != ResumeModeExplicit {
		t.Errorf("ResumeMode: got %q, want %q", cfg.ResumeMode, ResumeModeExplicit)
	}
	if cfg.DurDirReadMode != ReadModeData {
		t.Errorf("DurDirReadMode: got %q, want %q", cfg.DurDirReadMode, ReadModeData)
	}
	if cfg.DurDirTemplate != "glutton-durdir-data" {
		t.Errorf("DurDirTemplate: got %q, want glutton-durdir-data", cfg.DurDirTemplate)
	}
}

func TestParseInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{
			name: "negative trace probability",
			json: `{"trace_probability": -0.1}`,
		},
		{
			name: "trace probability > 1.0",
			json: `{"trace_probability": 1.5}`,
		},
		{
			name: "negative min wait",
			json: `{"min_wait_time": -1.0}`,
		},
		{
			name: "negative max wait",
			json: `{"max_wait_time": -1.0}`,
		},
		{
			name: "max wait less than min wait",
			json: `{"min_wait_time": 2.0, "max_wait_time": 1.0}`,
		},
		{
			name: "negative file size",
			json: `{"durdir_file_size_bytes": -100}`,
		},
		{
			name: "file size exceeds 2 GiB",
			json: `{"durdir_file_size_bytes": 2147483648}`,
		},
		{
			name: "invalid resume mode",
			json: `{"resume_mode": "invalid_mode"}`,
		},
		{
			name: "invalid read mode",
			json: `{"durdir_read_mode": "invalid_read"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.json), Config{})
			if err == nil {
				t.Errorf("expected Parse to fail for %s, got nil error", tt.name)
			}
		})
	}
}

func TestFetchValidAndInvalid(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/valid", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"resume_mode": "implicit", "durdir_read_mode": "digest"}`))
	})
	mux.HandleFunc("/invalid", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"resume_mode": "bogus"}`))
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx := context.Background()

	cfg, err := Fetch(ctx, ts.URL+"/valid", Config{})
	if err != nil {
		t.Fatalf("Fetch valid failed: %v", err)
	}
	if cfg.ResumeMode != ResumeModeImplicit || cfg.DurDirReadMode != ReadModeDigest {
		t.Errorf("Fetch valid values mismatch: got %+v", cfg)
	}

	_, err = Fetch(ctx, ts.URL+"/invalid", Config{})
	if err == nil {
		t.Errorf("expected Fetch invalid to fail, got nil")
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

	holder := NewHolder(Config{})
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
	if got := holder.Load().TraceProbability; got != 0.5 {
		t.Fatalf("holder trace_probability = %v, want 0.5", got)
	}

	// A value that does not change must not go to the sampler again.
	_, count := sampler.last()
	time.Sleep(100 * time.Millisecond)
	if _, after := sampler.last(); after != count {
		t.Errorf("sampler got %d more updates with no change", after-count)
	}
}

func TestStartPollStopsWithTheContext(t *testing.T) {
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	StartPoll(ctx, ts.URL, NewHolder(Config{}), &fakeSampler{},
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

// An interval of zero starts no loop.
func TestStartPollZeroInterval(t *testing.T) {
	var hits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer ts.Close()

	StartPoll(context.Background(), ts.URL, NewHolder(Config{}), &fakeSampler{},
		0, time.Second, func(error) {})
	time.Sleep(50 * time.Millisecond)
	if hits.Load() != 0 {
		t.Errorf("StartPoll with interval 0 made %d requests", hits.Load())
	}
}
