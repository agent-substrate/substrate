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

package actor

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func newTestRecorder(t *testing.T) *recorder {
	t.Helper()
	r, err := newRecorder()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.shutdown(context.Background()) })
	return r
}

func collectOf(t *testing.T, r *recorder) map[Distribution]Point {
	t.Helper()
	got, err := r.collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCollectIsOneWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newTestRecorder(t)
	for _, ms := range []float64{1, 2, 3} {
		r.record(ctx, Latency, (time.Duration(ms) * time.Millisecond).Seconds())
	}
	first := collectOf(t, r)
	if p := first[Latency]; p.Count != 3 || p.Min != 0.001 || p.Max != 0.003 {
		t.Errorf("first window = count %d min %v max %v, want 3, 0.001, 0.003", p.Count, p.Min, p.Max)
	}
	r.record(ctx, Latency, 0.5)
	second := collectOf(t, r)
	if p := second[Latency]; p.Count != 1 || p.Min != 0.5 || p.Max != 0.5 {
		t.Errorf("second window = count %d min %v max %v, want only the 0.5 s sample", p.Count, p.Min, p.Max)
	}
	if third := collectOf(t, r); len(third) != 0 {
		t.Errorf("third window has data with nothing recorded: %+v", third)
	}
}

func TestIdleDistributionsAreAbsent(t *testing.T) {
	t.Parallel()
	r := newTestRecorder(t)
	if got := collectOf(t, r); len(got) != 0 {
		t.Errorf("fresh recorder collected %v, want nothing", got)
	}
	r.record(context.Background(), DNS, 0.002)
	got := collectOf(t, r)
	for _, name := range DistributionNames {
		if _, ok := got[name]; ok != (name == DNS) {
			t.Errorf("%s present = %v after one dns sample, want %v", name, ok, name == DNS)
		}
	}
}

func TestOneSampleLandsInOneBucket(t *testing.T) {
	t.Parallel()
	r := newTestRecorder(t)
	r.record(context.Background(), Opening, time.Millisecond.Seconds())
	got := collectOf(t, r)
	p := got[Opening]
	// 1 ms is a bound: the sample goes in the bucket whose bound is 0.001,
	// not the one above it.
	if len(p.BucketCounts) != len(Bounds)+1 || p.BucketCounts[60] != 1 || p.Count != 1 || p.Sum != 0.001 {
		t.Errorf("1 ms sample: %d buckets, bucket 60 = %d, count %d, sum %v; want %d, 1, 1, 0.001", len(p.BucketCounts), p.BucketCounts[60], p.Count, p.Sum, len(Bounds)+1)
	}
	if !reflect.DeepEqual(p.Bounds, Bounds) {
		t.Errorf("point carries other bounds: %v", p.Bounds)
	}
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var back map[Distribution]Point
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, got) {
		t.Errorf("JSON round trip = %+v, want %+v", back, got)
	}
}

func TestRecordAfterShutdownDoesNotPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r, err := newRecorder()
	if err != nil {
		t.Fatal(err)
	}
	r.record(ctx, TLS, 0.01)
	if err := r.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	r.record(ctx, TLS, 0.01)
	if _, err := r.collect(ctx); err == nil {
		t.Errorf("Collect after Shutdown returned no error")
	}
}

func BenchmarkRecord(b *testing.B) {
	r, err := newRecorder()
	if err != nil {
		b.Fatal(err)
	}
	defer r.shutdown(context.Background())
	ctx := context.Background()
	for i := 0; b.Loop(); i++ {
		r.record(ctx, Latency, float64(i%1000)*1e-5)
	}
}

func BenchmarkCollect(b *testing.B) {
	r, err := newRecorder()
	if err != nil {
		b.Fatal(err)
	}
	defer r.shutdown(context.Background())
	ctx := context.Background()
	for b.Loop() {
		b.StopTimer()
		for _, name := range DistributionNames {
			for i := range 64 {
				r.record(ctx, name, float64(1+i)*1e-3)
			}
		}
		b.StartTimer()
		if _, err := r.collect(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
