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

package contract

import (
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// record puts one sample into p the way the SDK buckets it, so a test can
// build a Point without an instrument.
func record(p *Point, seconds float64) {
	if p.Count == 0 {
		p.Bounds = slices.Clone(Bounds)
		p.BucketCounts = make([]uint64, len(Bounds)+1)
		p.Min, p.Max = seconds, seconds
	}
	i, found := slices.BinarySearch(p.Bounds, seconds)
	if !found && i == len(p.Bounds) {
		i = len(p.Bounds) // overflow
	}
	p.BucketCounts[i]++
	p.Count++
	p.Sum += seconds
	p.Min, p.Max = min(p.Min, seconds), max(p.Max, seconds)
}

// pointOf is record over every sample.
func pointOf(samples ...float64) Point {
	var p Point
	for _, s := range samples {
		record(&p, s)
	}
	return p
}

func TestBounds(t *testing.T) {
	t.Parallel()
	if n := len(Bounds); n != 157 {
		t.Fatalf("len(Bounds) = %d, want 157", n)
	}
	if Bounds[0] != 1e-6 || Bounds[60] != 0.001 || Bounds[156] != 63.1 {
		t.Errorf("Bounds[0,60,156] = %v %v %v, want 1e-06 0.001 63.1", Bounds[0], Bounds[60], Bounds[156])
	}
	for i, b := range Bounds {
		if i > 0 && b <= Bounds[i-1] {
			t.Errorf("Bounds[%d] = %v does not exceed Bounds[%d] = %v", i, b, i-1, Bounds[i-1])
		}
		if again, _ := strconv.ParseFloat(strconv.FormatFloat(b, 'g', 4, 64), 64); again != b {
			t.Errorf("Bounds[%d] = %v has more than four significant digits", i, b)
		}
	}
}

func TestPointJSONRoundTrip(t *testing.T) {
	t.Parallel()
	in := map[Distribution]Point{TLS: pointOf(0.006, 0.009), DNS: {}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var back map[Distribution]Point
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, in) {
		t.Errorf("round trip = %+v, want %+v", back, in)
	}
}

func TestPointMergeAndQuantile(t *testing.T) {
	t.Parallel()
	var a, b Point
	for i := 1; i <= 90; i++ {
		record(&a, float64(i)*time.Millisecond.Seconds()) // 1..90 ms
	}
	for range 10 {
		record(&b, 5) // ten slow ones at 5 s
	}
	var sum Point
	for _, p := range []Point{a, b, {}} { // an empty point merges as a no-op
		if err := sum.Merge(p); err != nil {
			t.Fatal(err)
		}
	}
	if sum.Count != 100 || sum.Max != 5 || sum.Min != 0.001 {
		t.Errorf("merged count %d min %v max %v, want 100, 0.001, 5", sum.Count, sum.Min, sum.Max)
	}
	p50, _ := sum.Quantile(0.5)
	p99, _ := sum.Quantile(0.99)
	p999, _ := sum.Quantile(0.999)
	if p50 < 0.045 || p50 > 0.0502 || p99 != 5 || p999 != 5 {
		t.Errorf("p50 %v p99 %v p99.9 %v; want p50 near 45-50 ms, p99 and p99.9 = 5 (the maximum)", p50, p99, p999)
	}
	if mean := sum.Mean(); mean < 0.54 || mean > 0.55 {
		t.Errorf("Mean() = %v, want about 0.5409", mean)
	}
	if (Point{}).Mean() != 0 {
		t.Errorf("Mean() of an empty point = %v, want 0", (Point{}).Mean())
	}
}

func TestPointQuantile(t *testing.T) {
	t.Parallel()
	edge := pointOf(0.001, 0.001, 0.002, 0.002, 0.002) // two in the 1 ms bucket, three in the 2 ms one
	for _, tc := range []struct {
		name   string
		p      Point
		q      float64
		want   float64
		wantOK bool
	}{
		{"empty", Point{}, 0.5, 0, false},
		{"one sample is its own bound", pointOf(0.0015), 0.5, 0.0015, true},
		{"rank on a bucket edge", edge, 0.4, 0.001, true},
		{"rank just past a bucket edge", edge, 0.41, 0.002, true},
		{"q of 0 is the first bucket", edge, 0, 0.001, true},
		{"q of 1 is the last bucket", edge, 1, 0.002, true},
		{"overflow bucket is the maximum", pointOf(0.001, 100), 0.99, 100, true},
		{"capped at the maximum", pointOf(0.001, 0.0021), 1, 0.0021, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := tc.p.Quantile(tc.q)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("Quantile(%v) = %v, %v; want %v, %v", tc.q, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestPointMerge(t *testing.T) {
	t.Parallel()
	// Samples are powers of two, so the sums compare exactly whatever the
	// order of addition.
	full := pointOf(0.25, 0.5)
	other := pointOf(0.125)
	other.Bounds = slices.Clone(other.Bounds)
	other.Bounds[0] = 0.5e-6
	short := pointOf(0.125)
	short.BucketCounts = short.BucketCounts[:len(short.BucketCounts)-1]
	fewer := pointOf(0.125)
	fewer.Bounds, fewer.BucketCounts = fewer.Bounds[:10], fewer.BucketCounts[:11]
	for _, tc := range []struct {
		name    string
		into    Point
		from    Point
		want    Point
		wantErr string // a substring of the error; "" means no error
	}{
		{"empty into empty", Point{}, Point{}, Point{}, ""},
		{"empty into full", full, Point{}, full, ""},
		{"full into empty", Point{}, full, full, ""},
		{"equal bounds", full, pointOf(0.125, 2), pointOf(0.25, 0.5, 0.125, 2), ""},
		{"different bounds refused, naming the index", full, other, full, "bounds differ at index 0: 1e-06 vs 5e-07"},
		{"fewer bounds refused, naming both lengths", full, fewer, full, "10 bounds, want 157"},
		{"counts that do not fit the bounds refused", full, short, full, "157 bucket counts for 157 bounds"},
		{"receiver that does not fit its bounds refused", short, full, short, "receiver has 157 bucket counts for 157 bounds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.into
			got.BucketCounts = slices.Clone(got.BucketCounts)
			err := got.Merge(tc.from)
			if (err != nil) != (tc.wantErr != "") || (err != nil && !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("Merge() error = %v, want %q", err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("after Merge: count %d sum %v min %v max %v, want count %d sum %v min %v max %v",
					got.Count, got.Sum, got.Min, got.Max, tc.want.Count, tc.want.Sum, tc.want.Min, tc.want.Max)
			}
		})
	}
	// A merge into an empty point copies, so the source's buckets are its own.
	var sum Point
	if err := sum.Merge(full); err != nil {
		t.Fatal(err)
	}
	i, _ := slices.BinarySearch(Bounds, 0.25)
	sum.BucketCounts[i]++
	if full.BucketCounts[i] != 1 {
		t.Errorf("merging into an empty point shared the source's buckets")
	}
}
