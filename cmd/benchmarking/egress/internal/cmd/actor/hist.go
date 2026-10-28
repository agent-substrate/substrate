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
	"fmt"
	"math"
	"slices"
	"strconv"
)

const (
	bucketsPerDecade = 20
	// maxBucketSeconds stops the bounds: the first bound at or past it is the
	// last. Slower samples count in the overflow bucket.
	maxBucketSeconds = 60
)

// mantissas are 10^(j/20) for j = 0..19, to four significant digits.
var mantissas = [bucketsPerDecade]string{
	"1", "1.122", "1.259", "1.413", "1.585", "1.778", "1.995", "2.239", "2.512", "2.818",
	"3.162", "3.548", "3.981", "4.467", "5.012", "5.623", "6.31", "7.079", "7.943", "8.913",
}

// Bounds are the upper bounds, in seconds, of every distribution: 20 per
// decade from 1 µs to 63.1 s. Each is parsed from a decimal string, so every
// build on every architecture holds the same 157 values. Neighboring bounds
// differ by about 12%, the resolution of a quantile read from buckets.
var Bounds = func() []float64 {
	var bounds []float64
	for k := 0; ; k++ {
		b, err := strconv.ParseFloat(fmt.Sprintf("%se%d", mantissas[k%bucketsPerDecade], k/bucketsPerDecade-6), 64)
		if err != nil {
			panic(err)
		}
		bounds = append(bounds, b)
		if b >= maxBucketSeconds {
			return bounds
		}
	}
}()

// Distribution names one of the four distributions of a Stats, the key of
// its Point in Stats.Distributions.
type Distribution string

const (
	Opening Distribution = "opening" // end-to-end time of an endpoint's first request, when it succeeded
	Latency Distribution = "latency" // end-to-end time of later successful requests
	DNS     Distribution = "dns"     // lookup time, for requests that did a lookup
	TLS     Distribution = "tls"     // handshake time, for handshakes that completed
)

// DistributionNames lists the four distributions.
var DistributionNames = []Distribution{Opening, Latency, DNS, TLS}

// Point is one distribution over one window: the fields of an OTel
// HistogramDataPoint that a driver adds across actors. The SDK's type marshals
// to JSON but does not unmarshal, so both sides use this mirror. The zero
// Point is empty.
type Point struct {
	// Bounds are the upper bounds of the finite buckets; BucketCounts has one
	// more entry, the overflow bucket. Counts are per bucket, not cumulative.
	Bounds       []float64 `json:"bounds,omitempty"`
	BucketCounts []uint64  `json:"bucketCounts,omitempty"`
	Count        uint64    `json:"count"`
	Sum          float64   `json:"sum"`
	Min          float64   `json:"min,omitempty"`
	Max          float64   `json:"max,omitempty"`
}

// Merge adds o into p. It refuses, leaving p as it was, a point whose bounds
// are not p's, or whose counts do not fit its bounds.
func (p *Point) Merge(o Point) error {
	if o.Count == 0 {
		return nil
	}
	if len(o.BucketCounts) != len(o.Bounds)+1 {
		return fmt.Errorf("%d bucket counts for %d bounds", len(o.BucketCounts), len(o.Bounds))
	}
	if p.Count == 0 {
		*p = Point{Bounds: slices.Clone(o.Bounds), BucketCounts: slices.Clone(o.BucketCounts), Count: o.Count, Sum: o.Sum, Min: o.Min, Max: o.Max}
		return nil
	}
	if len(p.BucketCounts) != len(p.Bounds)+1 {
		return fmt.Errorf("receiver has %d bucket counts for %d bounds", len(p.BucketCounts), len(p.Bounds))
	}
	if len(p.Bounds) != len(o.Bounds) {
		return fmt.Errorf("%d bounds, want %d", len(o.Bounds), len(p.Bounds))
	}
	for i := range p.Bounds {
		if p.Bounds[i] != o.Bounds[i] {
			return fmt.Errorf("bounds differ at index %d: %v vs %v", i, p.Bounds[i], o.Bounds[i])
		}
	}
	for i := range p.BucketCounts {
		p.BucketCounts[i] += o.BucketCounts[i]
	}
	p.Count += o.Count
	p.Sum += o.Sum
	p.Min = min(p.Min, o.Min)
	p.Max = max(p.Max, o.Max)
	return nil
}

// Quantile returns the upper bound of the bucket holding the q-th sample,
// capped at the largest sample seen. ok is false for an empty point.
func (p Point) Quantile(q float64) (seconds float64, ok bool) {
	if p.Count == 0 {
		return 0, false
	}
	rank := max(uint64(math.Ceil(q*float64(p.Count))), 1)
	var seen uint64
	for i, c := range p.BucketCounts {
		seen += c
		if seen < rank {
			continue
		}
		if i == len(p.Bounds) {
			return p.Max, true // the overflow bucket: only the maximum bounds it
		}
		return min(p.Bounds[i], p.Max), true
	}
	return p.Max, true
}

// Mean returns the average sample, or 0 for an empty point.
func (p Point) Mean() float64 {
	if p.Count == 0 {
		return 0
	}
	return p.Sum / float64(p.Count)
}
