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

package egresstunnels

import (
	"fmt"
	"math"
	"slices"
	"time"
)

const (
	bucketsPerDecade = 20
	// maxBucketMicros is the largest finite bucket bound, 60s. Slower samples
	// land in the overflow bucket.
	maxBucketMicros = 60_000_000
)

// bucketBounds holds the inclusive upper bound, in microseconds, of each
// finite bucket: 10^(k/20) for k = 0, 1, ... up to the first bound at or past
// maxBucketMicros. Neighboring bounds differ by about 12%, which is the
// resolution of every quantile read from a Histogram.
var bucketBounds = func() []float64 {
	var bounds []float64
	for k := 0; ; k++ {
		b := math.Pow(10, float64(k)/bucketsPerDecade)
		bounds = append(bounds, b)
		if b >= maxBucketMicros {
			return bounds
		}
	}
}()

// Histogram counts durations in fixed log-spaced buckets. Every Histogram
// shares the same buckets, so histograms from many actors merge exactly.
// The zero value is empty and ready to use.
type Histogram struct {
	// Counts has one entry per finite bucket plus a final overflow bucket.
	// Nil until the first sample.
	Counts    []int64 `json:"counts,omitempty"`
	Count     int64   `json:"count"`
	SumMicros int64   `json:"sumMicros"`
	MaxMicros int64   `json:"maxMicros"`
}

// Record adds one sample.
func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 0 {
		us = 0
	}
	if h.Counts == nil {
		h.Counts = make([]int64, len(bucketBounds)+1)
	}
	// The first bound at or above the sample; len(bucketBounds) is overflow.
	i, _ := slices.BinarySearch(bucketBounds, float64(us))
	h.Counts[i]++
	h.Count++
	h.SumMicros += us
	h.MaxMicros = max(h.MaxMicros, us)
}

// Merge adds every sample of o to h. It refuses, leaving h as it was, a
// histogram whose buckets are not this package's: one from an actor built
// with other bucket bounds, or one whose counts were dropped in transit.
func (h *Histogram) Merge(o Histogram) error {
	if o.Count == 0 && o.Counts == nil {
		return nil
	}
	if err := checkBuckets(o); err != nil {
		return err
	}
	if h.Counts == nil {
		if h.Count > 0 {
			return checkBuckets(*h)
		}
		h.Counts = make([]int64, len(bucketBounds)+1)
	} else if err := checkBuckets(*h); err != nil {
		return err
	}
	for i, c := range o.Counts {
		h.Counts[i] += c
	}
	h.Count += o.Count
	h.SumMicros += o.SumMicros
	h.MaxMicros = max(h.MaxMicros, o.MaxMicros)
	return nil
}

// checkBuckets reports whether h's buckets are this package's.
func checkBuckets(h Histogram) error {
	if len(h.Counts) != len(bucketBounds)+1 {
		return fmt.Errorf("histogram has %d buckets, want %d", len(h.Counts), len(bucketBounds)+1)
	}
	return nil
}

// Quantile returns the upper bound of the bucket holding the q-th sample,
// capped at the largest sample seen. It returns 0 for an empty histogram.
func (h *Histogram) Quantile(q float64) time.Duration {
	if h.Count == 0 {
		return 0
	}
	rank := int64(math.Ceil(q * float64(h.Count)))
	rank = min(max(rank, 1), h.Count)
	var seen int64
	for i, c := range h.Counts {
		seen += c
		if seen < rank {
			continue
		}
		if i == len(bucketBounds) {
			break
		}
		return time.Duration(min(int64(math.Ceil(bucketBounds[i])), h.MaxMicros)) * time.Microsecond
	}
	return time.Duration(h.MaxMicros) * time.Microsecond
}

// Mean returns the average sample, or 0 for an empty histogram.
func (h *Histogram) Mean() time.Duration {
	if h.Count == 0 {
		return 0
	}
	return time.Duration(h.SumMicros/h.Count) * time.Microsecond
}

// Max returns the largest sample.
func (h *Histogram) Max() time.Duration {
	return time.Duration(h.MaxMicros) * time.Microsecond
}

func (h *Histogram) clone() Histogram {
	c := *h
	c.Counts = slices.Clone(h.Counts)
	return c
}
