// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package autoscaler holds the WorkerPool autoscaling control logic. This file
// is the pure decision core: given a pool's declarative bounds and a live
// observation it computes the next desired replica count. It has no Kubernetes
// or gRPC dependencies so it can be unit-tested exhaustively; the reconciler
// (see reconciler.go) supplies the observation and persists the small amount of
// timing state Step needs.
package autoscaler

import "time"

// Bounds are the declarative autoscaling inputs from WorkerPoolSpec. A nil
// pointer means the operator left the field unset.
type Bounds struct {
	MinReady     *int32
	TargetBuffer *int32
	MaxReplicas  *int32
}

// Enabled reports whether autoscaling is configured for the pool. With neither
// minReady nor targetBuffer set, the autoscaler leaves the pool alone — replicas
// stay whatever a human (or some other tool) set on the scale subresource.
func (b Bounds) Enabled() bool {
	return b.MinReady != nil || b.TargetBuffer != nil
}

// Observation is the measured live state of a pool at decision time.
type Observation struct {
	// Current is spec.replicas the autoscaler last set (the desired pod count).
	Current int32
	// Free is the number of registered workers with no actor assigned (warm/idle).
	Free int32
	// InFlight is pods requested but not yet registered as workers (still starting).
	InFlight int32
}

// Config tunes the loop's deliberate up/down asymmetry.
type Config struct {
	// ScaleDownStabilization is how long a shrink must be continuously wanted
	// before it is applied. Scale-up ignores it.
	ScaleDownStabilization time.Duration
	// MaxScaleUpStep caps how many replicas a single up-step may add above the
	// reservation floor. Zero means unlimited. The floor itself (MinReady) is
	// always reached in one step regardless of this cap.
	MaxScaleUpStep int32
}

// Decision is the loop's output for one tick.
type Decision struct {
	// Target is the replica count to write. It equals Observation.Current when
	// Changed is false.
	Target  int32
	Changed bool
	Reason  string
}

// clamp constrains v to [MinReady, MaxReplicas] (and to a non-negative value).
// MinReady can only raise v; MaxReplicas can only lower it. Admission already
// guarantees MinReady <= MaxReplicas (CEL rule on WorkerPoolSpec).
func clamp(v int32, b Bounds) int32 {
	if v < 0 {
		v = 0
	}
	if b.MinReady != nil && v < *b.MinReady {
		v = *b.MinReady
	}
	if b.MaxReplicas != nil && v > *b.MaxReplicas {
		v = *b.MaxReplicas
	}
	return v
}

// ideal is the instantaneous target that keeps Free ≈ TargetBuffer:
//
//	ideal = Current + TargetBuffer - (Free + InFlight)
//
// i.e. add the buffer deficit (when idle headroom is short) or subtract the
// surplus (when it is over-stocked), then clamp to [MinReady, MaxReplicas].
// Netting against InFlight is the anti-windup term: pods already starting count
// toward the buffer, so the loop does not pile on scale-ups while they boot.
//
// With TargetBuffer unset there is no buffer goal, so the ideal is just Current
// clamped to the bounds — which still lets MinReady raise an under-floored pool
// and MaxReplicas cap an over-sized one.
func ideal(b Bounds, o Observation) int32 {
	target := o.Current
	if b.TargetBuffer != nil {
		target = o.Current + *b.TargetBuffer - (o.Free + o.InFlight)
	}
	return clamp(target, b)
}

// Step computes the next Decision. now is the current time; downWantedSince is
// the instant the pool first became eligible to scale down, or the zero Time if
// it is not currently eligible. Both are state the caller persists between
// ticks. Step returns the decision and the updated downWantedSince to carry
// into the next call.
//
// The asymmetry encodes the design's core constraint:
//   - scale UP is latency-critical, so it is applied immediately (capped by
//     MaxScaleUpStep for buffer-driven growth, but never throttled below the
//     reservation floor);
//   - scale DOWN is safety-critical, so it is applied only after the shrink has
//     been wanted continuously for ScaleDownStabilization — any tick that no
//     longer wants to shrink resets the timer;
//   - the target is always within [MinReady, MaxReplicas].
func Step(b Bounds, o Observation, c Config, now, downWantedSince time.Time) (Decision, time.Time) {
	target := ideal(b, o)

	switch {
	case target > o.Current:
		// Scale up now. Cap buffer-driven growth, but re-clamp so the floor is
		// still reached in a single step.
		next := target
		if c.MaxScaleUpStep > 0 && next-o.Current > c.MaxScaleUpStep {
			next = clamp(o.Current+c.MaxScaleUpStep, b)
		}
		if next <= o.Current {
			return Decision{Target: o.Current, Reason: "steady"}, time.Time{}
		}
		return Decision{Target: next, Changed: true, Reason: "scale up: refill buffer"}, time.Time{}

	case target < o.Current:
		// Want to shrink: hold until the desire has persisted long enough.
		if downWantedSince.IsZero() {
			downWantedSince = now
		}
		if now.Sub(downWantedSince) >= c.ScaleDownStabilization {
			return Decision{Target: target, Changed: true, Reason: "scale down: surplus buffer"}, time.Time{}
		}
		return Decision{Target: o.Current, Reason: "scale down pending stabilization"}, downWantedSince

	default:
		return Decision{Target: o.Current, Reason: "steady"}, time.Time{}
	}
}
