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

// Package predictor estimates how much of their declared limits the actors on
// a worker will use, which is what worker-level overcommit books against the
// Worker instead of the limits themselves.
package predictor

import (
	"errors"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// ErrNotImplemented is returned by a Strategy that cannot predict yet.
var ErrNotImplemented = errors.New("overcommit predictor strategy is not implemented")

// ActorUsageSample is one actor's measured usage at a point in time.
type ActorUsageSample struct {
	ActorUID string
	// StartedAt is when the actor started on this worker.
	StartedAt time.Time
	// Limits are the actor's declared limits.
	Limits *ateapi.Resources
	// Usage is what the actor is measured to use.
	Usage *ateapi.Resources
	// HasCPURate reports whether Usage holds a CPU rate. The first sample of
	// a cgroup has no earlier one to compute a rate from.
	HasCPURate bool
}

// GroupUsageSnapshot is the usage of every actor on a worker at one moment.
type GroupUsageSnapshot struct {
	Timestamp time.Time
	// Actors is keyed by actor UID.
	Actors map[string]ActorUsageSample
}

// Strategy predicts the resources a group of actors needs.
type Strategy interface {
	// Name identifies the strategy in flags and logs.
	Name() string
	// Predict returns the resources the actors in snapshot are expected to
	// need, as of now.
	Predict(now time.Time, snapshot GroupUsageSnapshot) (*ateapi.Resources, error)
}

// WindowedMaxPredictor predicts from the maximum usage observed over a
// sliding history window. An actor younger than the maturation window has
// too little history and is predicted at its limits.
type WindowedMaxPredictor struct {
	maturationWindow time.Duration
	historyWindow    time.Duration
}

var _ Strategy = (*WindowedMaxPredictor)(nil)

// NewWindowedMaxPredictor returns a WindowedMaxPredictor that trusts an actor's
// usage once it has run for maturationWindow and remembers historyWindow of
// samples.
func NewWindowedMaxPredictor(maturationWindow, historyWindow time.Duration) *WindowedMaxPredictor {
	return &WindowedMaxPredictor{
		maturationWindow: maturationWindow,
		historyWindow:    historyWindow,
	}
}

// Name returns "windowed-max".
func (p *WindowedMaxPredictor) Name() string {
	return "windowed-max"
}

// Predict always returns ErrNotImplemented.
func (p *WindowedMaxPredictor) Predict(now time.Time, snapshot GroupUsageSnapshot) (*ateapi.Resources, error) {
	return nil, ErrNotImplemented
}
