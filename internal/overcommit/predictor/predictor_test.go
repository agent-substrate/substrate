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

package predictor

import (
	"errors"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestWindowedMaxPredictorName(t *testing.T) {
	if got, want := NewWindowedMaxPredictor(time.Minute, time.Hour).Name(), "windowed-max"; got != want {
		t.Errorf("Name() = %q, want %q", got, want)
	}
}

// The stub must refuse rather than predict zero: a zero prediction would hand
// every actor's resources back to the control plane.
func TestWindowedMaxPredictorNotImplemented(t *testing.T) {
	now := time.Now()
	snapshot := GroupUsageSnapshot{
		Timestamp: now,
		Actors: map[string]ActorUsageSample{
			"uid-a": {
				ActorUID:   "uid-a",
				StartedAt:  now.Add(-time.Hour),
				Limits:     &ateapi.Resources{},
				Usage:      &ateapi.Resources{},
				HasCPURate: true,
			},
		},
	}
	got, err := NewWindowedMaxPredictor(time.Minute, time.Hour).Predict(now, snapshot)
	if !errors.Is(err, ErrNotImplemented) {
		t.Errorf("Predict() error = %v, want ErrNotImplemented", err)
	}
	if got != nil {
		t.Errorf("Predict() = %v, want nil", got)
	}
}
