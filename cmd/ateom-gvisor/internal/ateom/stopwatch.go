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

package ateom

import (
	"log/slog"
	"time"
)

type Lap struct {
	Name          string
	Time          time.Time
	StepDuration  time.Duration
	TotalDuration time.Duration
}

type Stopwatch struct {
	Laps []Lap
}

var _ slog.LogValuer = (*Stopwatch)(nil)

func (s *Stopwatch) Click(name string) {
	now := time.Now()
	lap := Lap{
		Name: name,
		Time: now,
	}
	if len(s.Laps) > 0 {
		lap.StepDuration = now.Sub(s.Laps[len(s.Laps)-1].Time)
		lap.TotalDuration = now.Sub(s.Laps[0].Time)
	}
	s.Laps = append(s.Laps, lap)
}

type FormattedLap struct {
	Name  string
	Step  string
	Total string
}

func (s *Stopwatch) LogValue() slog.Value {
	formatted := []FormattedLap{}
	for _, l := range s.Laps {
		formatted = append(formatted, FormattedLap{
			Name:  l.Name,
			Step:  l.StepDuration.String(),
			Total: l.TotalDuration.String(),
		})
	}
	return slog.AnyValue(formatted)
}
