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

package main

import (
	"fmt"
	"time"
)

// resolveDelays applies each per-call override over the default delay. A
// negative override means "use the default".
func resolveDelays(def, run, restore, checkpoint, uploadPausedCheckpoint, terminate time.Duration) (delays, error) {
	if def < 0 {
		return delays{}, fmt.Errorf("delay must not be negative, got %v", def)
	}
	pick := func(d time.Duration) time.Duration {
		if d < 0 {
			return def
		}
		return d
	}
	return delays{
		run:                    pick(run),
		restore:                pick(restore),
		checkpoint:             pick(checkpoint),
		uploadPausedCheckpoint: pick(uploadPausedCheckpoint),
		terminate:              pick(terminate),
	}, nil
}
