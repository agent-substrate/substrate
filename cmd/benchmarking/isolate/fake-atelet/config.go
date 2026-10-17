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
	"strings"
	"time"

	"github.com/spf13/pflag"
)

// tuningFlags are the flags a FAKE_ATELET_<FLAG> environment variable can
// set, as from the fake-atelet-tuning ConfigMap a benchmark writes: the
// installer chooses the fake data plane, and the benchmark tunes it.
var tuningFlags = []string{
	"delay",
	"delay-run",
	"delay-restore",
	"delay-checkpoint",
	"delay-promote-snapshot",
	"delay-terminate",
	"mint-actor-certificate",
}

// tuningEnv is the environment variable that sets the tuning flag name.
func tuningEnv(name string) string {
	return "FAKE_ATELET_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

// applyTuningEnv sets each tuning flag the command line left unset from its
// environment variable, read through lookup. A flag on the command line
// wins.
func applyTuningEnv(fs *pflag.FlagSet, lookup func(string) (string, bool)) error {
	for _, name := range tuningFlags {
		if fs.Changed(name) {
			continue
		}
		value, ok := lookup(tuningEnv(name))
		if !ok {
			continue
		}
		if err := fs.Set(name, value); err != nil {
			return fmt.Errorf("%s=%q: %w", tuningEnv(name), value, err)
		}
	}
	return nil
}

// resolveDelays applies each per-call override over the default delay. A
// negative override means "use the default".
func resolveDelays(def, run, restore, checkpoint, promoteSnapshot, terminate time.Duration) (delays, error) {
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
		run:             pick(run),
		restore:         pick(restore),
		checkpoint:      pick(checkpoint),
		promoteSnapshot: pick(promoteSnapshot),
		terminate:       pick(terminate),
	}, nil
}
