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
	"os"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func TestResolveDelays(t *testing.T) {
	got, err := resolveDelays(time.Second, -1, 2*time.Second, 0, -1, -1)
	if err != nil {
		t.Fatalf("resolveDelays: %v", err)
	}
	want := delays{run: time.Second, restore: 2 * time.Second, checkpoint: 0, promoteSnapshot: time.Second, terminate: time.Second}
	if got != want {
		t.Errorf("resolveDelays = %+v, want %+v", got, want)
	}
	if _, err := resolveDelays(-time.Second, -1, -1, -1, -1, -1); err == nil {
		t.Error("resolveDelays accepted a negative default")
	}
}

// A benchmark tunes fake-atelet through FAKE_ATELET_* variables; a flag on
// the command line wins over its variable, and a bad value is an error.
func TestApplyTuningEnv(t *testing.T) {
	newFlags := func() (*pflag.FlagSet, *time.Duration, *time.Duration, *bool) {
		fs := pflag.NewFlagSet("fake-atelet", pflag.ContinueOnError)
		d := fs.Duration("delay", 0, "")
		run := fs.Duration("delay-run", -1, "")
		for _, name := range []string{"delay-restore", "delay-checkpoint", "delay-promote-snapshot", "delay-terminate"} {
			fs.Duration(name, -1, "")
		}
		mint := fs.Bool("mint-actor-certificate", true, "")
		fs.String("port", "", "")
		return fs, d, run, mint
	}
	env := func(vars map[string]string) func(string) (string, bool) {
		return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
	}

	fs, d, run, mint := newFlags()
	if err := fs.Parse([]string{"--delay-run=3s"}); err != nil {
		t.Fatal(err)
	}
	err := applyTuningEnv(fs, env(map[string]string{
		"FAKE_ATELET_DELAY":                  "500ms",
		"FAKE_ATELET_DELAY_RUN":              "9s",
		"FAKE_ATELET_MINT_ACTOR_CERTIFICATE": "false",
		// Not a tuning flag, so not read.
		"FAKE_ATELET_PORT": "1",
	}))
	if err != nil {
		t.Fatalf("applyTuningEnv() = %v", err)
	}
	if *d != 500*time.Millisecond || *run != 3*time.Second || *mint {
		t.Errorf("delay, delay-run, mint = %v, %v, %v, want 500ms from the environment, 3s from the flag, false", *d, *run, *mint)
	}
	if fs.Changed("port") {
		t.Error("FAKE_ATELET_PORT set --port, which is not a tuning flag")
	}

	fs, _, _, _ = newFlags()
	if err := applyTuningEnv(fs, env(map[string]string{"FAKE_ATELET_DELAY": "fast"})); err == nil {
		t.Error("applyTuningEnv() accepted FAKE_ATELET_DELAY=fast")
	}
}

// Every tuning flag is one fake-atelet registers, so applyTuningEnv can set
// it rather than failing startup.
func TestTuningFlagsRegistered(t *testing.T) {
	for _, name := range tuningFlags {
		if pflag.CommandLine.Lookup(name) == nil {
			t.Errorf("tuning flag %q is not a fake-atelet flag", name)
		}
	}
}

// The benchmark automation's isolate keys set exactly the tuning variables.
// fake-atelet ignores a variable it does not know, so a key left behind by a
// rename would tune nothing without an error.
func TestIsolateTuningMatchesAutomation(t *testing.T) {
	const automation = "../../../../benchmarking/automation/util.py"
	src, err := os.ReadFile(automation)
	if err != nil {
		t.Fatalf("reading %s: %v", automation, err)
	}
	block := regexp.MustCompile(`(?s)ISOLATE_TUNING = \{(.*?)\}`).FindSubmatch(src)
	if block == nil {
		t.Fatalf("%s has no ISOLATE_TUNING map", automation)
	}
	var got []string
	for _, m := range regexp.MustCompile(`"(FAKE_ATELET_[A-Z_]+)"`).FindAllSubmatch(block[1], -1) {
		got = append(got, string(m[1]))
	}
	var want []string
	for _, name := range tuningFlags {
		want = append(want, tuningEnv(name))
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("%s ISOLATE_TUNING sets %v, want fake-atelet's tuning variables %v", automation, got, want)
	}
}
