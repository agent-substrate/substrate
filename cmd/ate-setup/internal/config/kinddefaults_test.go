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

package config

import (
	"os"
	"runtime"
	"testing"
)

// kindDefaults are the values a Kind install fills in for settings nothing
// else supplied. applyKindDefaultsResolved writes them onto Config.
var kindDefaults = map[string]string{
	"context":             "kind-kind",
	"ko.dockerRepo":       "localhost:5001",
	"storage.bucketName":  "ate-snapshots",
	"ko.defaultPlatforms": "linux/" + runtimeGOARCH(),
}

// The report has to show the values the install uses. A Kind install fills in
// four settings that nothing else supplied, and they reach Config without
// reaching Resolved -- so the report says "" (from default) for a value the
// install is about to use.
func TestKindDefaultsReachTheReport(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_INSTALL_KIND", "true")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	r := cfg.Resolved()

	for key, want := range kindDefaults {
		t.Run(key, func(t *testing.T) {
			if got := r.String(key); got != want {
				t.Errorf("the report shows %s = %q, but the install uses %q", key, got, want)
			}
			if !r.Supplied(key) {
				t.Errorf("Supplied(%s) = false; a Kind default is a value the install uses, "+
					"and the report marks it as nothing having set it", key)
			}
		})
	}
}

// The record is written from the settings the report lists, so a Kind install
// whose defaults never reach Resolved records none of them. Replaying such a
// record reproduces nothing: the context, registry, platforms and bucket are
// all absent.
func TestKindInstallRecordsWhatItUsed(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_INSTALL_KIND", "true")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	path, _, err := RecordSuccess(t.TempDir(), cfg.Context, cfg.Resolved())
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	f, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile() error = %v", err)
	}
	for key, want := range kindDefaults {
		got, ok := f.Get(key)
		if !ok {
			t.Errorf("the record does not name %s, which the install used as %q:\n%s", key, want, raw)
			continue
		}
		if got != want {
			t.Errorf("the record has %s = %q, want %q", key, got, want)
		}
	}
}

// A replayed Kind record has to resolve to the install it recorded. It does,
// but not because the record carries the values: the record holds only
// kindCluster.enabled, and replaying it re-enters the same default block,
// which derives the four values again.
//
// That makes the round-trip hold only while the defaults stay put. A record
// is meant to pin an install against a later release that ships different
// defaults, and this one cannot: there is nothing in it to pin with. The test
// passes today and is here to fail if the derivation and the record ever
// disagree.
func TestKindRecordReplaysToTheSameInstall(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_INSTALL_KIND", "true")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	path, _, err := RecordSuccess(t.TempDir(), cfg.Context, cfg.Resolved())
	if err != nil {
		t.Fatalf("RecordSuccess() error = %v", err)
	}

	// Replay the record alone, with nothing in the environment.
	loadEnv(t)
	t.Setenv(ConfigPathEnv, path)
	replayed, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() from the record error = %v", err)
	}

	for _, tc := range []struct{ name, got, want string }{
		{"Context", replayed.Context, cfg.Context},
		{"KODockerRepo", replayed.KODockerRepo, cfg.KODockerRepo},
		{"KODefaultPlatforms", replayed.KODefaultPlatforms, cfg.KODefaultPlatforms},
		{"BucketName", replayed.BucketName, cfg.BucketName},
	} {
		if tc.got != tc.want {
			t.Errorf("replaying the record gives %s = %q, the run it recorded used %q",
				tc.name, tc.got, tc.want)
		}
	}
}

// runtimeGOARCH is the architecture applyKindDefaultsResolved builds the
// platform string from.
func runtimeGOARCH() string { return runtime.GOARCH }
