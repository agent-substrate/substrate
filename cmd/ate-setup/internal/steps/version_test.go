// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// TestSubstrateVersionPrebuilt covers the version a prebuilt install stamps.
// Root points at a directory that is not a checkout, so `git describe` has
// nothing to say and only the image tag can supply it.
func TestSubstrateVersionPrebuilt(t *testing.T) {
	t.Setenv("VERSION", "")

	cases := []struct {
		name       string
		source     images.Source
		want       string
		wantSuffix string
	}{{
		name:       "tag is the version",
		source:     images.Source{Repo: "example.com/substrate", Tag: "v0.0.0-503-4b3423c0"},
		want:       "v0.0.0-503-4b3423c0",
		wantSuffix: "v0-0-0-503-4b3423c0",
	}, {
		name:       "built from source falls back to git",
		source:     images.Source{},
		want:       "dev",
		wantSuffix: "dev",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{Root: t.TempDir(), Images: tc.source}}
			got, suffix, err := e.SubstrateVersion()
			if err != nil {
				t.Fatalf("SubstrateVersion: %v", err)
			}
			if got != tc.want || suffix != tc.wantSuffix {
				t.Fatalf("SubstrateVersion = %q, %q; want %q, %q", got, suffix, tc.want, tc.wantSuffix)
			}
		})
	}
}

func TestSubstituteVersion(t *testing.T) {
	e := &Env{substrateVersion: "v1.2.3", substrateVersionSuffix: "v1-2-3"}
	in := "" +
		"metadata:\n" +
		"  name: atelet-${SUBSTRATE_VERSION_SUFFIX}\n" +
		"  labels:\n" +
		"    ate.dev/substrate-version: ${SUBSTRATE_VERSION}\n" +
		"nodeSelector:\n" +
		"  ate.dev/substrate-version: \"${SUBSTRATE_VERSION}\"\n"
	// The unquoted scalar is re-quoted (an all-digit version must land as a
	// YAML string); already-quoted values and name suffixes pass through.
	want := "" +
		"metadata:\n" +
		"  name: atelet-v1-2-3\n" +
		"  labels:\n" +
		"    ate.dev/substrate-version: \"v1.2.3\"\n" +
		"nodeSelector:\n" +
		"  ate.dev/substrate-version: \"v1.2.3\"\n"

	got, err := e.SubstituteVersion([]byte(in))
	if err != nil {
		t.Fatalf("SubstituteVersion: %v", err)
	}
	if string(got) != want {
		t.Fatalf("SubstituteVersion mismatch:\ngot:\n%s\nwant:\n%s", got, want)
	}
}
