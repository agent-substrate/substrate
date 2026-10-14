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

// TestSubstrateVersionPrebuilt covers the version a prebuilt install reports.
// Root points at a directory that is not a checkout, so `git describe` has
// nothing to say and only the image tag can supply it.
func TestSubstrateVersionPrebuilt(t *testing.T) {
	t.Setenv("VERSION", "")

	cases := []struct {
		name   string
		source images.Source
		want   string
	}{{
		name:   "tag is the version",
		source: images.Source{Repo: "example.com/substrate", Tag: "v0.0.0-503-4b3423c0"},
		want:   "v0.0.0-503-4b3423c0",
	}, {
		name:   "a tag carrying its digest is the version without it",
		source: images.Source{Repo: "example.com/substrate", Tag: "v0.0.0@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
		want:   "v0.0.0",
	}, {
		name:   "built from source falls back to git",
		source: images.Source{},
		want:   "dev",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{Root: t.TempDir(), Images: tc.source}}
			if got := e.SubstrateVersion(); got != tc.want {
				t.Fatalf("SubstrateVersion = %q, want %q", got, tc.want)
			}
		})
	}
}
