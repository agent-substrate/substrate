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

package steps

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
)

// Only a source build of the envoy dataplane runs docker buildx, so only that
// configuration asks for the plugin.
func TestRequireEnvoyDataplaneBuild(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cfg       config.Config
		wantCheck bool
	}{
		{name: "source build with envoy", cfg: config.Config{Router: config.RouterEnvoy}, wantCheck: true},
		{name: "pre-built images", cfg: config.Config{Router: config.RouterEnvoy, Images: images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"}}},
		{name: "agentgateway dataplane", cfg: config.Config{Router: config.RouterAgentgateway}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A docker without the buildx plugin, recording whether it ran.
			dir := t.TempDir()
			calls := filepath.Join(dir, "calls")
			script := fmt.Sprintf("#!/bin/sh\necho \"$*\" >> %q\necho \"docker: unknown command: docker $1\" >&2\nexit 1\n", calls)
			if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

			cfg := tc.cfg
			err := (&Env{Cfg: &cfg}).requireEnvoyDataplaneBuild(t.Context())
			if tc.wantCheck {
				if err == nil || !strings.Contains(err.Error(), "docker buildx plugin") {
					t.Errorf("requireEnvoyDataplaneBuild() = %v, want an error naming the docker buildx plugin", err)
				}
			} else if err != nil {
				t.Errorf("requireEnvoyDataplaneBuild() = %v, want nil", err)
			}
			_, statErr := os.Stat(calls)
			if ran := statErr == nil; ran != tc.wantCheck {
				t.Errorf("docker ran = %t, want %t", ran, tc.wantCheck)
			}
		})
	}
}
