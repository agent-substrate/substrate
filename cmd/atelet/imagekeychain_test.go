// Copyright 2026 The Agent Substrate Authors
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

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
)

func TestImageKeychain(t *testing.T) {
	dir := t.TempDir()
	// "cm9ib3Q6czNjcmV0" is base64 of "robot:s3cret".
	dockerConfig := `{"auths":{"registry.example.com":{"auth":"cm9ib3Q6czNjcmV0"}}}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(dockerConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", dir)

	resolve := func(t *testing.T, kc authn.Keychain, repo string) authn.AuthConfig {
		t.Helper()
		r, err := name.NewRepository(repo)
		if err != nil {
			t.Fatal(err)
		}
		auth, err := authn.Resolve(context.Background(), kc, r)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", repo, err)
		}
		got, err := auth.Authorization()
		if err != nil {
			t.Fatalf("Authorization(%s): %v", repo, err)
		}
		return *got
	}

	t.Run("no provider reads the Docker config", func(t *testing.T) {
		kc, err := imageKeychain("", "")
		if err != nil {
			t.Fatalf("imageKeychain: %v", err)
		}
		if got := resolve(t, kc, "registry.example.com/team/app"); got.Username != "robot" || got.Password != "s3cret" {
			t.Errorf("listed registry got %q:%q, want robot:s3cret", got.Username, got.Password)
		}
		if got := resolve(t, kc, "other.example.com/team/app"); got != (authn.AuthConfig{}) {
			t.Errorf("unlisted registry got %+v, want anonymous", got)
		}
	})

	t.Run("provider config without a bin dir fails", func(t *testing.T) {
		if _, err := imageKeychain(filepath.Join(dir, "provider.yaml"), ""); err == nil {
			t.Error("imageKeychain accepted a provider config with no bin dir")
		}
	})
}
