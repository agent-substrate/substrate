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

package apiconfig

import (
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateconfigpb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestLoaderReloadsAfterThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		configFile := filepath.Join(t.TempDir(), "config.textproto")
		writeFile := func(content string) {
			t.Helper()
			if err := os.WriteFile(configFile, []byte(content), 0o600); err != nil {
				t.Fatalf("while writing config file: %v", err)
			}
		}

		writeFile(`logging { level: "info" }`)

		l, err := NewLoader(configFile)
		if err != nil {
			t.Fatalf("NewLoader: %v", err)
		}

		wantFirst := &ateconfigpb.APIConfig{
			Logging: &ateconfigpb.APILogging{Level: "info"},
		}
		gotFirst, firstHash := l.Config(t.Context())
		if diff := cmp.Diff(wantFirst, gotFirst, protocmp.Transform()); diff != "" {
			t.Fatalf("Initial config mismatch (-want +got):\n%s", diff)
		}

		writeFile(`logging { level: "debug" }`)

		// Before the reload threshold, the loader keeps serving the cached config.
		gotCached, cachedHash := l.Config(t.Context())
		if diff := cmp.Diff(wantFirst, gotCached, protocmp.Transform()); diff != "" {
			t.Errorf("Config changed before reload threshold (-want +got):\n%s", diff)
		}
		if cachedHash != firstHash {
			t.Errorf("Hash changed before reload threshold: got %x, want %x", cachedHash, firstHash)
		}

		time.Sleep(time.Minute + time.Second)

		wantReloaded := &ateconfigpb.APIConfig{
			Logging: &ateconfigpb.APILogging{Level: "debug"},
		}
		gotReloaded, reloadedHash := l.Config(t.Context())
		if diff := cmp.Diff(wantReloaded, gotReloaded, protocmp.Transform()); diff != "" {
			t.Errorf("Config not reloaded after threshold (-want +got):\n%s", diff)
		}
		if reloadedHash == firstHash {
			t.Errorf("Hash unchanged after reload threshold: %x", reloadedHash)
		}
	})
}
