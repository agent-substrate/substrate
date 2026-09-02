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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// TestKoRunnerPrebuilt covers the one resolver that cannot build. A pre-built
// install has no ko runner behind imageResolver, so a step that needs to build
// has to say so rather than dereference what is not there.
func TestKoRunnerPrebuilt(t *testing.T) {
	e := &Env{Cfg: &config.Config{
		Root:   t.TempDir(),
		Images: images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"},
	}}

	runner, err := e.koRunner()
	if err == nil {
		t.Fatalf("koRunner() = %v, nil; want an error under --image-repo", runner)
	}
	// The message has to name the flag: it is the thing the caller passed and
	// the thing they have to drop.
	if !strings.Contains(err.Error(), "--image-repo") {
		t.Errorf("koRunner() error = %q; want it to name --image-repo", err)
	}
}
