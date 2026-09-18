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

package riptide

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/imagestreaming"
)

func TestRiptide_Registration(t *testing.T) {
	ctx := context.Background()
	streamer, err := imagestreaming.Get(ctx, ProviderName, imagestreaming.Config{})
	if err != nil {
		t.Fatalf("imagestreaming.Get(%q) error = %v", ProviderName, err)
	}
	if streamer.Name() != ProviderName {
		t.Errorf("streamer.Name() = %q, want %q", streamer.Name(), ProviderName)
	}
}
