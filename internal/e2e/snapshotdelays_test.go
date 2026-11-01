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

package e2e

import (
	"testing"
	"time"
)

func TestShortSnapshotDelaysDecision(t *testing.T) {
	kind := []string{"--log-level=debug", "--snapshot-upload-delay=2s", "--snapshot-detach-delay=30s"}
	defaults := []string{"--log-level=debug"}
	tests := []struct {
		name     string
		args     []string
		required bool
		wantSkip bool
		wantFail bool
	}{
		{name: "short delays run", args: kind},
		{name: "short delays run in CI", args: kind, required: true},
		{name: "default delays skip", args: defaults, wantSkip: true},
		{name: "default delays fail in CI", args: defaults, required: true, wantFail: true},
		{name: "a long delay skips", args: []string{"--snapshot-upload-delay=2s", "--snapshot-detach-delay=30m"}, wantSkip: true},
		{name: "an unparsable delay fails", args: []string{"--snapshot-detach-delay=soon"}, wantFail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skip, fail := shortSnapshotDelaysDecision(tt.args, 30*time.Second, tt.required)
			if (skip != "") != tt.wantSkip || (fail != "") != tt.wantFail {
				t.Errorf("shortSnapshotDelaysDecision(%q, required=%t) = skip %q, fail %q; want skip %t, fail %t",
					tt.args, tt.required, skip, fail, tt.wantSkip, tt.wantFail)
			}
		})
	}
}
