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

package kata

import (
	"strings"
	"testing"
)

func TestBaseKernelParamsNoDebugConsole(t *testing.T) {
	for _, forbidden := range []string{"agent.debug_console", "agent.debug_console_vport", "1026"} {
		if strings.Contains(BaseKernelParams, forbidden) {
			t.Errorf("expected BaseKernelParams %q not to contain %q", BaseKernelParams, forbidden)
		}
	}
}

func TestWithAgentDebug(t *testing.T) {
	got := WithAgentDebug(BaseKernelParams)
	for _, want := range []string{
		"agent.log=debug",
		"agent.debug_console",
		"agent.debug_console_vport=1026",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected WithAgentDebug(%q) to contain %q, but got %q", BaseKernelParams, want, got)
		}
	}
	if again := WithAgentDebug(got); again != got {
		t.Errorf("expected WithAgentDebug to be idempotent, but got %q then %q", got, again)
	}
}
