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

func TestWithAgentDebug(t *testing.T) {
	got := WithAgentDebug("root=/dev/vda1")
	if !strings.Contains(got, "agent.log=debug") {
		t.Errorf("WithAgentDebug did not append agent.log=debug: %q", got)
	}
	// Idempotent: a second call must not append agent.log again.
	if again := WithAgentDebug(got); again != got {
		t.Errorf("WithAgentDebug not idempotent:\n first = %q\nsecond = %q", got, again)
	}
}
