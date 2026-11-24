// Copyright 2026 The Agent Substrate Authors
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

package actorlog

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/ateattr"
)

func TestEmitContainerExit(t *testing.T) {
	code := func(c int32) *int32 { return &c }
	tests := []struct {
		name     string
		exitCode *int32
		wantCode string // "" means the key is absent
	}{
		{name: "failure", exitCode: code(1), wantCode: "1"},
		// 0 is a real exit code: the record still says the container is gone.
		{name: "clean exit", exitCode: code(0), wantCode: "0"},
		{name: "code above 128", exitCode: code(139), wantCode: "139"},
		{name: "no exit code"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			al := NewActorLogger(&buf, false)
			al.EmitContainerExit(testAttribution, ContainerExit{Container: testContainer, ExitCode: tc.exitCode})

			// kubectl ate logs reads a line with the container label as that
			// container's output, and skips it under --source=lifecycle.
			if strings.Contains(buf.String(), `"`+containerNameLabel+`"`) {
				t.Errorf("record carries the container label: %s", buf.String())
			}
			m := decodeLine(t, &buf)
			if m["message"] != ContainerExitedMessage || m["level"] != "WARN" {
				t.Errorf("message, level = %v, %v, want %q, WARN", m["message"], m["level"], ContainerExitedMessage)
			}
			wantLabels := identityLabels()
			delete(wantLabels, containerNameLabel)
			assertLabels(t, labelGroup(t, al, m), wantLabels)
			if got := m[string(ateattr.ActorExitedContainerNameKey)]; got != testContainer {
				t.Errorf("exited container = %v, want %q", got, testContainer)
			}
			got, ok := m[string(ateattr.ActorExitedContainerExitCodeKey)]
			if tc.wantCode == "" {
				if ok {
					t.Errorf("exit code = %v, want it absent", got)
				}
				return
			}
			if fmt.Sprint(got) != tc.wantCode {
				t.Errorf("exit code = %v, want %s", got, tc.wantCode)
			}
		})
	}
}
