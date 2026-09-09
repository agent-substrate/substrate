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

// Package reaper collects detached child processes in ateom-microvm.
// Synchronous commands must use Run or RunCombined so the reaper cannot consume
// their exit status. Detached commands must use exec.Cmd.Start directly.
package reaper

import (
	"os/exec"

	"github.com/agent-substrate/substrate/internal/childreap"
)

var shared = childreap.New()

// Run runs cmd without allowing the child reaper to consume its exit status.
func Run(cmd *exec.Cmd) error {
	return shared.RunCommand(cmd)
}

// RunCombined is Run for callers that need cmd.CombinedOutput.
func RunCombined(cmd *exec.Cmd) ([]byte, error) {
	return shared.CombinedOutput(cmd)
}
