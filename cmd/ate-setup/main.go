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

// Command ate-setup installs and tears down Agent Substrate on a Kubernetes
// cluster. It is the installer; hack/install-ate.sh is a shim that translates
// its historical flags onto these commands. See cmd/ate-setup/commands.md for
// the flag-by-flag mapping and cmd/ate-setup/cli-diff.md for what the
// translation does not cover.
package main

import (
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/cmd"
)

func main() {
	cmd.Execute()
}
