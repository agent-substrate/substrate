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

// Package all links every bundled demo into the binary.
//
// Import it for its side effects; each demo package registers itself with the
// demos package from init. This is the one place that lists them, so a demo
// that is added but not linked in shows up here rather than as a subcommand
// that silently went missing.
package all

import (
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/autoscaledworkerpool"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/claudemultiplex"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/counter"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/egress"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/multitemplate"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/parking"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/sandbox"
)
