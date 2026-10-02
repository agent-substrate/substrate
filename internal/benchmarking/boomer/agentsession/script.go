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

package agentsession

import "time"

// A script is the whole benchmark, spelled out: one coding-agent session as
// a sequence of steps. Each step names what the agent is doing in plain
// English and lists the resource operations the real action would cost the
// sandbox. Nothing is hidden in the driver — to understand or change the
// workload, read or edit the YAML under scripts/ (scriptfile.go describes
// the format). This file holds the types a decoded script becomes.
//
// Between steps the agent is "waiting for the LLM to think": the driver
// suspends the actor, sleeps the step's think time, and lets the NEXT step's
// first request wake the actor through the atenet router (request parking).
// That idle gap is where Substrate earns its keep, so the think times are
// first-class script data, not driver noise.

// op is one resource effect inside a step, executed as a single glutton RPC
// through the router. Ops come from a decoded script (scriptfile.go); ping
// is also built directly, as the wake probe ahead of every step.
type op struct {
	kind     opKind
	key      string // file or RAM-array name inside the sandbox
	bytes    int64  // payload / file / RAM size
	millis   int64  // CPU burn wall-clock
	parallel int32  // CPU burn goroutines
}

type opKind int

const (
	// opIngest pushes bytes from the driver through the router into the
	// actor, which writes them to disk: real network ingress + disk write,
	// the shape of a download.
	opIngest opKind = iota
	// opBurnCPU spins the sandbox's CPU for a wall-clock duration.
	opBurnCPU
	// opWriteDisk writes locally generated random bytes to a sandbox file.
	opWriteDisk
	// opReadDiskDigest reads and sha256-hashes a sandbox file without
	// shipping the bytes back: disk read I/O only.
	opReadDiskDigest
	// opReadDiskData reads a sandbox file AND returns its bytes to the
	// driver: disk read + network egress through the router response.
	opReadDiskData
	// opFillRAM allocates a resident RAM array of random bytes.
	opFillRAM
	// opChurnRAM re-randomizes part of an existing RAM array in place,
	// dirtying pages so the next suspend snapshot has fresh content.
	opChurnRAM
	// opWalkRAM touches one byte per page of a RAM array, forcing every
	// page resident — after a resume this measures demand-paging cost.
	opWalkRAM
	// opPing is a minimal round-trip through the router.
	opPing
)

func ping() op { return op{kind: opPing} }

// Step is one agent action: what a coding agent would be doing, the think
// time that precedes it (the LLM producing this step), and the resource
// operations acting it out.
type Step struct {
	// Name keys the step's locust stats row: Step_<Name>.
	Name string
	// Agent says what the coding agent is doing, for humans.
	Agent string
	// Think is how long the LLM "thinks" before this step. The actor is
	// suspended for this gap (scaled by --agentsession-think-scale).
	Think time.Duration
	// Ops are the resource effects, executed in order.
	Ops []op
}
