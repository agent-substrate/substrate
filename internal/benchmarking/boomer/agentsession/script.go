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

// This file is the whole benchmark, spelled out: one coding-agent session as
// a script of steps. Each step names what the agent is doing in plain
// English and lists the resource operations the real action would cost the
// sandbox. Nothing is hidden in the driver — to understand or change the
// workload, edit this table.
//
// Between steps the agent is "waiting for the LLM to think": the driver
// suspends the actor, sleeps the step's think time, and lets the NEXT step's
// first request wake the actor through the atenet router (request parking).
// That idle gap is where Substrate earns its keep, so the think times are
// first-class script data, not driver noise.

// op is one resource effect inside a step, executed as a single glutton RPC
// through the router. Build them with the constructors below so the script
// stays declarative and greppable.
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

func ingest(key string, bytes int64) op { return op{kind: opIngest, key: key, bytes: bytes} }
func burn(millis int64, parallel int32) op {
	return op{kind: opBurnCPU, millis: millis, parallel: parallel}
}
func writeDisk(key string, bytes int64) op { return op{kind: opWriteDisk, key: key, bytes: bytes} }
func readDigest(key string) op             { return op{kind: opReadDiskDigest, key: key} }
func readData(key string) op               { return op{kind: opReadDiskData, key: key} }
func fillRAM(key string, bytes int64) op   { return op{kind: opFillRAM, key: key, bytes: bytes} }
func churnRAM(key string, bytes int64) op  { return op{kind: opChurnRAM, key: key, bytes: bytes} }
func walkRAM(key string) op                { return op{kind: opWalkRAM, key: key} }
func ping() op                             { return op{kind: opPing} }

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

const (
	_   = iota
	kib = int64(1) << (10 * iota)
	mib
)

// Sandbox object names, so the script reads like a filesystem.
const (
	repoBlob     = "repo_tarball"    // the cloned repository
	depsBlob     = "deps_cache"      // downloaded dependency archives
	buildOut     = "build_artifacts" // compiler output
	patchFile    = "patch"           // edits arriving from the LLM
	packageBlob  = "release_package" // the final packaged artifact
	contextRAM   = "agent_context"   // the agent process's resident working set
	compilerRAM  = "compiler_ws"     // build-time working set
	testDataBlob = "test_fixtures"   // fixtures written by the test steps
)

// Session is the default 20-step coding-agent session. The script itself
// declares ≈96Mi of resident RAM (contextRAM 32Mi + compilerRAM 64Mi) and
// ≈110Mi of files; the guest peak on top of that (kernel, kata-agent, Go
// allocator transients) has been observed around 320Mi, which is why the
// template calls for 1Gi actors. TestSessionBudgets bounds only the
// script-declared bytes, so edits that grow the working set fail the test
// and force the memory guidance to be revisited.
//
// The narrative: the agent is told to fetch a repository, get it building,
// fix a failing test, extend the test suite, refactor, and package the
// result — with an LLM round-trip (seconds of suspension) before every step.
func Session() []Step {
	return []Step{
		{
			Name:  "01_read_task",
			Agent: "Boots, reads the task prompt, loads its context window",
			Think: 2 * time.Second,
			Ops: []op{
				fillRAM(contextRAM, 32*mib), // the agent's resident working set for the whole session
				ping(),
			},
		},
		{
			Name:  "02_clone_repo",
			Agent: "git clone of the target repository",
			Think: 3 * time.Second,
			Ops: []op{
				ingest(repoBlob, 16*mib), // tarball arrives over the network, lands on disk
				burn(500, 1),             // checkout: decompress + write tree
			},
		},
		{
			Name:  "03_explore_tree",
			Agent: "Lists directories, greps for entry points",
			Think: 4 * time.Second,
			Ops: []op{
				readDigest(repoBlob), // walk the repo bytes on disk
				burn(200, 1),         // grep-ish scanning
			},
		},
		{
			Name:  "04_read_key_files",
			Agent: "Opens the files it plans to change; they enter the LLM context",
			Think: 3 * time.Second,
			Ops: []op{
				readData(repoBlob),          // file contents also travel back out to the 'LLM'
				churnRAM(contextRAM, 8*mib), // context window grows/changes
			},
		},
		{
			Name:  "05_install_deps",
			Agent: "Installs dependencies (pip install / go mod download)",
			Think: 2 * time.Second,
			Ops: []op{
				ingest(depsBlob, 32*mib), // packages arrive over the network
				burn(1500, 2),            // unpack, byte-compile, link
			},
		},
		{
			Name:  "06_first_build",
			Agent: "First full build of the project",
			Think: 3 * time.Second,
			Ops: []op{
				fillRAM(compilerRAM, 64*mib), // compiler working set
				burn(3000, 2),                // the compile itself
				writeDisk(buildOut, 24*mib),  // object files and binaries
			},
		},
		{
			Name:  "07_run_unit_tests",
			Agent: "Runs the existing unit tests; one fails",
			Think: 2 * time.Second,
			Ops: []op{
				readDigest(buildOut), // load test binaries
				burn(2500, 2),        // the test run
			},
		},
		{
			Name:  "08_reason_about_failure",
			Agent: "Re-reads the failing test and traces the bug (mostly thinking)",
			Think: 8 * time.Second, // the long LLM analysis turn
			Ops: []op{
				walkRAM(contextRAM), // re-touch the whole context after the long suspend
				ping(),
			},
		},
		{
			Name:  "09_edit_source",
			Agent: "Applies the LLM's fix to the source files",
			Think: 5 * time.Second,
			Ops: []op{
				ingest(patchFile, 64*kib),  // the patch arrives from the LLM
				writeDisk(repoBlob, 4*mib), // rewrite the touched sources
			},
		},
		{
			Name:  "10_incremental_build",
			Agent: "Rebuilds just the changed packages",
			Think: 2 * time.Second,
			Ops: []op{
				burn(1200, 2),
				writeDisk(buildOut, 8*mib),
			},
		},
		{
			Name:  "11_rerun_failed_test",
			Agent: "Re-runs the previously failing test; it passes",
			Think: 2 * time.Second,
			Ops: []op{
				burn(800, 1),
			},
		},
		{
			Name:  "12_write_new_tests",
			Agent: "Writes regression tests for the fix",
			Think: 6 * time.Second, // LLM authors the test file
			Ops: []op{
				ingest(patchFile, 128*kib),
				writeDisk(testDataBlob, 2*mib), // fixtures the new tests need
			},
		},
		{
			Name:  "13_run_new_tests",
			Agent: "Runs the new tests in isolation",
			Think: 2 * time.Second,
			Ops: []op{
				readDigest(testDataBlob),
				burn(1000, 1),
			},
		},
		{
			Name:  "14_full_test_suite",
			Agent: "Runs the entire test suite to check for regressions",
			Think: 3 * time.Second,
			Ops: []op{
				burn(4000, 2), // the longest compute step in the session
				readDigest(buildOut),
				churnRAM(contextRAM, 8*mib), // test output enters the context
			},
		},
		{
			Name:  "15_lint_format",
			Agent: "Runs the linter and formatter over the tree",
			Think: 2 * time.Second,
			Ops: []op{
				readDigest(repoBlob),
				burn(900, 1),
			},
		},
		{
			Name:  "16_refactor",
			Agent: "Applies the LLM's multi-file cleanup refactor",
			Think: 8 * time.Second, // LLM plans the refactor
			Ops: []op{
				ingest(patchFile, 512*kib),
				writeDisk(repoBlob, 6*mib),
				burn(600, 1),
			},
		},
		{
			Name:  "17_rebuild",
			Agent: "Full rebuild after the refactor",
			Think: 2 * time.Second,
			Ops: []op{
				churnRAM(compilerRAM, 32*mib), // compiler working set changes with the new tree
				burn(2000, 2),
				writeDisk(buildOut, 16*mib),
			},
		},
		{
			Name:  "18_final_test_suite",
			Agent: "Final full-suite run before shipping",
			Think: 2 * time.Second,
			Ops: []op{
				burn(3500, 2),
			},
		},
		{
			Name:  "19_package_artifact",
			Agent: "Builds the release package / container image",
			Think: 3 * time.Second,
			Ops: []op{
				readDigest(buildOut),
				burn(1000, 1),
				writeDisk(packageBlob, 24*mib),
			},
		},
		{
			Name:  "20_commit_and_summarize",
			Agent: "Commits, writes the summary, hands the session back",
			Think: 5 * time.Second, // LLM writes the summary
			Ops: []op{
				writeDisk(repoBlob, 256*kib), // the commit
				readData(packageBlob),        // artifact ships back out over the network
				walkRAM(contextRAM),          // final context read
			},
		},
	}
}
