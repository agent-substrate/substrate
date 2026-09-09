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

package steps

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// The benchmark and micro-VM stacks are still driven by shell. They orchestrate
// several other scripts (image builds, asset assembly, object-store staging)
// that are out of scope for this command, so ate-setup shells out to them with
// a translated argument list rather than reimplementing them.
const (
	deployLocustScript      = "benchmarking/deploy_locust.sh"
	installMicrovmDepScript = "hack/install-microvm-deps.sh"
)

// BenchmarkOptions shapes the benchmark WorkerPool.
type BenchmarkOptions struct {
	// WorkerCount is the number of WorkerPool replicas.
	WorkerCount int
	// SandboxClass is the sandbox runtime: gvisor or microvm.
	SandboxClass string
}

// Validate checks the options the shell script would otherwise reject after
// having already started work.
func (o BenchmarkOptions) Validate() error {
	if o.WorkerCount < 1 {
		return fmt.Errorf("--worker-count must be at least 1, got %d", o.WorkerCount)
	}
	switch o.SandboxClass {
	case config.SandboxClassGvisor, config.SandboxClassMicrovm:
		return nil
	default:
		return fmt.Errorf("--sandbox-class must be %s or %s, got %q",
			config.SandboxClassGvisor, config.SandboxClassMicrovm, o.SandboxClass)
	}
}

// DeployBenchmarks installs the benchmark workloads and the locust load test
// stack.
func (e *Env) DeployBenchmarks(ctx context.Context, opts BenchmarkOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}
	log.Stepf("deploy_benchmarks (worker_count=%d, sandbox_class=%s)", opts.WorkerCount, opts.SandboxClass)

	// The microvm SandboxConfig lives outside the default set installed by
	// `deploy ate-system`, which only installs gvisor-default. The workloads
	// deploy references it by name and would fail if this were skipped.
	if opts.SandboxClass == config.SandboxClassMicrovm {
		if err := e.runScript(ctx, installMicrovmDepScript, "--install"); err != nil {
			return err
		}
	}
	return e.runScript(ctx, deployLocustScript, deployLocustArgs(opts, e.Cfg.OtlpEndpoint, e.Cfg.BenchmarkActorMemory)...)
}

// deployLocustArgs builds the deploy_locust.sh argument list.
//
// The script reads these only as flags, never from the environment, so they
// have to be passed explicitly. Both trailing flags are omitted when unset:
// deploy_locust.sh rejects an empty --otlp-endpoint outright, and an empty
// --actor-memory would override the workload default with nothing.
func deployLocustArgs(opts BenchmarkOptions, otlpEndpoint, actorMemory string) []string {
	args := []string{
		"--deploy",
		"--worker-count", strconv.Itoa(opts.WorkerCount),
		"--sandbox-class", opts.SandboxClass,
	}
	// Send the actor telemetry to the same place as the control plane telemetry.
	if otlpEndpoint != "" {
		args = append(args, "--otlp-endpoint", otlpEndpoint)
	}
	if actorMemory != "" {
		args = append(args, "--actor-memory", actorMemory)
	}
	return args
}

// DeleteBenchmarks removes the locust stack and the benchmark workloads.
func (e *Env) DeleteBenchmarks(ctx context.Context, opts BenchmarkOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}
	log.Stepf("delete_benchmarks (sandbox_class=%s)", opts.SandboxClass)

	if err := e.runScript(ctx, deployLocustScript, "--delete"); err != nil {
		return err
	}
	// Only tear down the microvm SandboxConfig if the caller opted into
	// microvm: it is cluster-wide and may be in use by something else.
	if opts.SandboxClass == config.SandboxClassMicrovm {
		return e.runScript(ctx, installMicrovmDepScript, "--delete")
	}
	return nil
}

// runScript executes a repository script from the repository root with the
// environment the shell installer would have given it.
func (e *Env) runScript(ctx context.Context, relPath string, args ...string) error {
	cmd := exec.CommandContext(ctx, e.Cfg.Path(relPath), args...)
	cmd.Dir = e.Cfg.Root
	cmd.Env = e.Cfg.ScriptEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running %s: %w", relPath, err)
	}
	return nil
}
