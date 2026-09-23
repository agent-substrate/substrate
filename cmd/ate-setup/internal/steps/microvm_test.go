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
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// fakeMicrovmScript installs a stand-in for hack/install-microvm-deps.sh in a
// throwaway repository root and returns an Env pointing at it, plus a function
// reading back the lines it recorded.
//
// The script is the whole contract these steps have with the shell -- the
// exact flag and the directory it runs in -- so the test drives the real
// exec path rather than a seam around it. Only shell builtins are used: the
// script inherits Config.ScriptEnv(), which carries no PATH.
func fakeMicrovmScript(t *testing.T, exitCode string) (*Env, func() []string) {
	t.Helper()

	root := t.TempDir()
	record := filepath.Join(root, "record.txt")
	script := filepath.Join(root, installMicrovmDepScript)
	if err := os.MkdirAll(filepath.Dir(script), 0o755); err != nil {
		t.Fatalf("creating the script directory: %v", err)
	}
	body := "#!/bin/sh\n{ pwd; printf '%s\\n' \"$@\"; } > " + record + "\nexit " + exitCode + "\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("writing the fake script: %v", err)
	}

	// The steps announce themselves on the installer's log; tests have no use
	// for it.
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stdout) })

	return &Env{Cfg: &config.Config{Root: root}}, func() []string {
		t.Helper()
		got, err := os.ReadFile(record)
		if err != nil {
			t.Fatalf("the script did not run: %v", err)
		}
		lines := strings.Split(strings.TrimSuffix(string(got), "\n"), "\n")
		// macOS resolves TempDir through /private; compare what the shell saw
		// against the same resolution.
		if resolved, err := filepath.EvalSymlinks(root); err == nil {
			lines[0] = strings.Replace(lines[0], resolved, root, 1)
		}
		return lines
	}
}

// Getting the flag wrong is the whole failure mode here: --install and
// --delete are opposites, and the script takes nothing else.
func TestMicroVMDepsStepsPassTheRightFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Env) error
		want string
	}{
		{"deploy", func(e *Env) error { return e.DeployMicroVMDeps(t.Context()) }, "--install"},
		{"delete", func(e *Env) error { return e.DeleteMicroVMDeps(t.Context()) }, "--delete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, recorded := fakeMicrovmScript(t, "0")

			if err := tc.run(env); err != nil {
				t.Fatalf("running the step: %v", err)
			}

			got := recorded()
			// The script resolves manifests/ and hack/ relative to the
			// directory it starts in, so running it from anywhere but the
			// repository root would stage the wrong thing.
			if got[0] != env.Cfg.Root {
				t.Errorf("the script ran in %q, want the repository root %q", got[0], env.Cfg.Root)
			}
			if args := got[1:]; !slices.Equal(args, []string{tc.want}) {
				t.Errorf("the script got %v, want [%s]", args, tc.want)
			}
		})
	}
}

// A failed staging run has to stop the caller rather than leave it deploying
// workloads onto a cluster with no micro-VM assets.
func TestMicroVMDepsStepsReportAFailedScript(t *testing.T) {
	env, _ := fakeMicrovmScript(t, "1")

	err := env.DeployMicroVMDeps(t.Context())
	if err == nil {
		t.Fatal("DeployMicroVMDeps() = nil, want the script's failure")
	}
	if !strings.Contains(err.Error(), installMicrovmDepScript) {
		t.Errorf("error = %v, want it to name %s", err, installMicrovmDepScript)
	}
}

// Deploying benchmarks onto micro-VM has to bring the SandboxConfig with it:
// the workloads reference it by name. Only the micro-VM half is exercised here
// -- deploy_locust.sh is not stubbed, so the step fails right after.
func TestDeployBenchmarksInstallsMicroVMDeps(t *testing.T) {
	env, recorded := fakeMicrovmScript(t, "0")

	// The error is deploy_locust.sh missing, which is the next thing to run.
	_ = env.DeployBenchmarks(t.Context(), BenchmarkOptions{
		WorkerCount:  1,
		SandboxClass: config.SandboxClassMicrovm,
	})

	if args := recorded()[1:]; !slices.Equal(args, []string{"--install"}) {
		t.Errorf("the script got %v, want [--install]", args)
	}
}

// Confirm the opposite: a gvisor benchmark run must not touch the cluster-wide
// micro-VM SandboxConfig.
func TestDeleteBenchmarksLeavesMicroVMDepsAloneForGvisor(t *testing.T) {
	env, _ := fakeMicrovmScript(t, "0")

	_ = env.DeleteBenchmarks(t.Context(), BenchmarkOptions{
		WorkerCount:  1,
		SandboxClass: config.SandboxClassGvisor,
	})

	if _, err := os.Stat(filepath.Join(env.Cfg.Root, "record.txt")); !os.IsNotExist(err) {
		t.Error("the micro-VM script ran for a gvisor teardown")
	}
}

func TestMicroVMDepsStepsAreSeparableFromContext(t *testing.T) {
	// A cancelled context must stop the script rather than run it and discard
	// the result; the caller uses cancellation to abort a long staging run.
	env, _ := fakeMicrovmScript(t, "0")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := env.DeployMicroVMDeps(ctx); err == nil {
		t.Error("DeployMicroVMDeps() with a cancelled context = nil, want an error")
	}
}
