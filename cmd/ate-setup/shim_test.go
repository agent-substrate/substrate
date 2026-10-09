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

package main

// hack/install-ate.sh translates the installer's historical flags onto this
// binary's commands. These tests pin that translation: they put a stub `go` on
// PATH, run the script, and check the command lines it would have run. Nothing
// is built and no cluster is touched, so the whole file is a unit test of the
// argument mapping -- the one part of the shim that can silently break a CI job
// or a developer's muscle memory.

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/cmd"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	// Registers every bundled demo, so demos.All() is the real list.
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/all"
)

// stubGo is a `go` that records the arguments of every `go run ./cmd/ate-setup`
// the script makes, one invocation per line, and succeeds.
const stubGo = `#!/usr/bin/env bash
printf '%s\n' "$*" >> "${SHIM_TEST_LOG}"
`

// runShim runs hack/install-ate.sh with args and returns the recorded
// invocations and the script's exit status.
func runShim(t *testing.T, env []string, args ...string) (invocations []string, exitCode int) {
	t.Helper()

	root := repoRoot(t)
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(stubGo), 0o755); err != nil {
		t.Fatalf("writing the stub go: %v", err)
	}
	log := filepath.Join(t.TempDir(), "invocations")

	script := exec.Command("bash", filepath.Join(root, "hack", "install-ate.sh"))
	script.Args = append(script.Args, args...)
	script.Dir = root
	// A fixed environment: the script reads SETUP_CSI and STORAGE_CLASS, so a
	// developer who exports either would otherwise change the expected output.
	// git needs PATH, and the stub go has to come first on it.
	script.Env = append([]string{
		"PATH=" + bin + ":" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"SHIM_TEST_LOG=" + log,
	}, env...)

	out, err := script.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		exitCode = exitErr.ExitCode()
	default:
		t.Fatalf("running install-ate.sh %v: %v\n%s", args, err, out)
	}

	recorded, err := os.ReadFile(log)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("reading the invocation log: %v", err)
	}
	for _, line := range strings.Split(string(recorded), "\n") {
		if line != "" {
			invocations = append(invocations, line)
			assertResolves(t, line)
		}
	}
	return invocations, exitCode
}

// goRunPrefix is what the script puts in front of every command line, since it
// invokes the installer as `go run ./cmd/ate-setup`.
const goRunPrefix = "run ./cmd/ate-setup "

// assertResolves checks that a command line the script produced names a command
// ate-setup actually has, with flags it accepts and arguments it allows.
//
// Without this the tests below only pin the script against itself: the stub go
// exits 0 whatever it is handed, so renaming `deploy apiserver` would keep them
// green while every job that goes through the shim broke.
func assertResolves(t *testing.T, invocation string) {
	t.Helper()

	argv, ok := strings.CutPrefix(invocation, goRunPrefix)
	if !ok {
		t.Errorf("the script ran `go %s`, want it to run the installer as `go %s...`", invocation, goRunPrefix)
		return
	}
	args := strings.Fields(argv)

	// Find walks as deep into the tree as the arguments name a command, and
	// hands back what is left for that command to parse.
	target, rest, err := cmd.Root().Find(args)
	if err != nil {
		t.Errorf("`ate-setup %s`: %v", argv, err)
		return
	}
	if err := target.ParseFlags(rest); err != nil {
		t.Errorf("`ate-setup %s`: %v", argv, err)
		return
	}
	if err := target.ValidateArgs(target.Flags().Args()); err != nil {
		t.Errorf("`ate-setup %s`: %v", argv, err)
		return
	}
	// A command that only groups subcommands prints help and does nothing, so
	// resolving to one means the script named a subcommand that is gone.
	if !target.Runnable() {
		t.Errorf("`ate-setup %s` resolves to %q, which is not runnable", argv, target.CommandPath())
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("git rev-parse --show-toplevel: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// The script keeps its own copy of the demo list so that --help answers
// without a build and an unknown --deploy-demo-* is rejected outright. That
// copy has to track the registry: a demo added only to internal/demos would
// otherwise have no installer flag, which is exactly the regression the shim
// exists to prevent.
func TestShimDemoListMatchesTheRegistry(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "hack", "install-ate.sh"))
	if err != nil {
		t.Fatalf("reading install-ate.sh: %v", err)
	}
	match := regexp.MustCompile(`(?s)\nATE_DEMOS=\((.*?)\n\)`).FindSubmatch(script)
	if match == nil {
		t.Fatal("no ATE_DEMOS=( ... ) array in install-ate.sh")
	}
	listed := map[string]bool{}
	for _, name := range strings.Fields(string(match[1])) {
		listed[name] = true
	}

	registered := map[string]bool{}
	for _, d := range demos.All() {
		registered[d.Name()] = true
	}

	for name := range registered {
		if !listed[name] {
			t.Errorf("%s is registered but has no --deploy-%s flag; add it to ATE_DEMOS", name, name)
		}
	}
	for name := range listed {
		if !registered[name] {
			t.Errorf("ATE_DEMOS lists %s, which no demo package registers", name)
		}
	}
}

func TestShimTranslatesFlags(t *testing.T) {
	const prefix = "run ./cmd/ate-setup "

	for _, tc := range []struct {
		name string
		env  []string
		args []string
		want []string
	}{{
		name: "deploy ate-system carries the CSI driver, and --setup-csi also acts on its own",
		args: []string{"--deploy-ate-system", "--setup-csi=nfs"},
		want: []string{
			"deploy ate-system --setup-csi=nfs",
			"setup csi nfs",
		},
	}, {
		name: "a bare --setup-csi means nfs",
		args: []string{"--setup-csi"},
		want: []string{"setup csi nfs"},
	}, {
		name: "--setup-csi takes a separated value",
		args: []string{"--setup-csi", "hostpath"},
		want: []string{"setup csi hostpath"},
	}, {
		name: "no CSI driver by default",
		args: []string{"--deploy-ate-system"},
		want: []string{"deploy ate-system --setup-csi=none"},
	}, {
		// The value-bearing flags were pre-scanned, so they shape every action
		// regardless of where they appear.
		name: "global flags apply to actions that precede them",
		args: []string{"--deploy-atenet", "--atenet-dataplane", "agentgateway"},
		want: []string{"--atenet-dataplane=agentgateway deploy atenet"},
	}, {
		name: "cluster profile flags are forwarded in either value form",
		args: []string{"--deploy-ate-system", "--cluster-size", "size10", "--cordon-control-plane"},
		want: []string{"--cluster-size=size10 --cordon-control-plane deploy ate-system --setup-csi=none"},
	}, {
		name: "--cluster-size takes an attached value",
		args: []string{"--cluster-size=size10", "--deploy-ate-apiserver"},
		want: []string{"--cluster-size=size10 deploy apiserver"},
	}, {
		name: "--ateapi-log-level is forwarded in either value form",
		args: []string{"--ateapi-log-level", "debug", "--deploy-ate-apiserver", "--ateapi-log-level=warn"},
		want: []string{"--ateapi-log-level=debug --ateapi-log-level=warn deploy apiserver"},
	}, {
		name: "--credential-provider is forwarded with a separate value",
		args: []string{"--deploy-atenet", "--credential-provider", `{"name":"custom","address":"cred.ate-system.svc:50051"}`},
		want: []string{`--credential-provider={"name":"custom","address":"cred.ate-system.svc:50051"} deploy atenet`},
	}, {
		name: "--credential-provider takes an attached value",
		args: []string{`--credential-provider={"name":"k8s.io"}`, "--deploy-ate-system"},
		want: []string{`--credential-provider={"name":"k8s.io"} deploy ate-system --setup-csi=none`},
	}, {
		name: "actions run in command line order",
		args: []string{"--deploy-ate-apiserver", "--deploy-atelet", "--delete-atenet"},
		want: []string{
			"deploy apiserver",
			"deploy atelet",
			"delete atenet",
		},
	}, {
		name: "every create flag maps to a create subcommand",
		args: []string{
			"--create-jwt-authority-pool-secret",
			"--create-actor-id-ca-pool-secret",
			"--create-actor-id-ca-certs-secret",
			"--create-egress-mitm-ca-pool-secret",
			"--create-podcertificate-controller-cas",
			"--create-api-server-env-vars",
			"--create-api-authentication-config",
			"--create-api-config",
		},
		want: []string{
			"create jwt-authority-pool",
			"create actor-id-ca-pool",
			"create actor-id-ca-certs",
			"create egress-mitm-ca-pool",
			"create podcertificate-controller-cas",
			"create api-server-env-vars",
			"create api-authentication-config",
			"create api-config",
		},
	}, {
		name: "delete flags",
		args: []string{"--delete-ate-system", "--delete-all"},
		want: []string{"delete ate-system", "delete all"},
	}, {
		name: "benchmark flags are renamed and apply to both benchmark actions",
		args: []string{
			"--benchmark-worker-count=4", "--deploy-benchmarks",
			"--benchmark-sandbox-class", "microvm", "--delete-benchmarks",
		},
		want: []string{
			"deploy benchmarks --worker-count=4 --sandbox-class=microvm",
			"delete benchmarks --worker-count=4 --sandbox-class=microvm",
		},
	}, {
		name: "demo flags drop the demo- prefix",
		args: []string{"--deploy-demo-egress-microvm", "--delete-demo-counter-microvm"},
		want: []string{
			"deploy demo egress-microvm",
			"delete demo counter-microvm",
		},
	}, {
		name: "the counter external volume defaults to the standard storage class",
		args: []string{"--deploy-demo-counter-with-external-volume"},
		want: []string{"deploy demo counter --with-external-volume --storage-class=standard"},
	}, {
		name: "the counter external volume follows --setup-csi",
		args: []string{"--setup-csi=hostpath", "--deploy-demo-counter-with-external-volume"},
		want: []string{
			"setup csi hostpath",
			"deploy demo counter --with-external-volume --storage-class=csi-hostpath-sc",
		},
	}, {
		name: "STORAGE_CLASS beats the driver --setup-csi installed",
		env:  []string{"STORAGE_CLASS=my-class"},
		args: []string{"--setup-csi=nfs", "--deploy-demo-counter-with-external-volume"},
		want: []string{
			"setup csi nfs",
			"deploy demo counter --with-external-volume --storage-class=my-class",
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got, exitCode := runShim(t, tc.env, tc.args...)
			if exitCode != 0 {
				t.Fatalf("exit code = %d, want 0", exitCode)
			}
			want := make([]string, len(tc.want))
			for i, w := range tc.want {
				want[i] = prefix + w
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("invocations:\n  got  %q\n  want %q", got, want)
			}
		})
	}
}

// --benchmark-actor-memory has no ate-setup flag; the script exports it, which
// the stub go cannot observe through its arguments.
func TestShimExportsBenchmarkActorMemory(t *testing.T) {
	root := repoRoot(t)
	bin := t.TempDir()
	const reportEnv = `#!/usr/bin/env bash
printf '%s\n' "BENCHMARK_ACTOR_MEMORY=${BENCHMARK_ACTOR_MEMORY:-<unset>}"
`
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(reportEnv), 0o755); err != nil {
		t.Fatalf("writing the stub go: %v", err)
	}

	script := exec.Command("bash", filepath.Join(root, "hack", "install-ate.sh"),
		"--benchmark-actor-memory", "512Mi", "--deploy-benchmarks")
	script.Dir = root
	script.Env = []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}

	out, err := script.CombinedOutput()
	if err != nil {
		t.Fatalf("running install-ate.sh: %v\n%s", err, out)
	}
	if want := "BENCHMARK_ACTOR_MEMORY=512Mi\n"; string(out) != want {
		t.Errorf("stub go saw %q, want %q", out, want)
	}
}

func TestShimRejectsUnknownFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--no-such-flag"},
		{"--deploy-demo-no-such-demo"},
		{"--delete-demo-no-such-demo"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			got, exitCode := runShim(t, nil, args...)
			if exitCode != 1 {
				t.Errorf("exit code = %d, want 1", exitCode)
			}
			if len(got) != 0 {
				t.Errorf("ran %q, want nothing", got)
			}
		})
	}
}

// An action before an unknown flag still runs: the script dispatches as it
// walks the command line, as it always has.
func TestShimRunsActionsBeforeAnUnknownFlag(t *testing.T) {
	got, exitCode := runShim(t, nil, "--deploy-atelet", "--no-such-flag")
	if exitCode != 1 {
		t.Errorf("exit code = %d, want 1", exitCode)
	}
	if want := []string{"run ./cmd/ate-setup deploy atelet"}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("invocations = %q, want %q", got, want)
	}
}

func TestShimUsage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		exitCode int
	}{
		{name: "no arguments is an error", args: nil, exitCode: 1},
		{name: "--help anywhere exits cleanly", args: []string{"--deploy-ate-system", "--help"}, exitCode: 0},
		{name: "-h anywhere exits cleanly", args: []string{"-h"}, exitCode: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, exitCode := runShim(t, nil, tc.args...)
			if exitCode != tc.exitCode {
				t.Errorf("exit code = %d, want %d", exitCode, tc.exitCode)
			}
			if len(got) != 0 {
				t.Errorf("ran %q, want nothing: usage must not touch the cluster", got)
			}
		})
	}
}

// Every flag the script dispatches on has to be documented, since --help is
// the only place a user can discover them now that the demos are Go packages.
func TestShimUsageDocumentsEveryFlag(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "hack", "install-ate.sh"))
	if err != nil {
		t.Fatalf("reading install-ate.sh: %v", err)
	}
	usage := shimUsage(t)

	// The dispatch arms below `run_demo`'s definition, minus the demo
	// wildcards, which usage covers one demo at a time.
	for _, flag := range regexp.MustCompile(`(?m)^    (--[a-z0-9-]+)[)|]`).FindAllStringSubmatch(string(script), -1) {
		if !strings.Contains(usage, flag[1]) {
			t.Errorf("--help does not mention %s", flag[1])
		}
	}
	for _, d := range demos.All() {
		for _, flag := range []string{"--deploy-" + d.Name(), "--delete-" + d.Name()} {
			if !strings.Contains(usage, flag) {
				t.Errorf("--help does not mention %s", flag)
			}
		}
	}
}

func shimUsage(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	script := exec.Command("bash", filepath.Join(root, "hack", "install-ate.sh"), "--help")
	script.Dir = root
	script.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	out, err := script.Output()
	if err != nil {
		t.Fatalf("install-ate.sh --help: %v", err)
	}
	return string(out)
}

// Settings register from each command package's init, so the merged set only
// exists in a binary that links the whole command tree. Package config cannot
// check it: the commands that register are the ones config does not import.
// This package does import them, which is what makes the check possible here
// and nowhere below it.
func TestSettingRegistryIsValidAcrossEveryCommand(t *testing.T) {
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
}

// Command-scoped settings exist so a flag appears only where it applies. A
// setting that named a command nobody built would silently never bind.
func TestEveryScopedSettingNamesARealCommand(t *testing.T) {
	for _, s := range config.All() {
		for _, path := range s.Commands {
			t.Run(s.Key+" on "+path, func(t *testing.T) {
				target, _, err := cmd.Root().Find(strings.Fields(path))
				if err != nil {
					t.Fatalf("Find(%q): %v", path, err)
				}
				if target.CommandPath() != "ate-setup "+path {
					t.Fatalf("%q resolved to %q, which is not that command", path, target.CommandPath())
				}
				if target.Flags().Lookup(s.Flag) == nil {
					t.Errorf("--%s is not bound to %q", s.Flag, path)
				}
			})
		}
	}
}

// docPath resolves a repository path from this package's directory.
func docPath(t *testing.T, rel string) string {
	t.Helper()
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	return filepath.Join(root, rel)
}

// The worked example is the first thing an operator copies, so a renamed or
// removed key has to break the build rather than their install.
func TestExampleConfigurationParses(t *testing.T) {
	path := docPath(t, "docs/examples/substrate-install.yaml")
	f, err := config.ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile(%s) = %v", path, err)
	}
	if len(f.Keys()) == 0 {
		t.Error("the example sets no settings, so it demonstrates nothing")
	}
}

// The settings table is generated from the registry, so a setting added
// without documenting it is the drift this catches.
func TestOperatorDocListsEverySetting(t *testing.T) {
	path := docPath(t, "docs/operator-install.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	doc := string(raw)

	for _, s := range config.All() {
		t.Run(s.Key, func(t *testing.T) {
			want := []string{"`" + s.Key + "`", "`" + s.Env + "`"}
			// A secret has no flag, so the docs have none to name.
			if s.Flag != "" {
				want = append(want, "`--"+s.Flag+"`")
			}
			for _, want := range want {
				if !strings.Contains(doc, want) {
					t.Errorf("docs/operator-install.md does not mention %s", want)
				}
			}
		})
	}
}

// The operator documentation must not name a setting that no longer exists:
// following it would configure nothing and the install would silently use a
// default.
func TestDocsNameNoRemovedSetting(t *testing.T) {
	// The two controls that select how configuration is read and reported.
	// They are not settings themselves, so no registry entry declares them.
	notSettings := map[string]bool{
		"ATE_CONFIG":             true,
		"ATE_NO_REPORT_DEFAULTS": true,
	}

	declared := map[string]bool{}
	for _, s := range config.All() {
		declared[s.Env] = true
	}

	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	for _, rel := range []string{"docs/operator-install.md", "docs/examples/substrate-install.yaml"} {
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", rel, err)
		}
		seen := map[string]bool{}
		for _, m := range regexp.MustCompile(`\b(ATE_[A-Z0-9_]{3,})\b`).FindAllStringSubmatch(string(raw), -1) {
			name := m[1]
			if declared[name] || notSettings[name] || seen[name] {
				continue
			}
			seen[name] = true
			t.Errorf("%s names %s, which no setting declares", rel, name)
		}
	}
}
