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

package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func resetFlagsRecursive(c *cobra.Command) {
	resetFlagSet := func(fs *pflag.FlagSet) {
		fs.VisitAll(func(f *pflag.Flag) {
			f.Changed = false
			_ = f.Value.Set(f.DefValue)
		})
	}
	resetFlagSet(c.LocalFlags())
	resetFlagSet(c.PersistentFlags())
	for _, sub := range c.Commands() {
		resetFlagsRecursive(sub)
	}
}

func runRootCLI(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	resetFlagsRecursive(rootCmd)
	var outBuf, errBuf bytes.Buffer
	rootCmd.SetOut(&outBuf)
	rootCmd.SetErr(&errBuf)
	rootCmd.SetArgs(args)
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
		resetFlagsRecursive(rootCmd)
	})
	err = executeRoot(rootCmd)
	return outBuf.String(), errBuf.String(), err
}

func TestHelpOutputGoesToStdout(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantStdoutSub string
	}{
		{
			name:          "root --help",
			args:          []string{"--help"},
			wantStdoutSub: "Usage:\n  kubectl-ate [command]",
		},
		{
			name:          "verb --help",
			args:          []string{"get", "--help"},
			wantStdoutSub: "Usage:\n  kubectl-ate get [command]",
		},
		{
			name:          "leaf -h",
			args:          []string{"create", "actor", "-h"},
			wantStdoutSub: "Usage:\n  kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "help subcommand",
			args:          []string{"help", "create", "actor"},
			wantStdoutSub: "Usage:\n  kubectl-ate create actor <actor-name> [flags]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runRootCLI(t, tc.args...)
			if err != nil {
				t.Fatalf("unexpected error: %v (stderr: %s)", err, stderr)
			}
			if stderr != "" {
				t.Errorf("expected empty stderr on --help, got:\n%s", stderr)
			}
			if !strings.Contains(stdout, tc.wantStdoutSub) {
				t.Errorf("stdout = %q, want it to contain %q", stdout, tc.wantStdoutSub)
			}
		})
	}
}

func TestCLIUsageErrorsGoToStderrWithUsage(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		wantErrorLine string
		wantUsageLine string
	}{
		// Flag value / parsing errors
		{
			name:          "get actor -a missing argument",
			args:          []string{"get", "actor", "-a"},
			wantErrorLine: `Error: flag "-a/--atespace" requires a value`,
			wantUsageLine: "kubectl-ate get actor [actor-name ...] [flags]",
		},
		{
			name:          "get actor --atespace missing argument",
			args:          []string{"get", "actor", "--atespace"},
			wantErrorLine: `Error: flag "-a/--atespace" requires a value`,
			wantUsageLine: "kubectl-ate get actor [actor-name ...] [flags]",
		},
		{
			name:          "create actor -a missing argument",
			args:          []string{"create", "actor", "-a"},
			wantErrorLine: `Error: flag "-a/--atespace" requires a value`,
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "create actor-template -f missing argument",
			args:          []string{"create", "actor-template", "-f"},
			wantErrorLine: `Error: flag "-f/--filename" requires a value`,
			wantUsageLine: "kubectl-ate create actor-template -f <manifest> [flags]",
		},
		{
			name:          "global -o missing argument",
			args:          []string{"get", "atespaces", "-o"},
			wantErrorLine: `Error: flag "-o/--output" requires a value`,
			wantUsageLine: "kubectl-ate get atespace [atespace-name ...] [flags]",
		},
		{
			name:          "swallowed flag after -a",
			args:          []string{"create", "actor", "my-actor", "-a", "--template", "counter"},
			wantErrorLine: `Error: flag "-a/--atespace" requires a value, got flag "--template"`,
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "invalid output format",
			args:          []string{"get", "atespaces", "-o", "xml"},
			wantErrorLine: `Error: invalid output format "xml". Must be one of: table, json, yaml`,
			wantUsageLine: "kubectl-ate get atespace [atespace-name ...] [flags]",
		},
		{
			name:          "unknown short flag on leaf",
			args:          []string{"get", "atespaces", "-x"},
			wantErrorLine: `Error: unknown flag "-x"`,
			wantUsageLine: "kubectl-ate get atespace [atespace-name ...] [flags]",
		},
		{
			name:          "unknown long flag on leaf",
			args:          []string{"get", "atespaces", "--bogus"},
			wantErrorLine: `Error: unknown flag "--bogus"`,
			wantUsageLine: "kubectl-ate get atespace [atespace-name ...] [flags]",
		},

		// Combined positional-arg and required-flag errors
		{
			name:          "create actor with only -a reports missing arg and --template",
			args:          []string{"create", "actor", "-a", "my-atespace"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flag --template",
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "create actor with nothing reports missing arg and both required flags",
			args:          []string{"create", "actor"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flags -a/--atespace, --template",
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "create actor with name only reports both missing flags",
			args:          []string{"create", "actor", "my-actor"},
			wantErrorLine: "Error: missing required flags -a/--atespace, --template",
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "create actor with name and -a reports missing --template",
			args:          []string{"create", "actor", "my-actor", "-a", "my-atespace"},
			wantErrorLine: "Error: missing required flag --template",
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "create actor with both flags but no name reports missing <actor-name>",
			args:          []string{"create", "actor", "-a", "my-atespace", "--template", "counter"},
			wantErrorLine: "Error: missing required argument <actor-name>",
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "create actor with too many args",
			args:          []string{"create", "actor", "a1", "a2", "-a", "my-atespace", "--template", "counter"},
			wantErrorLine: `Error: expected 1 <actor-name> argument, received 2 ("a1", "a2")`,
			wantUsageLine: "kubectl-ate create actor <actor-name> [flags]",
		},
		{
			name:          "update actor with nothing",
			args:          []string{"update", "actor"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flag -a/--atespace",
			wantUsageLine: "kubectl-ate update actor <actor-name> [flags]",
		},
		{
			name:          "update actor missing -a",
			args:          []string{"update", "actor", "my-actor", "--template", "counter"},
			wantErrorLine: "Error: missing required flag -a/--atespace",
			wantUsageLine: "kubectl-ate update actor <actor-name> [flags]",
		},
		{
			name:          "delete actor with nothing",
			args:          []string{"delete", "actor"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flag -a/--atespace",
			wantUsageLine: "kubectl-ate delete actor <actor-name> [flags]",
		},
		{
			name:          "delete actor missing -a",
			args:          []string{"delete", "actor", "my-actor"},
			wantErrorLine: "Error: missing required flag -a/--atespace",
			wantUsageLine: "kubectl-ate delete actor <actor-name> [flags]",
		},
		{
			name:          "revert actor with nothing",
			args:          []string{"revert", "actor"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flag -a/--atespace",
			wantUsageLine: "kubectl-ate revert actor <actor-name> [flags]",
		},
		{
			name:          "logs actor with nothing",
			args:          []string{"logs", "actor"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flag -a/--atespace",
			wantUsageLine: "kubectl-ate logs actor <actor-name> [flags]",
		},
		{
			name:          "create actor-template missing -f",
			args:          []string{"create", "actor-template"},
			wantErrorLine: "Error: missing required flag -f/--filename",
			wantUsageLine: "kubectl-ate create actor-template -f <manifest> [flags]",
		},
		{
			name:          "create actor-template unexpected positional arg",
			args:          []string{"create", "actor-template", "my-tpl", "-f", "tpl.yaml"},
			wantErrorLine: `Error: unexpected argument "my-tpl" (the template name comes from the manifest passed to -f/--filename)`,
			wantUsageLine: "kubectl-ate create actor-template -f <manifest> [flags]",
		},
		{
			name:          "delete actor-template with nothing",
			args:          []string{"delete", "actor-template"},
			wantErrorLine: "Error: missing required argument <template-name> and required flag -a/--atespace",
			wantUsageLine: "kubectl-ate delete actor-template <template-name> [flags]",
		},
		{
			name:          "create atespace missing name",
			args:          []string{"create", "atespace"},
			wantErrorLine: "Error: missing required argument <atespace-name>",
			wantUsageLine: "kubectl-ate create atespace <atespace-name> [flags]",
		},
		{
			name:          "get egress-policy missing all",
			args:          []string{"get", "egress-policy"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flag -a/--atespace",
			wantUsageLine: "kubectl-ate get egress-policy <actor-name> [flags]",
		},
		{
			name:          "get egress-policy rejects multiple args",
			args:          []string{"get", "egress-policy", "c1", "c2", "-a", "team-a"},
			wantErrorLine: `Error: expected 1 <actor-name> argument, received 2 ("c1", "c2")`,
			wantUsageLine: "kubectl-ate get egress-policy <actor-name> [flags]",
		},
		{
			name:          "create egress-policy missing all",
			args:          []string{"create", "egress-policy"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flags -a/--atespace, -f/--filename",
			wantUsageLine: "kubectl-ate create egress-policy <actor-name> -f <manifest> [flags]",
		},
		{
			name:          "create egress-policy rejects multiple args",
			args:          []string{"create", "egress-policy", "c1", "c2", "-a", "team-a", "-f", "policy.yaml"},
			wantErrorLine: `Error: expected 1 <actor-name> argument, received 2 ("c1", "c2")`,
			wantUsageLine: "kubectl-ate create egress-policy <actor-name> -f <manifest> [flags]",
		},
		{
			name:          "update egress-policy missing all",
			args:          []string{"update", "egress-policy"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flags -a/--atespace, -f/--filename",
			wantUsageLine: "kubectl-ate update egress-policy <actor-name> -f <manifest> [flags]",
		},
		{
			name:          "update egress-policy rejects multiple args",
			args:          []string{"update", "egress-policy", "c1", "c2", "-a", "team-a", "-f", "policy.yaml"},
			wantErrorLine: `Error: expected 1 <actor-name> argument, received 2 ("c1", "c2")`,
			wantUsageLine: "kubectl-ate update egress-policy <actor-name> -f <manifest> [flags]",
		},
		{
			name:          "delete egress-policy missing all",
			args:          []string{"delete", "egress-policy"},
			wantErrorLine: "Error: missing required argument <actor-name> and required flag -a/--atespace",
			wantUsageLine: "kubectl-ate delete egress-policy <actor-name> [flags]",
		},
		{
			name:          "delete egress-policy rejects multiple args",
			args:          []string{"delete", "egress-policy", "c1", "c2", "-a", "team-a"},
			wantErrorLine: `Error: expected 1 <actor-name> argument, received 2 ("c1", "c2")`,
			wantUsageLine: "kubectl-ate delete egress-policy <actor-name> [flags]",
		},
		{
			name:          "create tag missing all",
			args:          []string{"create", "tag"},
			wantErrorLine: "Error: missing required argument <tag-name> and required flags -a/--atespace, --actor",
			wantUsageLine: "kubectl-ate create tag <tag-name> [flags]",
		},
		{
			name:          "update tag missing scope",
			args:          []string{"update", "tag", "t1", "-a", "space-1"},
			wantErrorLine: "Error: missing required flag --scope",
			wantUsageLine: "kubectl-ate update tag <tag-name> [flags]",
		},
		{
			name:          "top workers unexpected positional arg",
			args:          []string{"top", "workers", "w1"},
			wantErrorLine: `Error: "kubectl-ate top worker" takes no positional arguments (received "w1")`,
			wantUsageLine: "kubectl-ate top worker [flags]",
		},
		{
			name:          "admin make-ca-pool missing --name",
			args:          []string{"admin", "make-ca-pool"},
			wantErrorLine: "Error: missing required flag --name",
			wantUsageLine: "kubectl-ate admin make-ca-pool [flags]",
		},
		{
			name:          "admin make-ca-pool unexpected positional arg",
			args:          []string{"admin", "make-ca-pool", "my-secret", "--name", "my-secret"},
			wantErrorLine: `Error: unexpected argument "my-secret" (use --name to specify the secret name)`,
			wantUsageLine: "kubectl-ate admin make-ca-pool [flags]",
		},
		{
			name:          "admin activate-jwt-key missing both required flags",
			args:          []string{"admin", "activate-jwt-key"},
			wantErrorLine: "Error: missing required flags --name, --key-id",
			wantUsageLine: "kubectl-ate admin activate-jwt-key [flags]",
		},

		// Pre-connection flag validation (runs without dialing any cluster)
		{
			name:          "get actor without -a or -A",
			args:          []string{"get", "actor"},
			wantErrorLine: "Error: -a/--atespace <name> or -A/--all-atespaces is required",
			wantUsageLine: "kubectl-ate get actor [actor-name ...] [flags]",
		},
		{
			name:          "get actor with both -a and -A",
			args:          []string{"get", "actor", "-a", "space-1", "-A"},
			wantErrorLine: "Error: -a/--atespace and -A/--all-atespaces are mutually exclusive",
			wantUsageLine: "kubectl-ate get actor [actor-name ...] [flags]",
		},
		{
			name:          "get actor by name without -a",
			args:          []string{"get", "actor", "my-actor"},
			wantErrorLine: "Error: -a/--atespace is required when getting actors by name",
			wantUsageLine: "kubectl-ate get actor [actor-name ...] [flags]",
		},
		{
			name:          "get actor by name with -A",
			args:          []string{"get", "actor", "my-actor", "-A"},
			wantErrorLine: "Error: -A/--all-atespaces cannot be used when getting actors by name",
			wantUsageLine: "kubectl-ate get actor [actor-name ...] [flags]",
		},
		{
			name:          "get actor-template without -a or -A",
			args:          []string{"get", "actor-template"},
			wantErrorLine: "Error: -a/--atespace <name> or -A/--all-atespaces is required",
			wantUsageLine: "kubectl-ate get actor-template [template-name ...] [flags]",
		},
		{
			name:          "get tag without -a or -A",
			args:          []string{"get", "tag"},
			wantErrorLine: "Error: -a/--atespace <name> or -A/--all-atespaces is required",
			wantUsageLine: "kubectl-ate get tag [tag-name ...] [flags]",
		},
		{
			name:          "get tag by name with -A",
			args:          []string{"get", "tag", "t1", "-A"},
			wantErrorLine: "Error: -A/--all-atespaces cannot be used when getting tags by name",
			wantUsageLine: "kubectl-ate get tag [tag-name ...] [flags]",
		},
		{
			name:          "get workers by name with filter flag",
			args:          []string{"get", "workers", "w1", "-n", "default"},
			wantErrorLine: "Error: -n/--namespace cannot be used when getting workers by name",
			wantUsageLine: "kubectl-ate get worker [worker-name ...] [flags]",
		},
		{
			name:          "get workers with invalid label selector",
			args:          []string{"get", "workers", "-l", "===bad"},
			wantErrorLine: `Error: invalid label selector "===bad"`,
			wantUsageLine: "kubectl-ate get worker [worker-name ...] [flags]",
		},
		{
			name:          "top workers with invalid label selector",
			args:          []string{"top", "workers", "-l", "===bad"},
			wantErrorLine: `Error: invalid label selector "===bad"`,
			wantUsageLine: "kubectl-ate top worker [flags]",
		},
		{
			name:          "logs actor with invalid --source",
			args:          []string{"logs", "actor", "my-actor", "-a", "space-1", "--source=bogus"},
			wantErrorLine: `Error: invalid --source "bogus"`,
			wantUsageLine: "kubectl-ate logs actor <actor-name> [flags]",
		},
		{
			name:          "update actor without --template or --worker-selector",
			args:          []string{"update", "actor", "my-actor", "-a", "space-1"},
			wantErrorLine: "Error: at least one of --template or --worker-selector is required",
			wantUsageLine: "kubectl-ate update actor <actor-name> [flags]",
		},
		{
			name:          "update actor with malformed --template",
			args:          []string{"update", "actor", "my-actor", "-a", "space-1", "--template", "a/b/c"},
			wantErrorLine: `Error: malformed reference "a/b/c" (expected <name> or <atespace>/<name>)`,
			wantUsageLine: "kubectl-ate update actor <actor-name> [flags]",
		},
		{
			name:          "update actor with invalid --worker-selector",
			args:          []string{"update", "actor", "my-actor", "-a", "space-1", "--worker-selector", "===bad"},
			wantErrorLine: `Error: invalid worker selector "===bad"`,
			wantUsageLine: "kubectl-ate update actor <actor-name> [flags]",
		},
		{
			name:          "create tag with invalid --scope",
			args:          []string{"create", "tag", "t1", "-a", "space-1", "--actor", "a1", "--scope=bogus"},
			wantErrorLine: `Error: invalid scope "bogus"; must be atespace or published`,
			wantUsageLine: "kubectl-ate create tag <tag-name> [flags]",
		},
		{
			name:          "delete egress-policy with invalid --version",
			args:          []string{"delete", "egress-policy", "a1", "-a", "space-1", "--version=0"},
			wantErrorLine: "Error: --version must be at least 1, got 0",
			wantUsageLine: "kubectl-ate delete egress-policy <actor-name> [flags]",
		},
		{
			name:          "admin make-ca-pool with invalid --key-type",
			args:          []string{"admin", "make-ca-pool", "--name", "s", "--key-type", "RSA"},
			wantErrorLine: `Error: unknown key type "RSA"`,
			wantUsageLine: "kubectl-ate admin make-ca-pool [flags]",
		},

		// Bare verb and unknown subcommand errors
		{
			name:          "bare root command",
			args:          []string{},
			wantErrorLine: "Error: missing required subcommand",
			wantUsageLine: "kubectl-ate [command]",
		},
		{
			name:          "unknown root subcommand",
			args:          []string{"bogus"},
			wantErrorLine: `Error: unknown subcommand "bogus"`,
			wantUsageLine: "kubectl-ate [command]",
		},
		{
			name:          "bare get command",
			args:          []string{"get"},
			wantErrorLine: "Error: missing required subcommand",
			wantUsageLine: "kubectl-ate get [command]",
		},
		{
			name:          "bare create command",
			args:          []string{"create"},
			wantErrorLine: "Error: missing required subcommand",
			wantUsageLine: "kubectl-ate create [command]",
		},
		{
			name:          "create plural actors suggests singular actor",
			args:          []string{"create", "actors", "my-actor", "-a", "space-1", "--template", "counter"},
			wantErrorLine: `Error: unknown subcommand "actors" (did you mean "actor"?)`,
			wantUsageLine: "kubectl-ate create [command]",
		},
		{
			name:          "logs without actor subcommand suggests logs actor <name>",
			args:          []string{"logs", "my-actor", "-a", "space-1"},
			wantErrorLine: `Error: unknown subcommand "my-actor" (did you mean "actor my-actor"?)`,
			wantUsageLine: "kubectl-ate logs [command]",
		},
		{
			name:          "logs with unknown subcommand and -h still reports unknown subcommand",
			args:          []string{"logs", "asdasd", "-h"},
			wantErrorLine: `Error: unknown subcommand "asdasd" (did you mean "actor asdasd"?)`,
			wantUsageLine: "kubectl-ate logs [command]",
		},
		{
			name:          "flag passed to verb without resource type",
			args:          []string{"get", "-a", "space-1"},
			wantErrorLine: `Error: unknown flag "-a"`,
			wantUsageLine: "kubectl-ate get [command]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, err := runRootCLI(t, tc.args...)
			if err == nil {
				t.Fatalf("expected error for args %v, got nil", tc.args)
			}
			if stdout != "" {
				t.Errorf("expected empty stdout on error, got:\n%s", stdout)
			}
			firstLine, _, _ := strings.Cut(stderr, "\n")
			if !strings.Contains(firstLine, tc.wantErrorLine) {
				t.Errorf("first stderr line = %q, want it to contain %q\nfull stderr:\n%s", firstLine, tc.wantErrorLine, stderr)
			}
			if !strings.Contains(stderr, "\n\nUsage:\n") {
				t.Errorf("stderr missing Usage section:\n%s", stderr)
			}
			if !strings.Contains(stderr, tc.wantUsageLine) {
				t.Errorf("stderr = %q, want usage line %q", stderr, tc.wantUsageLine)
			}
		})
	}
}

func TestRuntimeErrorsDoNotPrintUsage(t *testing.T) {
	stdout, stderr, err := runRootCLI(t, "create", "actor-template", "-f", "/nonexistent/path/to/manifest.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent manifest file, got nil")
	}
	if stdout != "" {
		t.Errorf("expected empty stdout, got:\n%s", stdout)
	}
	if !strings.HasPrefix(stderr, "Error: ") {
		t.Errorf("expected stderr to start with 'Error: ', got:\n%s", stderr)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Errorf("expected runtime error not to print Usage, got:\n%s", stderr)
	}
}
