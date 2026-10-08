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

// Package cmd implements the ate-setup command tree.
package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/version"
)

// Inputs that select where configuration comes from, as opposed to the
// settings themselves. Every setting is registered from config.Registry and
// read back off the flag set in PersistentPreRunE.
var (
	configPath       string
	noDevEnv         bool
	noReportDefaults bool
)

// NoReportDefaultsEnv shortens the configuration report without the flag, for
// a caller that runs ate-setup repeatedly and does not want the full listing
// each time.
const NoReportDefaultsEnv = "ATE_NO_REPORT_DEFAULTS"

// env is the shared execution context, built once per invocation.
var env *steps.Env

// resolved is the configuration this run settled on, kept so the record
// written afterwards describes what actually ran. A failed run is recorded
// from Execute, which cobra's PersistentPostRunE does not reach.
var resolved *config.Resolved

// recordAnnotation says whether a command's runs are recorded: "yes" or "no".
// The nearest command that carries it, the command itself or an ancestor,
// decides, so the deploy subtree opts in once and a component added later
// cannot quietly miss it. Demos and benchmarks opt back out: they deploy on
// top of an installed system with only their own settings, so recording them
// would replace the system's record with one that no longer reproduces it.
// Commands that install nothing -- delete, publish, setup -- are not marked.
const recordAnnotation = "record"

// recordsRun reports whether the nearest annotated command, cmd or an
// ancestor, opted in.
func recordsRun(cmd *cobra.Command) bool {
	for c := cmd; c != nil; c = c.Parent() {
		if v, ok := c.Annotations[recordAnnotation]; ok {
			return v == "yes"
		}
	}
	return false
}

var rootCmd = &cobra.Command{
	Use:   "ate-setup",
	Short: "Install and tear down Agent Substrate on a Kubernetes cluster",
	Long: `ate-setup deploys the Agent Substrate control plane, its supporting
secrets and config, and the bundled demos.

Cluster selection follows kubeconfig: pass --context to target a specific
cluster, or set KUBECTL_CONTEXT. Use --kind for a local Kind cluster, which
selects the kind manifest overlays, the local image registry, and host-only
image builds.

Developer settings are read from .ate-dev-env.sh at the repository root when it
is present (skipped for --kind, and by NO_DEV_ENV=1).`,
	Version:      version.String(),
	SilenceUsage: true,
	// Execute prints the error itself. Without this cobra prints it too, and
	// the two copies sandwich anything written in between.
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Commands that touch neither config nor cluster (help, version,
		// completion) opt out by way of not being run through this path.
		// cmd.Flags() rather than the root's persistent set: cobra parses a
		// subcommand's own flags before this hook runs, and command-scoped
		// settings live there. The complete set is local plus inherited.
		cfg, err := config.LoadFlags(cmd.Flags(), config.LoadOptions{
			ConfigPath: configPath,
			NoDevEnv:   noDevEnv,
		})
		if err != nil {
			return err
		}
		resolved = cfg.Resolved()

		// Reported before the cluster is touched, so the settings are on
		// screen even when connecting fails.
		resolved.Report(log.Writer(), config.ReportOptions{OmitDefaults: noReportDefaults})

		// Populate the kubeconfig before any client is built, for the
		// GKE-from-.ate-dev-env.sh flow.
		if err := cfg.EnsureClusterCredentials(cmd.Context()); err != nil {
			return err
		}
		env, err = steps.NewEnv(cfg)
		if err != nil {
			return err
		}
		return nil
	},
	// The bare command has no Run: cobra prints the help text and exits, and
	// because the command is not runnable, PersistentPreRunE never fires. No
	// setup is implied by an argument-less invocation; the user picks one.

	// Cobra runs this only when RunE returned nil, which is the success
	// condition a record of a completed install needs.
	PersistentPostRunE: func(cmd *cobra.Command, _ []string) error {
		recordRun(cmd, nil)
		return nil
	},
}

// recordRun writes what this run used, so the next one can reproduce it by
// pointing --config at the file. Recording is best effort: an install that
// worked must not be reported as failed because a directory was read-only.
func recordRun(cmd *cobra.Command, runErr error) {
	if resolved == nil || !recordsRun(cmd) {
		return
	}
	// The record package takes the directory and the context rather than
	// reading them from the settings, so naming the keys is this caller's job.
	dir, err := config.RecordDir(resolved.String("record.dir"))
	if err != nil {
		log.Warnf("not recording this run: %v", err)
		return
	}
	context := resolved.ClusterKey()

	// Recorded so the file says what the cluster runs. Left out when the run
	// failed before it had an environment to ask.
	var version string
	if env != nil {
		if v, _, err := env.SubstrateVersion(); err == nil {
			version = v
		}
	}

	var path string
	var omitted []string
	if runErr == nil {
		path, omitted, err = config.RecordSuccess(dir, context, version, resolved)
	} else {
		path, omitted, err = config.RecordFailure(dir, context, version, resolved, cmd.CommandPath())
	}
	if err != nil {
		log.Warnf("not recording this run: %v", err)
		return
	}

	if runErr == nil {
		log.Infof("recorded this configuration in %s", path)
		return
	}
	log.Infof("the settings this run used were written to %s", path)
	log.Infof("retry with: %s --config %s", cmd.CommandPath(), path)
	if len(omitted) > 0 {
		log.Infof("%d secret setting(s) were not written and must be supplied again: %s",
			len(omitted), strings.Join(omitted, ", "))
	}
}

// Root is the command tree, for callers that need to inspect it rather than
// run it. hack/install-ate.sh translates the historical installer flags into
// ate-setup command lines, and its test resolves each one against this to check
// it still names a command that exists.
func Root() *cobra.Command { return rootCmd }

// Execute runs the command tree.
func Execute() {
	// Settings register from each command package's init, so the merged set
	// only exists once the tree is built. A clash is a programming error, but
	// it cannot be caught at compile time.
	if err := config.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
	// ExecuteC reports which command ran, which the failure record needs and
	// Execute does not return.
	executed, err := rootCmd.ExecuteC()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		// After the error, so the retry line is the last thing on screen:
		// the error says what went wrong, the record says what to do next.
		if executed != nil {
			recordRun(executed, err)
		}
		os.Exit(1)
	}
}

func init() {
	f := rootCmd.PersistentFlags()
	// One flag per config.Registry entry, so a setting cannot reach the
	// environment and the file but miss the command line.
	config.BindFlags(f)
	f.StringVar(&configPath, "config", "",
		"Path to the installation configuration file (or "+config.ConfigPathEnv+"). Its settings "+
			"outrank the environment and are outranked by flags")
	f.BoolVar(&noDevEnv, "no-dev-env", false, "Do not source .ate-dev-env.sh")
	f.BoolVar(&noReportDefaults, "no-report-defaults", os.Getenv(NoReportDefaultsEnv) != "",
		"List only the settings something set in the configuration report (or "+NoReportDefaultsEnv+")")

	// Cobra's default completion command would run PersistentPreRunE and
	// require a cluster; the tree is not deep enough to justify that.
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}
