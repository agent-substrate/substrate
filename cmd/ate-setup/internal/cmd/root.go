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

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/version"
)

// opts collects the global flags. They are resolved into a config.Config in
// PersistentPreRunE so that every subcommand sees the same environment.
var opts config.Options

// env is the shared execution context, built once per invocation.
var env *steps.Env

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
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("podcert-workers-per-signer") && opts.PodcertWorkersPerSigner < 1 {
			return fmt.Errorf("--podcert-workers-per-signer must be a positive integer, got %d", opts.PodcertWorkersPerSigner)
		}
		// Commands that touch neither config nor cluster (help, version,
		// completion) opt out by way of not being run through this path.
		cfg, err := config.Load(opts)
		if err != nil {
			return err
		}
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
}

// Execute runs the command tree.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func init() {
	f := rootCmd.PersistentFlags()
	f.BoolVar(&opts.Kind, "kind", false,
		"Target a local Kind cluster: use the kind overlays, the local registry, and host-architecture builds")
	f.StringVar(&opts.Kubeconfig, "kubeconfig", "", "Path to the kubeconfig file")
	f.StringVar(&opts.Context, "context", "", "Name of the kubeconfig context to use (defaults to KUBECTL_CONTEXT)")
	f.StringVar(&opts.Router, "atenet-router", "", "atenet router dataplane: envoy or agentgateway (default envoy)")
	f.StringVar(&opts.RolloutTimeout, "rollout-timeout", "", "Timeout for workload rollouts as a duration string (e.g. 60s, 5m)")
	f.IntVar(&opts.PodcertWorkersPerSigner, "podcert-workers-per-signer", 0, "Number of worker goroutines per signer in podcertificate-controller")
	f.BoolVar(&opts.ExperimentalUseSDSMint, "experimental-use-sdsmint", false, "Deploy egress gateway with dynamic per-SNI certificate minting")
	f.StringVar(&opts.AdditionalEgressExtprocService, "experimental-additional-egress-extproc-service", "", "Run an additional ext_proc authorization filter served by NS/SVC:PORT (requires --experimental-use-sdsmint)")
	f.BoolVar(&opts.NoDevEnv, "no-dev-env", false, "Do not source .ate-dev-env.sh")

	// Cobra's default completion command would run PersistentPreRunE and
	// require a cluster; the tree is not deep enough to justify that.
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}
