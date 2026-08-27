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
	"context"

	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create an individual secret or config used by ate-system",
	Long: `Create one of the resources ate-system depends on.

These are created automatically by "ate-setup deploy ate-system"; the
subcommands here exist to (re)create a single resource on an existing
cluster.`,
}

func init() {
	rootCmd.AddCommand(createCmd)

	for _, sub := range []struct {
		use   string
		short string
		run   func(*steps.Env, context.Context) error
	}{
		{
			use:   "jwt-authority-pool",
			short: "Create the actor-identity JWT authority pool secret",
			run:   (*steps.Env).CreateJWTAuthorityPoolSecret,
		},
		{
			use:   "actor-id-ca-pool",
			short: "Create the actor-identity CA pool secret",
			run:   (*steps.Env).CreateActorIDCAPoolSecret,
		},
		{
			use:   "actor-id-ca-certs",
			short: "Create the actor-identity CA trust bundle secret used by the egress gateway",
			run:   (*steps.Env).CreateActorIDCACertsSecret,
		},
		{
			use:   "egress-mitm-ca-pool",
			short: "Create the egress MITM CA pool secret used by the egress gateway with SDS mint",
			run:   (*steps.Env).CreateEgressMITMCAPoolSecret,
		},
		{
			use:   "podcertificate-controller-cas",
			short: "Create the podcertificate controller's servicedns and podidentity CA pools",
			run:   (*steps.Env).CreatePodCertificateControllerCAs,
		},
		{
			use:   "api-server-env-vars",
			short: "Create the ate-api-server environment ConfigMap",
			run:   (*steps.Env).CreateAPIServerEnvVars,
		},
		{
			use:   "api-authentication-config",
			short: "Create the default ate-api-server authentication config",
			run:   (*steps.Env).CreateAPIAuthenticationConfig,
		},
	} {
		run := sub.run
		createCmd.AddCommand(&cobra.Command{
			Use:   sub.use,
			Short: sub.short,
			Args:  cobra.NoArgs,
			RunE: func(cmd *cobra.Command, _ []string) error {
				return run(env, cmd.Context())
			},
		})
	}
}
