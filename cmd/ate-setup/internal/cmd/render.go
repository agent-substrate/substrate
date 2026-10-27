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
	"github.com/spf13/cobra"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
)

// renderAteSystemPath is the command path the render settings bind to.
const renderAteSystemPath = "render ate-system"

var renderCmd = &cobra.Command{
	Use:   "render",
	Short: "Write the manifests a deploy applies, without contacting a cluster",
	// Rendering reads the checkout and the image registry and never a cluster,
	// so it runs where there is no kubeconfig, such as a release job.
	Annotations: map[string]string{clusterAnnotation: "no"},
}

var renderAteSystemCmd = &cobra.Command{
	Use:   "ate-system",
	Short: "Write the manifests deploy ate-system applies into a directory",
	Long: `Write the manifests "deploy ate-system" applies to a fresh cluster into
--output-dir, one file per apply step, with every image pinned to a digest.

  ate-setup render ate-system --image-repo REPO --image-tag TAG --output-dir DIR

The files are numbered in apply order. Create the key pools after the
namespace, and apply the CRDs and wait for them before the rest, because a
later file holds a SandboxConfig:

  kubectl apply -f DIR/00-namespace.yaml
  ate-setup create jwt-authority-pool
  ate-setup create actor-id-ca-pool
  ate-setup create actor-id-ca-certs
  ate-setup create egress-mitm-ca-pool
  ate-setup create podcertificate-controller-cas
  kubectl apply -f DIR/01-crds.yaml
  kubectl wait --for=condition=Established crd --all
  kubectl apply -f DIR

The same settings shape the output as shape a deploy, except that nothing is
read from a cluster:

- The service account issuer is EXPECTED_JWT_ISSUER, else the GKE issuer when
  --gcp-project-id, --gcp-cluster-location and --gcp-cluster-name are set,
  else https://kubernetes.default.svc.
- None of deploy's checks against an existing install run, and the settings
  ConfigMaps are written whole. Do not apply the output over a running
  install.
- The CA and JWT pool Secrets are not written. Each holds a private key
  generated for one cluster, which would end up wherever the output goes,
  and reapplying a rendered pool would replace it and invalidate every
  certificate and token issued from it. The "ate-setup create" commands
  above create each pool only when it is absent, as deploy ate-system does.
  actor-id-ca-certs is copied from the actor-id-ca-pool Secret in the
  cluster, so it is created there too.
- The k8s-credential-provider namespace policy is not written. Operators
  edit it, and reapplying it would reset their policy. Create it once with
  kubectl apply -f manifests/egress-credential-injection/namespace-policy.yaml.
- The bundled PostgreSQL identities are not created: deploy ate-system and
  deploy postgres create them by running SQL in the postgres pod, which no
  manifest does yet. The CSI drivers are not written either.
- With an external database (ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING),
  ate-api-server-secret-envvars is not written, so its connection strings stay
  out of the directory. Create it before applying, with keys
  ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING,
  ATE_API_POSTGRES_OWNER_CONNECTION_STRING and ATE_API_POSTGRES_SCHEMA.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return env.RenderAteSystem(cmd.Context(), env.Cfg.Resolved().String("render.outputDir"))
	},
}

func init() {
	rootCmd.AddCommand(renderCmd)
	renderCmd.AddCommand(renderAteSystemCmd)

	config.RegisterCommand(config.Setting{
		Key: "render.outputDir", Env: "ATE_RENDER_OUTPUT_DIR", Flag: "output-dir",
		Kind:     config.KindString,
		Commands: []string{renderAteSystemPath},
		Usage:    "Directory the rendered manifests are written to; it must be new or empty",
	})
	config.BindCommandFlags(renderAteSystemPath, renderAteSystemCmd.Flags())
}
