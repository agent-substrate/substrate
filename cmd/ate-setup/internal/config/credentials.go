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

package config

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// EnsureClusterCredentials fetches GKE credentials when the configuration
// names a project but no explicit context.
//
// This reproduces the prologue of the shell installer: a developer whose
// .ate-dev-env.sh sets PROJECT_ID gets their kubeconfig populated before
// anything talks to the cluster. Setting a context (or KUBECTL_CONTEXT) is
// taken as "I already have credentials" and skips the call, as it did there.
func (c *Config) EnsureClusterCredentials(ctx context.Context) error {
	if c.Kind || c.Context != "" || c.ProjectID == "" {
		return nil
	}
	if c.ClusterName == "" || c.ClusterLocation == "" {
		return fmt.Errorf("PROJECT_ID is set but CLUSTER_NAME and CLUSTER_LOCATION are not; "+
			"set them in %s or pass --context to use an already-configured cluster", devEnvFile)
	}

	log.Stepf("gcloud container clusters get-credentials %s", c.ClusterName)
	cmd := exec.CommandContext(ctx, "gcloud", "container", "clusters", "get-credentials",
		c.ClusterName,
		"--location", c.ClusterLocation,
		"--project", c.ProjectID,
	)
	cmd.Env = c.ScriptEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while fetching credentials for cluster %s: %w", c.ClusterName, err)
	}
	return nil
}
