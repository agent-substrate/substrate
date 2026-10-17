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
	"strings"
	"time"

	"k8s.io/client-go/tools/clientcmd"
)

// CheckKindCluster checks local deployment prerequisites without creating or
// replacing a cluster. It uses the same kubeconfig loading rules as the clients.
func (c *Config) CheckKindCluster(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", "info").CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return fmt.Errorf("cannot access Docker; start Docker and check `docker info` before deploying to Kind: %w\n%s", err, strings.TrimSpace(string(output)))
	}

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = c.Kubeconfig
	config, err := rules.Load()
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("while reading kubeconfig: %w", err)
	}
	name := c.Context
	if err == nil {
		if name == "" {
			name = config.CurrentContext
		}
		if _, exists := config.Contexts[name]; exists {
			return nil
		}
	}

	cluster := c.Resolved().String("kindCluster.name")
	return fmt.Errorf("Kubernetes context %q was not found. If Kind cluster %q already exists, restore its kubeconfig with:\n"+
		"  hack/kind.sh export kubeconfig --name=%q\n"+
		"Otherwise create the cluster and local registry with:\n"+
		"  KIND_CLUSTER_NAME=%q hack/create-kind-cluster.sh\n"+
		"The creation script replaces an existing cluster with the same name. For a custom context, check --context and --kubeconfig instead",
		name, cluster, cluster, cluster)
}
