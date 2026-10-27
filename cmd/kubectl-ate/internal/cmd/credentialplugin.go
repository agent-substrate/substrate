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
	"os"

	"go.ate.dev/substrate/internal/ateclient"
	"golang.org/x/term"
)

const (
	// kubeconfigEnv and kubeContextEnv pass --kubeconfig and --context to a
	// credential plugin.
	kubeconfigEnv  = "ATE_KUBECONFIG"
	kubeContextEnv = "ATE_KUBE_CONTEXT"
)

// credentialPluginConfig returns the plugin for --credential-plugin and
// --credential-plugin-arg, or nil when no plugin is given.
func credentialPluginConfig() (*ateclient.CredentialPlugin, error) {
	if credentialPlugin == "" {
		if len(credentialPluginArgs) > 0 {
			return nil, usageErrorf("--credential-plugin-arg requires --credential-plugin")
		}
		return nil, nil
	}
	if tokenFile != "" {
		return nil, usageErrorf("--token-file cannot be combined with --credential-plugin")
	}
	plugin := &ateclient.CredentialPlugin{
		Command: credentialPlugin,
		Args:    credentialPluginArgs,
		// Set even when empty, so that the plugin does not inherit values
		// from the user's environment that kubectl ate itself ignores.
		Env: []string{kubeconfigEnv + "=" + kubeconfig, kubeContextEnv + "=" + k8sContext},
	}
	if term.IsTerminal(int(os.Stdin.Fd())) {
		plugin.Stdin = os.Stdin
	}
	return plugin, nil
}
