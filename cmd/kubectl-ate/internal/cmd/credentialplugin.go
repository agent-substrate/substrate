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
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode"

	"go.ate.dev/substrate/cmd/kubectl-ate/internal/credplugin"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const (
	// credentialPluginEnv names the credential plugin when --credential-plugin
	// is not given.
	credentialPluginEnv = "ATE_CREDENTIAL_PLUGIN"
	// kubeconfigEnv and kubeContextEnv pass --kubeconfig and --context to a
	// credential plugin; `kubectl ate credential` uses them when it is given
	// neither flag itself.
	kubeconfigEnv  = "ATE_KUBECONFIG"
	kubeContextEnv = "ATE_KUBE_CONTEXT"
)

// credentialPluginExec returns the exec stanza for --credential-plugin, else
// for $ATE_CREDENTIAL_PLUGIN. It returns nil when --token-file is given or
// neither names a plugin.
func credentialPluginExec() (*clientcmdapi.ExecConfig, error) {
	if tokenFile != "" {
		if credentialPlugin != "" {
			return nil, usageErrorf("--token-file cannot be combined with --credential-plugin")
		}
		return nil, nil
	}
	value := credentialPlugin
	if value == "" {
		value = os.Getenv(credentialPluginEnv)
	}
	if value == "" {
		return nil, nil
	}

	// An existing file runs with no arguments, so its path may hold spaces.
	command, args := value, []string(nil)
	if _, err := os.Stat(value); err != nil {
		words, err := splitCommandLine(value)
		if err != nil {
			return nil, usageErrorf("credential plugin: %w", err)
		}
		if len(words) == 0 {
			return nil, usageErrorf("credential plugin is blank")
		}
		command, args = words[0], words[1:]
	}
	var env []clientcmdapi.ExecEnvVar
	if kubeconfig != "" {
		env = append(env, clientcmdapi.ExecEnvVar{Name: kubeconfigEnv, Value: kubeconfig})
	}
	if k8sContext != "" {
		env = append(env, clientcmdapi.ExecEnvVar{Name: kubeContextEnv, Value: k8sContext})
	}
	return &clientcmdapi.ExecConfig{
		APIVersion:         credplugin.APIVersionV1,
		Command:            command,
		Args:               args,
		Env:                env,
		InteractiveMode:    clientcmdapi.IfAvailableExecInteractiveMode,
		ProvideClusterInfo: true,
	}, nil
}

// splitCommandLine splits s into words with shell quoting and backslash
// escapes, expanding nothing.
func splitCommandLine(s string) ([]string, error) {
	var words []string
	var word strings.Builder
	inWord := false
	var quote rune
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		r := rs[i]
		switch {
		case quote == '\'':
			if r == '\'' {
				quote = 0
			} else {
				word.WriteRune(r)
			}
		case quote == '"':
			switch {
			case r == '"':
				quote = 0
			case r == '\\' && i+1 < len(rs) && strings.ContainsRune("$`\"\\\n", rs[i+1]):
				i++
				word.WriteRune(rs[i])
			default:
				word.WriteRune(r)
			}
		case r == '\\':
			if i+1 == len(rs) {
				return nil, errors.New("trailing backslash")
			}
			i++
			word.WriteRune(rs[i])
			inWord = true
		case r == '\'' || r == '"':
			quote, inWord = r, true
		case unicode.IsSpace(r):
			if inWord {
				words = append(words, word.String())
				word.Reset()
				inWord = false
			}
		default:
			word.WriteRune(r)
			inWord = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated %c quote", quote)
	}
	if inWord {
		words = append(words, word.String())
	}
	return words, nil
}
