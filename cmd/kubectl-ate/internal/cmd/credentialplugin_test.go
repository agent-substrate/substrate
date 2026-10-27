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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.ate.dev/substrate/internal/ateclient"
)

func setFlag[T any](t *testing.T, p *T, v T) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

func TestCredentialPluginConfig(t *testing.T) {
	plugin := func(command string, args []string, kubeconfig, kubeContext string) *ateclient.CredentialPlugin {
		return &ateclient.CredentialPlugin{
			Command: command,
			Args:    args,
			Env:     []string{kubeconfigEnv + "=" + kubeconfig, kubeContextEnv + "=" + kubeContext},
		}
	}
	for _, tc := range []struct {
		name                                     string
		flag, tokenFile, kubeconfig, kubeContext string
		args                                     []string
		want                                     *ateclient.CredentialPlugin
		wantErr                                  bool
	}{
		{name: "unset"},
		{name: "token file", tokenFile: "/token"},
		{name: "token file and flag", flag: "plugin", tokenFile: "/token", wantErr: true},
		{name: "args without flag", args: []string{"--x"}, wantErr: true},
		{
			name: "args",
			flag: "kubectl-ate",
			args: []string{"service-account-token", "--service-account=ate-users/alice"},
			want: plugin("kubectl-ate", []string{"service-account-token", "--service-account=ate-users/alice"}, "", ""),
		},
		{name: "path with spaces", flag: "/opt/my plugin", want: plugin("/opt/my plugin", nil, "", "")},
		{
			name: "kube flags", flag: "plugin", kubeconfig: "/kubeconfig", kubeContext: "kind-a",
			want: plugin("plugin", nil, "/kubeconfig", "kind-a"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setFlag(t, &credentialPlugin, tc.flag)
			setFlag(t, &credentialPluginArgs, tc.args)
			setFlag(t, &tokenFile, tc.tokenFile)
			setFlag(t, &kubeconfig, tc.kubeconfig)
			setFlag(t, &k8sContext, tc.kubeContext)

			got, err := credentialPluginConfig()
			if (err != nil) != tc.wantErr {
				t.Fatalf("credentialPluginConfig() error = %v, want error %t", err, tc.wantErr)
			}
			// Stdin depends on whether the test's stdin is a terminal.
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty(), cmpopts.IgnoreFields(ateclient.CredentialPlugin{}, "Stdin")); diff != "" {
				t.Errorf("credentialPluginConfig() (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCredentialPluginArgFlag(t *testing.T) {
	resetFlagsRecursive(rootCmd)
	t.Cleanup(func() { resetFlagsRecursive(rootCmd) })
	setFlag(t, &tokenFile, "")
	if err := rootCmd.PersistentFlags().Parse([]string{
		"--credential-plugin=plugin",
		"--credential-plugin-arg=--scopes=a,b",
		"--credential-plugin-arg=--x",
	}); err != nil {
		t.Fatal(err)
	}
	got, err := credentialPluginConfig()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"--scopes=a,b", "--x"}; !cmp.Equal(got.Args, want) {
		t.Errorf("Args = %q, want %q", got.Args, want)
	}
}
