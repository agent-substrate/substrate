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
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func setString(t *testing.T, p *string, v string) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

func TestCredentialPluginExec(t *testing.T) {
	spaced := filepath.Join(t.TempDir(), "my plugin")
	if err := os.WriteFile(spaced, nil, 0o700); err != nil {
		t.Fatal(err)
	}
	execConfig := func(command string, args []string, env ...clientcmdapi.ExecEnvVar) *clientcmdapi.ExecConfig {
		return &clientcmdapi.ExecConfig{
			APIVersion:         "client.authentication.k8s.io/v1",
			Command:            command,
			Args:               args,
			Env:                env,
			InteractiveMode:    clientcmdapi.IfAvailableExecInteractiveMode,
			ProvideClusterInfo: true,
		}
	}
	for _, tc := range []struct {
		name                                          string
		flag, env, tokenFile, kubeconfig, kubeContext string
		want                                          *clientcmdapi.ExecConfig
		wantErr                                       bool
	}{
		{name: "unset"},
		{name: "token file over env", env: "plugin", tokenFile: "/token"},
		{name: "token file and flag", flag: "plugin", tokenFile: "/token", wantErr: true},
		{
			name: "command line",
			flag: "kubectl-ate credential --service-account='ate-users/alice'",
			want: execConfig("kubectl-ate", []string{"credential", "--service-account=ate-users/alice"}),
		},
		{name: "env", env: "plugin --x", want: execConfig("plugin", []string{"--x"})},
		{name: "flag over env", flag: "a", env: "b", want: execConfig("a", nil)},
		{name: "existing file", flag: spaced, want: execConfig(spaced, nil)},
		{
			name: "kube flags", flag: spaced, kubeconfig: "/kubeconfig", kubeContext: "kind-a",
			want: execConfig(spaced, nil,
				clientcmdapi.ExecEnvVar{Name: kubeconfigEnv, Value: "/kubeconfig"},
				clientcmdapi.ExecEnvVar{Name: kubeContextEnv, Value: "kind-a"}),
		},
		{name: "unterminated quote", flag: `plugin "x`, wantErr: true},
		{name: "blank", flag: "  ", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(credentialPluginEnv, tc.env)
			setString(t, &credentialPlugin, tc.flag)
			setString(t, &tokenFile, tc.tokenFile)
			setString(t, &kubeconfig, tc.kubeconfig)
			setString(t, &k8sContext, tc.kubeContext)

			got, err := credentialPluginExec()
			if (err != nil) != tc.wantErr {
				t.Fatalf("credentialPluginExec() error = %v, want error %t", err, tc.wantErr)
			}
			if diff := cmp.Diff(tc.want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("credentialPluginExec() (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSplitCommandLine(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{" a  b\tc ", []string{"a", "b", "c"}},
		{`'a b' "c d"`, []string{"a b", "c d"}},
		{`--flag="a b"`, []string{"--flag=a b"}},
		{`a\ b`, []string{"a b"}},
		{`"a\"b" "C:\path"`, []string{`a"b`, `C:\path`}},
		{`'a\b' 'it''s' ''`, []string{`a\b`, "its", ""}},
		{"x#y $HOME", []string{"x#y", "$HOME"}},
	} {
		got, err := splitCommandLine(tc.in)
		if err != nil {
			t.Errorf("splitCommandLine(%q): %v", tc.in, err)
			continue
		}
		if diff := cmp.Diff(tc.want, got); diff != "" {
			t.Errorf("splitCommandLine(%q) (-want +got):\n%s", tc.in, diff)
		}
	}
	for _, in := range []string{`"a`, `'a`, `a\`} {
		if got, err := splitCommandLine(in); err == nil {
			t.Errorf("splitCommandLine(%q) = %q, want an error", in, got)
		}
	}
}
