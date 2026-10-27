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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/internal/ateclient"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	clientauthv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
)

const saTokenLifetime = time.Hour

var (
	saTokenServiceAccount string
	saTokenAudience       string
	saTokenOutput         string
)

var serviceAccountTokenCmd = &cobra.Command{
	Use:   "service-account-token",
	Short: "Mint a ServiceAccount token for ate-api, as a kubectl credential plugin",
	Long: `service-account-token mints a token for ate-api for the --service-account
ServiceAccount, using your kubeconfig, and prints it as an ExecCredential for
--credential-plugin. You need permission to create tokens for that
ServiceAccount.

Without --kubeconfig it uses $` + kubeconfigEnv + `, and without --context
$` + kubeContextEnv + `; kubectl ate sets both from its own flags when it runs the
plugin.`,
	Example: `  # Act as a per-user ServiceAccount
  kubectl ate --credential-plugin=kubectl-ate --credential-plugin-arg=service-account-token \
    --credential-plugin-arg=--service-account=ate-users/alice get actors -a alice-space

  # A raw token for grpcurl
  grpcurl -H "authorization: Bearer $(kubectl ate service-account-token --service-account=ate-users/alice -o token)" ...`,
	Args: noArgs(""),
	RunE: func(cmd *cobra.Command, _ []string) error {
		if saTokenOutput != "json" && saTokenOutput != "token" {
			return usageErrorf("invalid output format %q. Must be one of: json, token", saTokenOutput)
		}
		token, expiry, err := mintSAToken(cmd)
		if err != nil {
			return err
		}
		out := cmd.OutOrStdout()
		if saTokenOutput == "token" {
			_, err := fmt.Fprintln(out, token)
			return err
		}
		exp := metav1.NewTime(expiry)
		return json.NewEncoder(out).Encode(clientauthv1.ExecCredential{
			TypeMeta: metav1.TypeMeta{APIVersion: clientauthv1.SchemeGroupVersion.String(), Kind: "ExecCredential"},
			Status:   &clientauthv1.ExecCredentialStatus{Token: token, ExpirationTimestamp: &exp},
		})
	},
}

// mintSAToken mints a token for the --service-account ServiceAccount.
func mintSAToken(cmd *cobra.Command) (string, time.Time, error) {
	namespace, name, ok := strings.Cut(saTokenServiceAccount, "/")
	if !ok || namespace == "" || name == "" {
		return "", time.Time{}, usageErrorf("--service-account must be NAMESPACE/NAME, got %q", saTokenServiceAccount)
	}
	audience := saTokenAudience
	if audience == "" {
		audience = ateclient.DefaultAudience()
	}

	kubeconfigPath, kubeContext := kubeconfig, k8sContext
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv(kubeconfigEnv)
	}
	if kubeContext == "" {
		kubeContext = os.Getenv(kubeContextEnv)
	}
	config, err := loadKubeConfig(cmd.ErrOrStderr(), kubeconfigPath, kubeContext)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("while reading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("while creating Kubernetes client: %w", err)
	}
	return ateclient.MintServiceAccountToken(cmd.Context(), clientset, namespace, name, audience, saTokenLifetime)
}

func init() {
	serviceAccountTokenCmd.Flags().StringVar(&saTokenServiceAccount, "service-account", "", "NAMESPACE/NAME of the ServiceAccount to mint a token for")
	_ = serviceAccountTokenCmd.MarkFlagRequired("service-account")
	serviceAccountTokenCmd.Flags().StringVar(&saTokenAudience, "audience", "", "audience of the minted token (defaults to ate-api's in-cluster Service name)")
	// Shadows the root --output, whose formats do not apply here.
	serviceAccountTokenCmd.Flags().StringVarP(&saTokenOutput, "output", "o", "json", "output format: json (an ExecCredential) or token (the raw bearer token)")
	rootCmd.AddCommand(serviceAccountTokenCmd)
}
