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
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/credplugin"
	"go.ate.dev/substrate/internal/ateclient"
	"k8s.io/client-go/kubernetes"
)

const (
	credentialLifetime = time.Hour
	// credentialMinRemaining is how long a cached token must still be valid
	// to be handed out again.
	credentialMinRemaining = 5 * time.Minute
)

var (
	credentialServiceAccount string
	credentialAudience       string
	credentialOutput         string
	credentialRefresh        bool

	// mintServiceAccountToken and newCredentialCache are replaced in tests.
	mintServiceAccountToken = ateclient.MintServiceAccountToken
	newCredentialCache      = credplugin.DefaultCache
)

var credentialCmd = &cobra.Command{
	Use:   "credential",
	Short: "Print a credential for ate-api, as a kubeconfig exec credential plugin",
	Long: `credential mints a Kubernetes ServiceAccount token for ate-api and prints it
as an ExecCredential, so it can be passed to --credential-plugin (or named in a
kubeconfig exec stanza). Tokens are cached under ~/.ate/cache until shortly
before they expire.

Without --kubeconfig it uses $` + kubeconfigEnv + `, and without --context
$` + kubeContextEnv + `; kubectl ate sets both from its own flags when it runs the
plugin. The audience is --audience, else spec.cluster.config.audience from
KUBERNETES_EXEC_INFO, else ate-api's in-cluster Service name.`,
	Example: `  # Act as a per-user ServiceAccount you are allowed to create tokens for
  export ` + credentialPluginEnv + `="kubectl-ate credential --service-account=ate-users/alice"
  kubectl ate whoami

  # Print the shared ate-client ServiceAccount's credential
  kubectl ate credential --context=my-cluster

  # A raw token for grpcurl
  grpcurl -H "authorization: Bearer $(kubectl ate credential -o token)" ...`,
	Args: noArgs(""),
	RunE: func(cmd *cobra.Command, _ []string) error {
		info, err := credentialInfo()
		if err != nil {
			return err
		}
		token, expiry, err := serviceAccountCredential(cmd, info.Audience)
		if err != nil {
			return err
		}
		return writeCredential(cmd.OutOrStdout(), info, token, expiry)
	},
}

var credentialWrapCmd = &cobra.Command{
	Use:   "wrap -- COMMAND [ARG...]",
	Short: "Run a command that prints a JWT and print it as an ExecCredential",
	Long: `wrap runs COMMAND, reads a JWT from its standard output, and prints it as an
ExecCredential that expires at the JWT's exp claim. The token is not cached:
COMMAND is run every time the client needs a credential.

Only wrap commands whose issuer limits who can obtain a token for ate-api's
configured audience; see docs/authentication.md.`,
	Example: `  kubectl ate credential wrap -- my-corp-sso print-token --audience=ate`,
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		info, err := credentialInfo()
		if err != nil {
			return err
		}
		c := exec.CommandContext(cmd.Context(), args[0], args[1:]...)
		c.Stderr = cmd.ErrOrStderr()
		if info.Interactive {
			c.Stdin = os.Stdin
		}
		out, err := c.Output()
		if err != nil {
			return fmt.Errorf("run %s: %w", args[0], err)
		}
		token := strings.TrimSpace(string(out))
		if token == "" {
			return fmt.Errorf("%s printed no token", args[0])
		}
		expiry, err := credplugin.JWTExpiry(token)
		if err != nil {
			return fmt.Errorf("%s did not print a JWT: %w", args[0], err)
		}
		return writeCredential(cmd.OutOrStdout(), info, token, expiry)
	},
}

func credentialInfo() (credplugin.Info, error) {
	if credentialOutput != "json" && credentialOutput != "token" {
		return credplugin.Info{}, usageErrorf("invalid output format %q. Must be one of: json, token", credentialOutput)
	}
	return credplugin.ParseInfo(os.Getenv(credplugin.InfoEnv))
}

func writeCredential(out io.Writer, info credplugin.Info, token string, expiry time.Time) error {
	if credentialOutput == "token" {
		_, err := fmt.Fprintln(out, token)
		return err
	}
	return credplugin.WriteCredential(out, info.APIVersion, token, expiry)
}

// serviceAccountCredential returns a cached or newly minted token for the
// --service-account ServiceAccount.
func serviceAccountCredential(cmd *cobra.Command, execAudience string) (string, time.Time, error) {
	errOut := cmd.ErrOrStderr()
	namespace, name := ateclient.DefaultServiceAccount()
	if credentialServiceAccount != "" {
		var ok bool
		namespace, name, ok = strings.Cut(credentialServiceAccount, "/")
		if !ok || namespace == "" || name == "" {
			return "", time.Time{}, usageErrorf("--service-account must be NAMESPACE/NAME, got %q", credentialServiceAccount)
		}
	} else {
		warnSharedIdentityCredential(errOut)
	}
	audience := credentialAudience
	if audience == "" {
		audience = execAudience
	}
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
	config, err := loadKubeConfig(errOut, kubeconfigPath, kubeContext)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("while reading kubeconfig: %w", err)
	}
	// Keyed by the kubeconfig too: whoever can mint here is not necessarily
	// whoever can mint through another kubeconfig context on the same host.
	key := []string{config.Host, kubeconfigPath, kubeContext, namespace, name, audience}
	cache, err := newCredentialCache()
	if err != nil {
		vlogf(errOut, vBasic, "Not caching the credential: %v", err)
	} else if !credentialRefresh && os.Getenv(ateclient.RefreshCredentialEnv) == "" {
		if token, expiry, ok := cache.Get(key, credentialMinRemaining); ok {
			vlogf(errOut, vFlow, "Using the cached token for %s/%s, valid until %s", namespace, name, expiry.Format(time.RFC3339))
			return token, expiry, nil
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("while creating Kubernetes client: %w", err)
	}
	token, expiry, err := mintServiceAccountToken(cmd.Context(), clientset, namespace, name, audience, credentialLifetime)
	if err != nil {
		return "", time.Time{}, err
	}
	if cache != nil {
		if err := cache.Put(key, token, expiry); err != nil {
			vlogf(errOut, vBasic, "Failed to cache the credential: %v", err)
		}
	}
	return token, expiry, nil
}

// warnSharedIdentityCredential warns that the token is for the shared
// fallback ServiceAccount; see warnSharedIdentity.
func warnSharedIdentityCredential(errOut io.Writer) {
	fmt.Fprintf(errOut, "Warning: using a token for the shared identity %s rather than one for you. Pass --service-account to use a ServiceAccount of your own.\n",
		ateclient.FallbackPrincipal())
}

func init() {
	credentialCmd.Flags().StringVar(&credentialServiceAccount, "service-account", "", "NAMESPACE/NAME of the ServiceAccount to mint a token for (defaults to the shared ate-client ServiceAccount)")
	credentialCmd.Flags().StringVar(&credentialAudience, "audience", "", "audience of the minted token (overrides KUBERNETES_EXEC_INFO)")
	credentialCmd.Flags().BoolVar(&credentialRefresh, "refresh", false, "mint a new token and replace the cached one")
	// Shadows the root --output, whose formats do not apply here.
	credentialCmd.PersistentFlags().StringVarP(&credentialOutput, "output", "o", "json", "output format: json (an ExecCredential) or token (the raw bearer token)")
	credentialCmd.AddCommand(credentialWrapCmd)
	rootCmd.AddCommand(credentialCmd)
}
