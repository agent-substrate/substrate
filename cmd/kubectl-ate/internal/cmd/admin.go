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
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/localjwtauthority"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/util/retry"
)

var (
	// Each pool command writes one secret, so they share the flags naming it.
	poolSecretNamespaceFlag string
	poolSecretNameFlag      string
	makeCaPoolIDFlag        string
	makeCaPoolKeyTypeFlag   string
	makeCaPoolValidityFlag  time.Duration
	jwtAlgFlag              string
	jwtKeyIDFlag            string
)

var adminCmd = &cobra.Command{
	Use:   "admin",
	Short: "Administration and debugging commands",
}

var makeCaPoolCmd = &cobra.Command{
	Use:   "make-ca-pool",
	Short: "Make a new secret that contains a CA pool to be used by a signing controller",
	Args:  noArgs("use --name to specify the secret name", "name"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		var keyType localca.KeyType
		switch makeCaPoolKeyTypeFlag {
		case "ED25519":
			keyType = localca.KeyTypeED25519
		case "ECDSAP256":
			keyType = localca.KeyTypeECDSAP256
		default:
			return usageErrorf("unknown key type %q", makeCaPoolKeyTypeFlag)
		}

		ca, err := localca.GenerateCA(
			makeCaPoolIDFlag,
			keyType,
			makeCaPoolValidityFlag,
		)
		if err != nil {
			return fmt.Errorf("while generating CA: %w", err)
		}

		kc, err := newKubeClient(cmd.ErrOrStderr())
		if err != nil {
			return err
		}

		pool := &localca.ConcretePool{
			CAs:              []*localca.CA{ca},
			ActiveForSigning: makeCaPoolIDFlag,
		}

		poolBytes, err := localca.Marshal(pool)
		if err != nil {
			return fmt.Errorf("while marshaling pool: %w", err)
		}
		certificateChain, err := ca.TLSCertificateChainPEM()
		if err != nil {
			return fmt.Errorf("while encoding CA certificate chain: %w", err)
		}
		privateKey, err := ca.TLSPrivateKeyPEM()
		if err != nil {
			return fmt.Errorf("while encoding CA private key: %w", err)
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: poolSecretNamespaceFlag,
				Name:      poolSecretNameFlag,
			},
			Type: corev1.SecretTypeTLS,
			Data: map[string][]byte{
				"pool":                  poolBytes,
				corev1.TLSCertKey:       certificateChain,
				corev1.TLSPrivateKeyKey: privateKey,
			},
		}

		_, err = kc.CoreV1().Secrets(poolSecretNamespaceFlag).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("while uploading pool state to secret: %w", err)
		}

		fmt.Printf("Successfully created CA pool secret %s/%s\n", poolSecretNamespaceFlag, poolSecretNameFlag)
		return nil
	},
}

var makeJwtPoolCmd = &cobra.Command{
	Use:   "make-jwt-pool",
	Short: "Make a new secret that contains a JWT authority pool to be used by the actor ID broker",
	Args:  noArgs("use --name to specify the secret name", "name"),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		secret, keyID, err := newJWTPoolSecret(poolSecretNamespaceFlag, poolSecretNameFlag, jwtAlgFlag, jwtKeyIDFlag)
		if err != nil {
			return wrapUsageError(err)
		}

		kc, err := newKubeClient(cmd.ErrOrStderr())
		if err != nil {
			return err
		}

		_, err = kc.CoreV1().Secrets(poolSecretNamespaceFlag).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("while uploading pool state to secret: %w", err)
		}

		fmt.Printf("Successfully created JWT authority pool secret %s/%s with %s key %s\n", poolSecretNamespaceFlag, poolSecretNameFlag, jwtAlgFlag, keyID)
		return nil
	},
}

var listJwtKeysCmd = &cobra.Command{
	Use:   "list-jwt-keys",
	Short: "List the keys in a JWT authority pool secret",
	Args:  noArgs("use --name to specify the secret name", "name"),
	RunE: func(cmd *cobra.Command, args []string) error {
		kc, err := newKubeClient(cmd.ErrOrStderr())
		if err != nil {
			return err
		}

		_, pool, err := getJWTPool(cmd.Context(), kc.CoreV1().Secrets(poolSecretNamespaceFlag), poolSecretNameFlag)
		if err != nil {
			return err
		}
		return printJWTKeys(cmd.OutOrStdout(), pool)
	},
}

var addJwtKeyCmd = &cobra.Command{
	Use:   "add-jwt-key",
	Short: "Add an inactive signing key to a JWT authority pool secret",
	Long: `Add an inactive signing key to a JWT authority pool secret.

The key is published to relying parties but signs nothing until activate-jwt-key
makes it the signing key. Activate it only once relying parties have refetched
the key set.`,
	Args: noArgs("use --name to specify the secret name", "name"),
	RunE: func(cmd *cobra.Command, args []string) error {
		authority, err := localjwtauthority.GenerateAuthority(jwtAlgFlag, jwtKeyIDFlag)
		if err != nil {
			return wrapUsageError(fmt.Errorf("while generating JWT authority: %w", err))
		}

		kc, err := newKubeClient(cmd.ErrOrStderr())
		if err != nil {
			return err
		}

		if err := updateJWTPool(cmd.Context(), cmd.ErrOrStderr(), kc.CoreV1().Secrets(poolSecretNamespaceFlag), poolSecretNameFlag, func(pool *localjwtauthority.ConcretePool) error {
			return pool.AddAuthority(authority)
		}); err != nil {
			return err
		}

		fmt.Printf("Added inactive %s key %s to JWT authority pool secret %s/%s\n", jwtAlgFlag, authority.ID, poolSecretNamespaceFlag, poolSecretNameFlag)
		return nil
	},
}

var activateJwtKeyCmd = &cobra.Command{
	Use:   "activate-jwt-key",
	Short: "Make a key in a JWT authority pool secret the one that signs",
	Args:  noArgs("use --name and --key-id to specify the secret and key", "name", "key-id"),
	RunE: func(cmd *cobra.Command, args []string) error {
		kc, err := newKubeClient(cmd.ErrOrStderr())
		if err != nil {
			return err
		}

		if err := updateJWTPool(cmd.Context(), cmd.ErrOrStderr(), kc.CoreV1().Secrets(poolSecretNamespaceFlag), poolSecretNameFlag, func(pool *localjwtauthority.ConcretePool) error {
			return pool.Activate(jwtKeyIDFlag)
		}); err != nil {
			return err
		}

		fmt.Printf("Activated key %s in JWT authority pool secret %s/%s\n", jwtKeyIDFlag, poolSecretNamespaceFlag, poolSecretNameFlag)
		return nil
	},
}

var removeJwtKeyCmd = &cobra.Command{
	Use:   "remove-jwt-key",
	Short: "Remove an inactive key from a JWT authority pool secret",
	Long: `Remove an inactive key from a JWT authority pool secret.

Tokens the key signed stop verifying once relying parties refetch the key set,
so remove it only after the last of them has expired.`,
	Args: noArgs("use --name and --key-id to specify the secret and key", "name", "key-id"),
	RunE: func(cmd *cobra.Command, args []string) error {
		kc, err := newKubeClient(cmd.ErrOrStderr())
		if err != nil {
			return err
		}

		if err := updateJWTPool(cmd.Context(), cmd.ErrOrStderr(), kc.CoreV1().Secrets(poolSecretNamespaceFlag), poolSecretNameFlag, func(pool *localjwtauthority.ConcretePool) error {
			return pool.RemoveAuthority(jwtKeyIDFlag)
		}); err != nil {
			return err
		}

		fmt.Printf("Removed key %s from JWT authority pool secret %s/%s\n", jwtKeyIDFlag, poolSecretNamespaceFlag, poolSecretNameFlag)
		return nil
	},
}

// updateJWTPool applies change to the pool in the named secret and writes it
// back. The write is conditional on the secret's resourceVersion, and a
// conflict reruns change against the current pool.
func updateJWTPool(ctx context.Context, errOut io.Writer, secrets typedcorev1.SecretInterface, name string, change func(*localjwtauthority.ConcretePool) error) error {
	attempt := 0
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		attempt++
		if attempt > 1 {
			vlogf(errOut, vBasic, "Conflict updating secret %q (attempt %d); retrying...", name, attempt)
		}
		secret, pool, err := getJWTPool(ctx, secrets, name)
		if err != nil {
			return err
		}

		if err := change(pool); err != nil {
			return err
		}

		wire, err := localjwtauthority.Marshal(pool)
		if err != nil {
			return fmt.Errorf("while marshaling pool: %w", err)
		}
		jwks, err := pool.JWKS()
		if err != nil {
			return fmt.Errorf("while publishing the pool's public keys: %w", err)
		}
		// Written together so a verifier reading "jwks" never lags "pool".
		secret.Data["pool"] = wire
		secret.Data["jwks"] = jwks
		if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("while writing pool secret: %w", err)
		}
		return nil
	})
}

// getJWTPool reads the named secret and parses the pool it holds.
func getJWTPool(ctx context.Context, secrets typedcorev1.SecretInterface, name string) (*corev1.Secret, *localjwtauthority.ConcretePool, error) {
	secret, err := secrets.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("while reading pool secret: %w", err)
	}
	wire, ok := secret.Data["pool"]
	if !ok {
		return nil, nil, fmt.Errorf("secret %s/%s has no \"pool\" key", secret.Namespace, secret.Name)
	}
	pool, err := localjwtauthority.Unmarshal(wire)
	if err != nil {
		return nil, nil, fmt.Errorf("while parsing pool: %w", err)
	}
	return secret, pool, nil
}

// printJWTKeys prints the ID and algorithm of every key in the pool, marking
// the one that signs. It prints nothing about the private keys.
func printJWTKeys(out io.Writer, pool *localjwtauthority.ConcretePool) error {
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ACTIVE\tKEY ID\tALGORITHM")
	for _, authority := range pool.Authorities {
		active := ""
		if authority.ID == pool.ActiveID() {
			active = "*"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", active, authority.ID, authority.Algorithm)
	}
	return w.Flush()
}

// newKubeClient builds a client from the --kubeconfig and --context flags.
func newKubeClient(errOut io.Writer) (kubernetes.Interface, error) {
	kconfig, err := loadKubeConfig(errOut, kubeconfig, k8sContext)
	if err != nil {
		return nil, fmt.Errorf("while reading kubeconfig: %w", err)
	}
	kc, err := kubernetes.NewForConfig(kconfig)
	if err != nil {
		return nil, fmt.Errorf("while creating Kubernetes client: %w", err)
	}
	return kc, nil
}

// newJWTPoolSecret builds a Secret holding a pool with one active authority,
// and returns the authority's key ID.
func newJWTPoolSecret(namespace, name, algorithm, keyID string) (*corev1.Secret, string, error) {
	poolBytes, id, err := localjwtauthority.GeneratePool(algorithm, keyID)
	if err != nil {
		return nil, "", fmt.Errorf("while generating JWT authority pool: %w", err)
	}
	jwks, err := localjwtauthority.PublicJWKS(poolBytes)
	if err != nil {
		return nil, "", fmt.Errorf("while publishing the pool's public keys: %w", err)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string][]byte{
			"pool": poolBytes,
			"jwks": jwks,
		},
	}, id, nil
}

func init() {
	requireSubcommand(adminCmd)
	rootCmd.AddCommand(adminCmd)

	makeCaPoolCmd.Flags().StringVar(&makeCaPoolIDFlag, "ca-id", "", "ID of the initial CA in the pool")
	makeCaPoolCmd.Flags().StringVar(&poolSecretNamespaceFlag, "secret-namespace", "default", "namespace of the pool secret")
	makeCaPoolCmd.Flags().StringVar(&poolSecretNameFlag, "name", "", "name of the pool secret")
	makeCaPoolCmd.Flags().StringVar(&makeCaPoolKeyTypeFlag, "key-type", "ED25519", "CA key type: ED25519 or ECDSAP256")
	makeCaPoolCmd.Flags().DurationVar(&makeCaPoolValidityFlag, "validity", 365*24*time.Hour, "how long the CA certificate is valid for")
	adminCmd.AddCommand(makeCaPoolCmd)

	makeJwtPoolCmd.Flags().StringVar(&jwtAlgFlag, "alg", "ES256", "signing algorithm of the initial key: ES256 or RS256 (4096-bit RSA)")
	makeJwtPoolCmd.Flags().StringVar(&jwtKeyIDFlag, "key-id", "", "ID of the initial JWT signing key in the pool (defaults to the base64url SHA-256 of the key's PKIX encoding)")
	makeJwtPoolCmd.Flags().StringVar(&poolSecretNamespaceFlag, "secret-namespace", "default", "namespace of the pool secret")
	makeJwtPoolCmd.Flags().StringVar(&poolSecretNameFlag, "name", "", "name of the pool secret")
	adminCmd.AddCommand(makeJwtPoolCmd)

	listJwtKeysCmd.Flags().StringVar(&poolSecretNamespaceFlag, "secret-namespace", "default", "namespace of the pool secret")
	listJwtKeysCmd.Flags().StringVar(&poolSecretNameFlag, "name", "", "name of the pool secret")
	adminCmd.AddCommand(listJwtKeysCmd)

	addJwtKeyCmd.Flags().StringVar(&jwtAlgFlag, "alg", "ES256", "signing algorithm of the new key: ES256 or RS256 (4096-bit RSA)")
	addJwtKeyCmd.Flags().StringVar(&jwtKeyIDFlag, "key-id", "", "ID of the new key (defaults to the base64url SHA-256 of the key's PKIX encoding)")
	addJwtKeyCmd.Flags().StringVar(&poolSecretNamespaceFlag, "secret-namespace", "default", "namespace of the pool secret")
	addJwtKeyCmd.Flags().StringVar(&poolSecretNameFlag, "name", "", "name of the pool secret")
	adminCmd.AddCommand(addJwtKeyCmd)

	for _, cmd := range []*cobra.Command{activateJwtKeyCmd, removeJwtKeyCmd} {
		cmd.Flags().StringVar(&jwtKeyIDFlag, "key-id", "", "ID of the key")
		cmd.Flags().StringVar(&poolSecretNamespaceFlag, "secret-namespace", "default", "namespace of the pool secret")
		cmd.Flags().StringVar(&poolSecretNameFlag, "name", "", "name of the pool secret")
		adminCmd.AddCommand(cmd)
	}
}
