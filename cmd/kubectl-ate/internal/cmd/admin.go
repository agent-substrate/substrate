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
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var (
	// Both pool commands write one secret, so they share the flags naming it.
	poolSecretNamespaceFlag string
	poolSecretNameFlag      string
	makeCaPoolIDFlag        string
	makeCaPoolKeyTypeFlag   string
	makeJwtPoolKeyIDFlag    string
)

var adminCmd = &cobra.Command{
	Use:   "admin",
	Short: "Administration and debugging commands",
}

var makeCaPoolCmd = &cobra.Command{
	Use:   "make-ca-pool",
	Short: "Make a new secret that contains a CA pool to be used by a signing controller",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		kconfig, err := ateclient.LoadKubeConfig(kubeconfig, k8sContext)
		if err != nil {
			return fmt.Errorf("while reading kubeconfig: %w", err)
		}

		kc, err := kubernetes.NewForConfig(kconfig)
		if err != nil {
			return fmt.Errorf("while creating Kubernetes client: %w", err)
		}

		var keyType localca.KeyType
		switch makeCaPoolKeyTypeFlag {
		case "ED25519":
			keyType = localca.KeyTypeED25519
		case "ECDSAP256":
			keyType = localca.KeyTypeECDSAP256
		default:
			return fmt.Errorf("unknown key type %q", makeCaPoolKeyTypeFlag)
		}

		ca, err := localca.GenerateCA(
			makeCaPoolIDFlag,
			keyType,
			365*24*time.Hour,
		)
		if err != nil {
			return fmt.Errorf("while generating CA: %w", err)
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
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		kconfig, err := ateclient.LoadKubeConfig(kubeconfig, k8sContext)
		if err != nil {
			return fmt.Errorf("while reading kubeconfig: %w", err)
		}

		kc, err := kubernetes.NewForConfig(kconfig)
		if err != nil {
			return fmt.Errorf("while creating Kubernetes client: %w", err)
		}

		authority, err := localjwtauthority.GenerateECDSAP256Authority(makeJwtPoolKeyIDFlag)
		if err != nil {
			return fmt.Errorf("while generating JWT authority: %w", err)
		}

		pool := &localjwtauthority.ConcretePool{
			Authorities:      []*localjwtauthority.Authority{authority},
			ActiveForSigning: makeJwtPoolKeyIDFlag,
		}

		poolBytes, err := localjwtauthority.Marshal(pool)
		if err != nil {
			return fmt.Errorf("while marshaling pool: %w", err)
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: poolSecretNamespaceFlag,
				Name:      poolSecretNameFlag,
			},
			Data: map[string][]byte{
				"pool": poolBytes,
			},
		}

		_, err = kc.CoreV1().Secrets(poolSecretNamespaceFlag).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("while uploading pool state to secret: %w", err)
		}

		fmt.Printf("Successfully created JWT authority pool secret %s/%s\n", poolSecretNamespaceFlag, poolSecretNameFlag)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(adminCmd)

	makeCaPoolCmd.Flags().StringVar(&makeCaPoolIDFlag, "ca-id", "", "The ID of the initial CA in the Pool")
	makeCaPoolCmd.Flags().StringVar(&poolSecretNamespaceFlag, "secret-namespace", "default", "Create the secret in this namespace")
	makeCaPoolCmd.Flags().StringVar(&poolSecretNameFlag, "name", "", "Create the secret with this name")
	makeCaPoolCmd.Flags().StringVar(&makeCaPoolKeyTypeFlag, "key-type", "ED25519", "CA key type.  One of [ED25519, ECDSAP256]")
	_ = makeCaPoolCmd.MarkFlagRequired("name")
	adminCmd.AddCommand(makeCaPoolCmd)

	makeJwtPoolCmd.Flags().StringVar(&makeJwtPoolKeyIDFlag, "key-id", "1", "The ID of the initial JWT signing key in the pool")
	makeJwtPoolCmd.Flags().StringVar(&poolSecretNamespaceFlag, "secret-namespace", "default", "Create the secret in this namespace")
	makeJwtPoolCmd.Flags().StringVar(&poolSecretNameFlag, "name", "", "Create the secret with this name")
	_ = makeJwtPoolCmd.MarkFlagRequired("name")
	adminCmd.AddCommand(makeJwtPoolCmd)
}
