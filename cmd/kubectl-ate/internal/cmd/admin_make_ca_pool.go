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

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var caID string
var targetSecretNamespace string
var targetSecretName string

var makeCaPoolCmd = &cobra.Command{
	Use:   "make-ca-pool",
	Short: "Make a new secret that contains a CA pool to be used by a signing controller",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		kconfig, err := ateclient.LoadConfig(kubeconfig, k8sContext)
		if err != nil {
			return fmt.Errorf("while reading kubeconfig: %w", err)
		}

		kc, err := kubernetes.NewForConfig(kconfig)
		if err != nil {
			return fmt.Errorf("while creating Kubernetes client: %w", err)
		}

		ca, err := localca.GenerateED25519CA(caID)
		if err != nil {
			return fmt.Errorf("while generating CA: %w", err)
		}

		pool := &localca.Pool{
			CAs: []*localca.CA{ca},
		}

		poolBytes, err := localca.Marshal(pool)
		if err != nil {
			return fmt.Errorf("while marshaling pool: %w", err)
		}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: targetSecretNamespace,
				Name:      targetSecretName,
			},
			Data: map[string][]byte{
				"pool": poolBytes,
			},
		}

		_, err = kc.CoreV1().Secrets(targetSecretNamespace).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("while uploading pool state to secret: %w", err)
		}

		fmt.Printf("Successfully created CA pool secret %s/%s\n", targetSecretNamespace, targetSecretName)
		return nil
	},
}

func init() {
	adminCmd.AddCommand(makeCaPoolCmd)
	makeCaPoolCmd.Flags().StringVar(&caID, "ca-id", "", "The ID of the initial CA in the Pool")
	makeCaPoolCmd.Flags().StringVar(&targetSecretNamespace, "secret-namespace", "default", "Create the secret in this namespace")
	makeCaPoolCmd.Flags().StringVar(&targetSecretName, "name", "", "Create the secret with this name")
	makeCaPoolCmd.MarkFlagRequired("name")
}
