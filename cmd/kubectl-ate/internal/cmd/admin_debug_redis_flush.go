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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
)

var debugRedisFlushCmd = &cobra.Command{
	Use:   "debug-flush-redis",
	Short: "DANGEROUS: Flush all data from Redis",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
		if err != nil {
			return fmt.Errorf("failed to connect to ate-api-server: %w", err)
		}
		defer apiClient.Close()

		_, err = apiClient.DebugClear(ctx, &ateapipb.DebugClearRequest{})
		if err != nil {
			return fmt.Errorf("failed to flush redis: %w", err)
		}

		fmt.Println("Successfully flushed all data from Redis.")
		return nil
	},
}

func init() {
	adminCmd.AddCommand(debugRedisFlushCmd)
}
