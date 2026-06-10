// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

var (
	createClusterFlag           bool
	createGvisorNodePoolFlag    bool
	createSnapshotBucketFlag    bool
	createIamPolicyBindingsFlag bool
	grantGkeNodePermissionsFlag bool
	grantAteletPermissionsFlag  bool
	enableApisFlag              bool
	createDashboardsFlag        bool
	allFlag                     bool
)

var rootCmd = &cobra.Command{
	Use:   "setup",
	Short: "Setup GCP resources for Agent Substrate",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		env, err := loadEnv()
		if err != nil {
			return err
		}

		setupTasks := []struct {
			name     string
			flag     *bool
			taskFunc func(context.Context, *Environment) error
		}{
			{"enable apis", &enableApisFlag, enableRequiredAPIs},
			{"create cluster", &createClusterFlag, createClusterIdempotent},
			{"create snapshot bucket", &createSnapshotBucketFlag, createSnapshotBucket},
			{"create iam policy bindings", &createIamPolicyBindingsFlag, createIamPolicyBindings},
			{"grant gke node permissions", &grantGkeNodePermissionsFlag, grantGkeNodePermissions},
			{"grant atelet permissions", &grantAteletPermissionsFlag, grantAteletPermissions},
			{"create monitoring dashboards", &createDashboardsFlag, createMonitoringDashboards},
		}

		if cmd.Flags().NFlag() == 0 {
			return cmd.Help()
		}

		if allFlag {
			if cmd.Flags().NFlag() > 1 {
				return fmt.Errorf("the --all flag cannot be used with other task-specific flags")
			}
			errs := []error{}
			for _, task := range setupTasks {
				if err := task.taskFunc(ctx, env); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", task.name, err))
				}
			}
			return errors.Join(errs...)
		}

		for _, task := range setupTasks {
			if *task.flag {
				if err := task.taskFunc(ctx, env); err != nil {
					return fmt.Errorf("%s: %w", task.name, err)
				}
			}
		}

		return nil
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.Flags().BoolVar(&createClusterFlag, "create-cluster", false, "Create GKE cluster")
	rootCmd.Flags().BoolVar(&createGvisorNodePoolFlag, "create-gvisor-node-pool", false, "Create gVisor node pool")
	rootCmd.Flags().BoolVar(&createSnapshotBucketFlag, "create-snapshot-bucket", false, "Create snapshot bucket")
	rootCmd.Flags().BoolVar(&createIamPolicyBindingsFlag, "create-iam-policy-bindings", false, "Create IAM policy bindings for atelet")
	rootCmd.Flags().BoolVar(&grantGkeNodePermissionsFlag, "grant-gke-node-permissions", false, "Grant GKE nodes permission to pull images")
	rootCmd.Flags().BoolVar(&grantAteletPermissionsFlag, "grant-atelet-permissions", false, "Grant atelet permission to read/write snapshots and pull images")
	rootCmd.Flags().BoolVar(&enableApisFlag, "enable-apis", false, "Enable required Google Cloud APIs")
	rootCmd.Flags().BoolVar(&createDashboardsFlag, "create-monitoring-dashboards", false, "Create/update Cloud Monitoring dashboards")
	rootCmd.Flags().BoolVar(&allFlag, "all", false, "Run all setup steps")
}
