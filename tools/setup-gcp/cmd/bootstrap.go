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
	"errors"
	"log/slog"

	"github.com/spf13/cobra"
)

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Fully bootstrap the GCP environment",
	Long:  `Runs all setup steps in order: enable APIs, create cluster, create bucket, grant IAM permissions, and create dashboards.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.ProjectID == "" {
			return errors.New("--project-id is required")
		}
		if cfg.ProjectNumber == "" {
			return errors.New("--project-number is required")
		}
		if cfg.BucketName == "" {
			return errors.New("--bucket-name is required")
		}

		ctx := cmd.Context()

		slog.Info("Starting full bootstrap...")

		slog.Info("Step 1/7: Enabling required APIs...")
		if err := enableRequiredAPIs(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Step 2/7: Creating GKE Cluster...")
		if err := createClusterIdempotent(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Step 3/7: Creating GCS Bucket for snapshots...")
		if err := createSnapshotBucket(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Step 4/7: Granting GKE Node permissions...")
		if err := grantGkeNodePermissions(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Step 5/7: Granting Atelet permissions...")
		if err := grantAteletPermissions(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Step 6/7: Creating IAM policy bindings for bucket...")
		if err := createIamPolicyBindings(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Step 7/7: Creating Monitoring Dashboards...")
		if err := createMonitoringDashboards(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Bootstrap completed successfully.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(bootstrapCmd)

	// Register bootstrap-specific flags that map to Config fields.
	// We use distinct names to avoid confusion and match the desired design.
	bootstrapCmd.Flags().StringVar(&cfg.ClusterName, "cluster-name", getEnv("CLUSTER_NAME", "substrate-poc"), "Name of the GKE cluster [env: CLUSTER_NAME]")
	bootstrapCmd.Flags().StringVar(&cfg.ClusterLocation, "cluster-location", getEnv("CLUSTER_LOCATION", "us-central1-c"), "Zone or region for the cluster [env: CLUSTER_LOCATION]")
	bootstrapCmd.Flags().StringVar(&cfg.ClusterVersion, "cluster-version", getEnv("CLUSTER_VERSION", ""), "Kubernetes version [env: CLUSTER_VERSION]")
	bootstrapCmd.Flags().StringVar(&cfg.Network, "network", getEnv("NETWORK", "default"), "VPC network name [env: NETWORK]")
	bootstrapCmd.Flags().StringVar(&cfg.Subnetwork, "subnetwork", getEnv("SUBNETWORK", "default"), "VPC subnetwork name [env: SUBNETWORK]")
	bootstrapCmd.Flags().StringVar(&cfg.MachineType, "machine-type", getEnv("GVISOR_NODE_MACHINE_TYPE", "c3-standard-4"), "Machine type for the gVisor node pool [env: GVISOR_NODE_MACHINE_TYPE]")
	bootstrapCmd.Flags().StringVar(&cfg.BucketName, "bucket-name", getEnv("BUCKET_NAME", ""), "Name of the GCS bucket for snapshots [env: BUCKET_NAME]")
	bootstrapCmd.Flags().StringVar(&cfg.DashboardDir, "dashboard-dir", getEnv("DASHBOARD_DIR", "tools/setup-gcp/dashboards"), "Directory containing dashboard JSON files [env: DASHBOARD_DIR]")
}
