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
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
)

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Fully bootstrap the GCP environment",
	Long:  `Enables APIs, creates the cluster and bucket, grants IAM permissions, and creates dashboards. Use --create-repository to also create an image repository for source builds.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		warnDeprecatedMachineTypeEnv(cmd)
		if err := resolveProjectID(ctx, &cfg); err != nil {
			return err
		}
		if cfg.CreateArtifactRepository {
			if err := validateRepositoryFlags(ctx, &cfg); err != nil {
				return err
			}
		}
		if cfg.BucketName == "" {
			return errors.New("--bucket-name is required")
		}

		slog.Info("Starting full bootstrap...")
		total := 7
		if cfg.CreateArtifactRepository {
			total++
		}
		step := 0
		logStep := func(message string) {
			step++
			slog.Info(fmt.Sprintf("Step %d/%d: %s", step, total, message))
		}

		logStep("Enabling required APIs...")
		if err := enableRequiredAPIs(ctx, &cfg); err != nil {
			return err
		}

		if cfg.CreateArtifactRepository {
			logStep("Creating Artifact Registry repository...")
			if err := createArtifactRepository(ctx, &cfg); err != nil {
				return err
			}
		}

		logStep("Creating GKE Cluster...")
		if err := createClusterIdempotent(ctx, &cfg); err != nil {
			return err
		}

		logStep("Creating GCS Bucket for snapshots...")
		if err := createSnapshotBucket(ctx, &cfg); err != nil {
			return err
		}

		logStep("Granting GKE Node permissions...")
		if err := grantGkeNodePermissions(ctx, &cfg); err != nil {
			return err
		}

		logStep("Granting Atelet permissions...")
		if err := grantAteletPermissions(ctx, &cfg); err != nil {
			return err
		}

		logStep("Creating IAM policy bindings for bucket...")
		if err := createIamPolicyBindings(ctx, &cfg); err != nil {
			return err
		}

		logStep("Creating Monitoring Dashboards...")
		if err := createMonitoringDashboards(ctx, &cfg); err != nil {
			return err
		}

		slog.Info("Bootstrap completed successfully.")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(bootstrapCmd)

	bootstrapCmd.Flags().StringVar(&cfg.ClusterName, "cluster-name", getEnv("CLUSTER_NAME", "substrate-poc"), "Name of the GKE cluster [env: CLUSTER_NAME]")
	bootstrapCmd.Flags().StringVar(&cfg.ClusterLocation, "cluster-location", getEnv("CLUSTER_LOCATION", "us-west1-c"), "Zone or region for the cluster [env: CLUSTER_LOCATION]")
	bootstrapCmd.Flags().StringVar(&cfg.ClusterVersion, "cluster-version", getEnv("CLUSTER_VERSION", ""), "Kubernetes version [env: CLUSTER_VERSION]")
	bootstrapCmd.Flags().StringVar(&cfg.Network, "network", getEnv("NETWORK", "default"), "VPC network name [env: NETWORK]")
	bootstrapCmd.Flags().StringVar(&cfg.Subnetwork, "subnetwork", getEnv("SUBNETWORK", "default"), "VPC subnetwork name [env: SUBNETWORK]")
	bootstrapCmd.Flags().StringVar(&cfg.MachineType, "machine-type", resolveMachineTypeDefault(), "Machine type for the node pool [env: NODE_MACHINE_TYPE]")
	bootstrapCmd.Flags().BoolVar(&cfg.EnableNestedVirtualization, "enable-nested-virtualization", getEnv("ENABLE_NESTED_VIRTUALIZATION", true), "Create the node pool with nested virtualization, exposing /dev/kvm for micro-VM workers; needs a machine type that supports it. Turn off with --enable-nested-virtualization=false [env: ENABLE_NESTED_VIRTUALIZATION]")
	bootstrapCmd.Flags().Int32Var(&cfg.BootDiskSizeGB, "boot-disk-size", getEnv("BOOT_DISK_SIZE_GB", int32(0)), "Boot disk size in GB for the node pool; 0 = GKE default (100 GB) [env: BOOT_DISK_SIZE_GB]")
	bootstrapCmd.Flags().StringVar(&cfg.BootDiskType, "boot-disk-type", getEnv("BOOT_DISK_TYPE", ""), "Boot disk type for the node pool; empty = GKE default [env: BOOT_DISK_TYPE]")
	bootstrapCmd.Flags().StringVar(&cfg.BucketName, "bucket-name", getEnv("BUCKET_NAME", ""), "Name of the GCS bucket for snapshots [env: BUCKET_NAME]")
	bootstrapCmd.Flags().BoolVar(&cfg.CreateArtifactRepository, "create-repository", getEnv("CREATE_ARTIFACT_REPOSITORY", false), "Create an Artifact Registry repository for source builds [env: CREATE_ARTIFACT_REPOSITORY]")
	bootstrapCmd.Flags().StringVar(&cfg.ArtifactRegistryRepository, "repository-name", getEnv("ARTIFACT_REGISTRY_REPOSITORY", "ate-images"), "Name of the Artifact Registry Docker repository [env: ARTIFACT_REGISTRY_REPOSITORY]")
	bootstrapCmd.Flags().StringVar(&cfg.DashboardDir, "dashboard-dir", getEnv("DASHBOARD_DIR", "tools/setup-gcp/dashboards"), "Directory containing dashboard JSON files [env: DASHBOARD_DIR]")
}
