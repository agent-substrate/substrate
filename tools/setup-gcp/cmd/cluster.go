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
	"log/slog"
	"slices"
	"strings"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

var requiredBetaAPIs = []string{
	"certificates.k8s.io/v1beta1/podcertificaterequests",
	"certificates.k8s.io/v1beta1/clustertrustbundles",
}

func deleteCluster(ctx context.Context, cfg *Config) error {
	client, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", cfg.ProjectID, cfg.ClusterLocation, cfg.ClusterName)
	slog.Info("Deleting cluster", slog.String("cluster", cfg.ClusterName))
	op, err := client.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}
	return waitContainerOperation(ctx, client, op.Name, cfg)
}

func createClusterInternal(ctx context.Context, cfg *Config, client *container.ClusterManagerClient, parent string) error {
	slog.Info("Cluster does not exist. Creating...", slog.String("cluster", cfg.ClusterName))
	var networkConfig *containerpb.NetworkConfig
	if cfg.EnableDataplaneV2 {
		networkConfig = &containerpb.NetworkConfig{
			DatapathProvider: containerpb.DatapathProvider_ADVANCED_DATAPATH,
		}
	}
	req := &containerpb.CreateClusterRequest{
		Parent: parent,
		Cluster: &containerpb.Cluster{
			Name:                  cfg.ClusterName,
			InitialClusterVersion: cfg.ClusterVersion,
			NodePools: []*containerpb.NodePool{
				{
					Name:             "substrate-node-pool",
					InitialNodeCount: 2,
					Config: &containerpb.NodeConfig{
						MachineType: cfg.MachineType,
					},
				},
			},
			EnableK8SBetaApis: &containerpb.K8SBetaAPIConfig{
				EnabledApis: requiredBetaAPIs,
			},
			WorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{
				WorkloadPool: fmt.Sprintf("%s.svc.id.goog", cfg.ProjectID),
			},
			Network:                    cfg.Network,
			Subnetwork:                 cfg.Subnetwork,
			NetworkConfig:              networkConfig,
			ManagedOpentelemetryConfig: &containerpb.ManagedOpenTelemetryConfig{Scope: containerpb.ManagedOpenTelemetryConfig_COLLECTION_AND_INSTRUMENTATION_COMPONENTS.Enum()},
		},
	}
	op, err := client.CreateCluster(ctx, req)
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	return waitContainerOperation(ctx, client, op.Name, cfg)
}

func createClusterIdempotent(ctx context.Context, cfg *Config) error {
	client, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	parent := fmt.Sprintf("projects/%s/locations/%s", cfg.ProjectID, cfg.ClusterLocation)
	clusterName := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", cfg.ProjectID, cfg.ClusterLocation, cfg.ClusterName)

	slog.Info("Checking if cluster exists", slog.String("cluster", cfg.ClusterName), slog.String("location", cfg.ClusterLocation))
	cluster, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: clusterName})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting cluster: %w", err)
		}

		return createClusterInternal(ctx, cfg, client, parent)
	}

	slog.Info("Cluster exists. Checking attributes...", slog.String("cluster", cfg.ClusterName))

	// Recreate cluster if network configuration mismatches.
	expectedNetwork := fmt.Sprintf("projects/%s/global/networks/%s", cfg.ProjectID, cfg.Network)
	if cluster.NetworkConfig != nil && cluster.NetworkConfig.Network != "" && !strings.HasSuffix(cluster.NetworkConfig.Network, expectedNetwork) {
		slog.Info("Mismatch in network", slog.String("current", cluster.NetworkConfig.Network), slog.String("expected", expectedNetwork))
		if err := deleteCluster(ctx, cfg); err != nil {
			return err
		}
		return createClusterInternal(ctx, cfg, client, parent)
	}

	// Recreate cluster if subnet configuration mismatches.
	expectedSubnetwork := fmt.Sprintf("projects/%s/regions/%s/subnetworks/%s", cfg.ProjectID, cfg.Region, cfg.Subnetwork)
	if cluster.NetworkConfig != nil && cluster.NetworkConfig.Subnetwork != "" && !strings.HasSuffix(cluster.NetworkConfig.Subnetwork, expectedSubnetwork) {
		slog.Info("Mismatch in subnetwork", slog.String("current", cluster.NetworkConfig.Subnetwork), slog.String("expected", expectedSubnetwork))
		if err := deleteCluster(ctx, cfg); err != nil {
			return err
		}
		return createClusterInternal(ctx, cfg, client, parent)
	}

	// Recreate cluster if dataplane v2 configuration mismatches.
	currentIsV2 := cluster.NetworkConfig != nil && cluster.NetworkConfig.DatapathProvider == containerpb.DatapathProvider_ADVANCED_DATAPATH
	if currentIsV2 != cfg.EnableDataplaneV2 {
		slog.Info("Mismatch in Dataplane V2 configuration", slog.Bool("current", currentIsV2), slog.Bool("expected", cfg.EnableDataplaneV2))
		if err := deleteCluster(ctx, cfg); err != nil {
			return err
		}
		return createClusterInternal(ctx, cfg, client, parent)
	}

	expectedWorkloadPool := fmt.Sprintf("%s.svc.id.goog", cfg.ProjectID)
	currentWorkloadPool := ""
	if cluster.WorkloadIdentityConfig != nil {
		currentWorkloadPool = cluster.WorkloadIdentityConfig.WorkloadPool
	}
	if currentWorkloadPool != expectedWorkloadPool {
		slog.Info("Mismatch in workload pool", slog.String("current", currentWorkloadPool), slog.String("expected", expectedWorkloadPool))
		slog.Info("Updating cluster WorkloadIdentityConfig...")
		op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
			Name: clusterName,
			Update: &containerpb.ClusterUpdate{
				DesiredWorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{
					WorkloadPool: expectedWorkloadPool,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("update cluster workload identity: %w", err)
		}
		if err := waitContainerOperation(ctx, client, op.Name, cfg); err != nil {
			return err
		}
	} else {
		slog.Info("Cluster WorkloadIdentityConfig match perfectly.", slog.String("cluster", cfg.ClusterName))
	}

	if cluster.EnableK8SBetaApis == nil ||
		len(cluster.EnableK8SBetaApis.EnabledApis) == 0 ||
		!containsAll(cluster.EnableK8SBetaApis.EnabledApis, requiredBetaAPIs) {

		clusterEnabledAPIs := []string{}
		if cluster.EnableK8SBetaApis != nil && len(cluster.EnableK8SBetaApis.EnabledApis) > 0 {
			clusterEnabledAPIs = cluster.EnableK8SBetaApis.EnabledApis
		}
		slog.Info("Mismatch in EnableK8SBetaApis", slog.String("current", strings.Join(clusterEnabledAPIs, ",")), slog.String("expected", strings.Join(requiredBetaAPIs, ",")))

		var combinedAPIs []string
		for _, api := range append(requiredBetaAPIs, clusterEnabledAPIs...) {
			if !slices.Contains(combinedAPIs, api) {
				combinedAPIs = append(combinedAPIs, api)
			}
		}

		op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
			Name: clusterName,
			Update: &containerpb.ClusterUpdate{
				DesiredK8SBetaApis: &containerpb.K8SBetaAPIConfig{
					EnabledApis: combinedAPIs,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("update cluster beta apis: %w", err)
		}
		if err := waitContainerOperation(ctx, client, op.Name, cfg); err != nil {
			return err
		}
	} else {
		slog.Info("Cluster EnableK8SBetaApis match perfectly.", slog.String("cluster", cfg.ClusterName))
	}

	desiredOTelScope := containerpb.ManagedOpenTelemetryConfig_COLLECTION_AND_INSTRUMENTATION_COMPONENTS
	if cluster.GetManagedOpentelemetryConfig().GetScope() != desiredOTelScope {
		slog.Info("Mismatch in Managed OpenTelemetry config",
			slog.String("current", cluster.GetManagedOpentelemetryConfig().GetScope().String()),
			slog.String("expected", desiredOTelScope.String()))
		slog.Info("Updating cluster ManagedOpentelemetryConfig...")
		op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
			Name: clusterName,
			Update: &containerpb.ClusterUpdate{
				DesiredManagedOpentelemetryConfig: &containerpb.ManagedOpenTelemetryConfig{Scope: desiredOTelScope.Enum()},
			},
		})
		if err != nil {
			return fmt.Errorf("update cluster managed opentelemetry: %w", err)
		}
		if err := waitContainerOperation(ctx, client, op.Name, cfg); err != nil {
			return err
		}
	} else {
		slog.Info("Cluster ManagedOpentelemetryConfig match perfectly.", slog.String("cluster", cfg.ClusterName))
	}

	return nil
}

func waitContainerOperation(ctx context.Context, client *container.ClusterManagerClient, opName string, cfg *Config) error {
	slog.Info("Waiting for operation to complete...", slog.String("operation", opName))

	fullName := opName
	if !strings.HasPrefix(opName, "projects/") {
		fullName = fmt.Sprintf("projects/%s/locations/%s/operations/%s", cfg.ProjectID, cfg.ClusterLocation, opName)
	}

	err := wait.PollUntilContextTimeout(ctx, 10*time.Second, 30*time.Minute, true, func(pollCtx context.Context) (bool, error) {
		op, err := client.GetOperation(pollCtx, &containerpb.GetOperationRequest{
			Name: fullName,
		})
		if err != nil {
			return false, fmt.Errorf("failed to get operation status: %w", err)
		}
		if op.Status == containerpb.Operation_DONE {
			if op.Error != nil {
				return true, fmt.Errorf("operation failed: %v", op.Error)
			}
			slog.Info("Operation completed successfully.", slog.String("operation", opName))
			return true, nil
		}
		if op.Status == containerpb.Operation_ABORTING {
			return true, fmt.Errorf("operation %s is aborting", opName)
		}
		return false, nil
	})

	if err != nil {
		return fmt.Errorf("wait for operation %s: %w", opName, err)
	}

	return nil
}

func containsAll(clusterAPIs []string, requiredAPIs []string) bool {
	for _, s := range requiredAPIs {
		if !slices.Contains(clusterAPIs, s) {
			return false
		}
	}
	return true
}

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Create GKE cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		if cfg.ProjectID == "" {
			return errors.New("--project-id is required")
		}
		return createClusterIdempotent(cmd.Context(), &cfg)
	},
}

func init() {
	createCmd.AddCommand(clusterCmd)
	clusterCmd.Flags().StringVar(&cfg.ClusterName, "name", getEnv("CLUSTER_NAME", "substrate-poc"), "Name of the GKE cluster [env: CLUSTER_NAME]")
	clusterCmd.Flags().StringVar(&cfg.ClusterLocation, "location", getEnv("CLUSTER_LOCATION", "us-central1-c"), "Zone or region for the cluster [env: CLUSTER_LOCATION]")
	clusterCmd.Flags().StringVar(&cfg.ClusterVersion, "version", getEnv("CLUSTER_VERSION", ""), "Kubernetes version [env: CLUSTER_VERSION]")
	clusterCmd.Flags().StringVar(&cfg.Network, "network", getEnv("NETWORK", "default"), "VPC network name [env: NETWORK]")
	clusterCmd.Flags().StringVar(&cfg.Subnetwork, "subnetwork", getEnv("SUBNETWORK", "default"), "VPC subnetwork name [env: SUBNETWORK]")
	clusterCmd.Flags().StringVar(&cfg.MachineType, "machine-type", getEnv("GVISOR_NODE_MACHINE_TYPE", "c3-standard-4"), "Machine type for the gVisor node pool [env: GVISOR_NODE_MACHINE_TYPE]")
	clusterCmd.Flags().BoolVar(&cfg.EnableDataplaneV2, "enable-dataplane-v2", getEnv("ENABLE_DATAPLANE_V2", true), "Enable Dataplane V2 [env: ENABLE_DATAPLANE_V2]")
}
