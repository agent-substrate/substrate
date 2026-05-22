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
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	container "cloud.google.com/go/container/apiv1"
	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

var requiredBetaAPIs = []string{
	"certificates.k8s.io/v1beta1/podcertificaterequests",
	"certificates.k8s.io/v1beta1/clustertrustbundles",
}

func deleteCluster(ctx context.Context, env *Environment) error {
	client, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	name := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", env.ProjectID, env.ClusterLocation, env.ClusterName)
	slog.Info("Deleting cluster", slog.String("cluster", env.ClusterName))
	op, err := client.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}
	return waitContainerOperation(ctx, client, op.Name, env)
}

func createClusterInternal(ctx context.Context, env *Environment, client *container.ClusterManagerClient, parent string) error {
	slog.Info("Cluster does not exist. Creating...", slog.String("cluster", env.ClusterName))
	req := &containerpb.CreateClusterRequest{
		Parent: parent,
		Cluster: &containerpb.Cluster{
			Name:                  env.ClusterName,
			InitialClusterVersion: env.ClusterVersion,
			NodePools: []*containerpb.NodePool{
				{
					Name:             "substrate-node-pool",
					InitialNodeCount: 2,
					Config: &containerpb.NodeConfig{
						MachineType: env.GVisorNodeMachineType,
					},
				},
			},
			EnableK8SBetaApis: &containerpb.K8SBetaAPIConfig{
				EnabledApis: requiredBetaAPIs,
			},
			WorkloadIdentityConfig: &containerpb.WorkloadIdentityConfig{
				WorkloadPool: fmt.Sprintf("%s.svc.id.goog", env.ProjectID),
			},
			Network:    env.Network,
			Subnetwork: env.Subnetwork,
		},
	}
	op, err := client.CreateCluster(ctx, req)
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	return waitContainerOperation(ctx, client, op.Name, env)
}

func createClusterIdempotent(ctx context.Context, env *Environment) error {
	client, err := container.NewClusterManagerClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	parent := fmt.Sprintf("projects/%s/locations/%s", env.ProjectID, env.ClusterLocation)
	clusterName := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", env.ProjectID, env.ClusterLocation, env.ClusterName)

	slog.Info("Checking if cluster exists", slog.String("cluster", env.ClusterName), slog.String("location", env.ClusterLocation))
	cluster, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: clusterName})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return fmt.Errorf("getting cluster: %w", err)
		}

		return createClusterInternal(ctx, env, client, parent)
	}

	slog.Info("Cluster exists. Checking attributes...", slog.String("cluster", env.ClusterName))

	// Recreate cluster if network configuration mismatches.
	expectedNetwork := fmt.Sprintf("projects/%s/global/networks/%s", env.ProjectID, env.Network)
	if cluster.NetworkConfig != nil && cluster.NetworkConfig.Network != "" && !strings.HasSuffix(cluster.NetworkConfig.Network, expectedNetwork) {
		slog.Info("Mismatch in network", slog.String("current", cluster.NetworkConfig.Network), slog.String("expected", expectedNetwork))
		if err := deleteCluster(ctx, env); err != nil {
			return err
		}
		return createClusterInternal(ctx, env, client, parent)
	}

	// Recreate cluster if subnet configuration mismatches.
	expectedSubnetwork := fmt.Sprintf("projects/%s/regions/%s/subnetworks/%s", env.ProjectID, env.GCERegion, env.Subnetwork)
	if cluster.NetworkConfig != nil && cluster.NetworkConfig.Subnetwork != "" && !strings.HasSuffix(cluster.NetworkConfig.Subnetwork, expectedSubnetwork) {
		slog.Info("Mismatch in subnetwork", slog.String("current", cluster.NetworkConfig.Subnetwork), slog.String("expected", expectedSubnetwork))
		if err := deleteCluster(ctx, env); err != nil {
			return err
		}
		return createClusterInternal(ctx, env, client, parent)
	}

	expectedWorkloadPool := fmt.Sprintf("%s.svc.id.goog", env.ProjectID)
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
		if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
			return err
		}
	} else {
		slog.Info("Cluster WorkloadIdentityConfig match perfectly.", slog.String("cluster", env.ClusterName))
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
		if err := waitContainerOperation(ctx, client, op.Name, env); err != nil {
			return err
		}
	} else {
		slog.Info("Cluster EnableK8SBetaApis match perfectly.", slog.String("cluster", env.ClusterName))
	}

	return nil
}

func waitContainerOperation(ctx context.Context, client *container.ClusterManagerClient, opName string, env *Environment) error {
	slog.Info("Waiting for operation to complete...", slog.String("operation", opName))

	fullName := opName
	if !strings.HasPrefix(opName, "projects/") {
		fullName = fmt.Sprintf("projects/%s/locations/%s/operations/%s", env.ProjectID, env.ClusterLocation, opName)
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
