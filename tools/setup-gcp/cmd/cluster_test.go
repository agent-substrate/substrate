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
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
)

func TestBuildCreateClusterRequest_FilestoreDisabled(t *testing.T) {
	cfg := &Config{
		ProjectID:       "test-project",
		ClusterName:     "test-cluster",
		ClusterLocation: "us-west1-c",
		MachineType:     "c3-standard-4",
	}
	parent := "projects/test-project/locations/us-west1-c"

	req := buildCreateClusterRequest(parent, cfg)
	if req.Cluster == nil {
		t.Fatal("expected req.Cluster to be non-nil")
	}

	addonsConfig := req.Cluster.AddonsConfig
	if addonsConfig == nil {
		t.Fatal("expected req.Cluster.AddonsConfig to be non-nil")
	}

	filestoreConfig := addonsConfig.GcpFilestoreCsiDriverConfig
	if filestoreConfig == nil {
		t.Fatal("expected GcpFilestoreCsiDriverConfig to be non-nil")
	}

	if filestoreConfig.Enabled {
		t.Errorf("expected GcpFilestoreCsiDriverConfig.Enabled to be false, got true")
	}
}

func TestBuildCreateClusterRequest_NodeConfig(t *testing.T) {
	tests := []struct {
		name         string
		cfg          *Config
		wantDiskSize int32
		wantDiskType string
	}{
		{
			name: "custom disk size and type",
			cfg: &Config{
				ProjectID:       "test-project",
				ClusterName:     "test-cluster",
				ClusterLocation: "us-west1-c",
				MachineType:     "c3-standard-8",
				BootDiskSizeGB:  500,
				BootDiskType:    "hyperdisk-balanced",
			},
			wantDiskSize: 500,
			wantDiskType: "hyperdisk-balanced",
		},
		{
			name: "unset disk size and type",
			cfg: &Config{
				ProjectID:       "test-project",
				ClusterName:     "test-cluster",
				ClusterLocation: "us-west1-c",
				MachineType:     "c3-standard-4",
			},
			wantDiskSize: 0,
			wantDiskType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := "projects/test-project/locations/us-west1-c"
			req := buildCreateClusterRequest(parent, tt.cfg)
			if req.Cluster == nil || len(req.Cluster.NodePools) == 0 {
				t.Fatal("expected non-empty node pools in request")
			}

			nodeConfig := req.Cluster.NodePools[0].Config
			if nodeConfig == nil {
				t.Fatal("expected non-nil node config")
			}

			if nodeConfig.MachineType != tt.cfg.MachineType {
				t.Errorf("MachineType = %q, want %q", nodeConfig.MachineType, tt.cfg.MachineType)
			}
			if nodeConfig.DiskSizeGb != tt.wantDiskSize {
				t.Errorf("DiskSizeGb = %d, want %d", nodeConfig.DiskSizeGb, tt.wantDiskSize)
			}
			if nodeConfig.DiskType != tt.wantDiskType {
				t.Errorf("DiskType = %q, want %q", nodeConfig.DiskType, tt.wantDiskType)
			}
		})
	}
}

func TestFilestoreCsiDriverEnabled(t *testing.T) {
	tests := []struct {
		name    string
		cluster *containerpb.Cluster
		want    bool
	}{
		{
			name:    "nil cluster",
			cluster: nil,
			want:    false,
		},
		{
			name:    "nil addons config",
			cluster: &containerpb.Cluster{},
			want:    false,
		},
		{
			name: "nil filestore config",
			cluster: &containerpb.Cluster{
				AddonsConfig: &containerpb.AddonsConfig{},
			},
			want: false,
		},
		{
			name: "filestore disabled",
			cluster: &containerpb.Cluster{
				AddonsConfig: &containerpb.AddonsConfig{
					GcpFilestoreCsiDriverConfig: &containerpb.GcpFilestoreCsiDriverConfig{
						Enabled: false,
					},
				},
			},
			want: false,
		},
		{
			name: "filestore enabled",
			cluster: &containerpb.Cluster{
				AddonsConfig: &containerpb.AddonsConfig{
					GcpFilestoreCsiDriverConfig: &containerpb.GcpFilestoreCsiDriverConfig{
						Enabled: true,
					},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := filestoreCsiDriverEnabled(tt.cluster); got != tt.want {
				t.Errorf("filestoreCsiDriverEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateBootDisk(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "valid positive boot disk size",
			cfg:  Config{BootDiskSizeGB: 500},
		},
		{
			name: "zero boot disk size (unset/default)",
			cfg:  Config{BootDiskSizeGB: 0},
		},
		{
			name:    "negative boot disk size",
			cfg:     Config{BootDiskSizeGB: -1},
			wantErr: "boot disk size -1 is invalid: must be greater than or equal to 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgCopy := tt.cfg
			err := validateBootDisk(&cfgCopy)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error %q, got nil", tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Errorf("got error %q, want %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
