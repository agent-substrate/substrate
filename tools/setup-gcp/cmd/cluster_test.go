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
	"strings"
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/proto"
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

func TestBuildCreateClusterRequest_NestedVirtualization(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want bool
	}{
		{
			name: "enabled",
			cfg: &Config{
				ProjectID:                  "test-project",
				ClusterName:                "test-cluster",
				ClusterLocation:            "us-west1-c",
				MachineType:                "n2-standard-8",
				EnableNestedVirtualization: true,
			},
			want: true,
		},
		{
			name: "disabled",
			cfg: &Config{
				ProjectID:       "test-project",
				ClusterName:     "test-cluster",
				ClusterLocation: "us-west1-c",
				MachineType:     "c3-standard-4",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent := "projects/test-project/locations/us-west1-c"
			req := buildCreateClusterRequest(parent, tt.cfg)
			if req.Cluster == nil || len(req.Cluster.NodePools) == 0 {
				t.Fatal("expected non-empty node pools in request")
			}

			got := req.Cluster.NodePools[0].Config.GetAdvancedMachineFeatures().GetEnableNestedVirtualization()
			if got != tt.want {
				t.Errorf("EnableNestedVirtualization = %v, want %v", got, tt.want)
			}

			// A disabled knob must leave the field unset rather than send
			// false, so GKE applies its own default.
			if !tt.want && req.Cluster.NodePools[0].Config.GetAdvancedMachineFeatures() != nil {
				t.Errorf("expected AdvancedMachineFeatures to be nil when the knob is off")
			}
		})
	}
}

func TestNestedVirtualizationEnabled(t *testing.T) {
	poolWith := func(enabled *bool) *containerpb.NodePool {
		return &containerpb.NodePool{
			Config: &containerpb.NodeConfig{
				AdvancedMachineFeatures: &containerpb.AdvancedMachineFeatures{
					EnableNestedVirtualization: enabled,
				},
			},
		}
	}

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
			name:    "no node pools",
			cluster: &containerpb.Cluster{},
			want:    false,
		},
		{
			name: "pool without advanced machine features",
			cluster: &containerpb.Cluster{
				NodePools: []*containerpb.NodePool{{Config: &containerpb.NodeConfig{}}},
			},
			want: false,
		},
		{
			name: "single pool with nested virtualization",
			cluster: &containerpb.Cluster{
				NodePools: []*containerpb.NodePool{poolWith(proto.Bool(true))},
			},
			want: true,
		},
		{
			name: "second pool carries the KVM nodes",
			cluster: &containerpb.Cluster{
				NodePools: []*containerpb.NodePool{
					poolWith(proto.Bool(false)),
					poolWith(proto.Bool(true)),
				},
			},
			want: true,
		},
		{
			name: "every pool disabled",
			cluster: &containerpb.Cluster{
				NodePools: []*containerpb.NodePool{
					poolWith(proto.Bool(false)),
					poolWith(nil),
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nestedVirtualizationEnabled(tt.cluster); got != tt.want {
				t.Errorf("nestedVirtualizationEnabled() = %v, want %v", got, tt.want)
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

func TestValidateClusterLocation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "compatible default us-west1 and us-west1-c",
			cfg:  Config{Region: "us-west1", ClusterLocation: "us-west1-c"},
		},
		{
			name: "compatible zone in region",
			cfg:  Config{Region: "us-central1", ClusterLocation: "us-central1-a"},
		},
		{
			name: "compatible regional cluster location",
			cfg:  Config{Region: "us-central1", ClusterLocation: "us-central1"},
		},
		{
			name:    "incompatible zone and region",
			cfg:     Config{Region: "us-central1", ClusterLocation: "us-west1-c"},
			wantErr: `cluster location "us-west1-c" is not compatible with region "us-central1"`,
		},
		{
			name:    "incompatible regions",
			cfg:     Config{Region: "us-central1", ClusterLocation: "us-west1"},
			wantErr: `cluster location "us-west1" is not compatible with region "us-central1"`,
		},
		{
			name:    "similar prefix but different region number",
			cfg:     Config{Region: "us-central1", ClusterLocation: "us-central2-c"},
			wantErr: `cluster location "us-central2-c" is not compatible with region "us-central1"`,
		},
		{
			name:    "missing region",
			cfg:     Config{Region: "", ClusterLocation: "us-central1-c"},
			wantErr: "--region is required",
		},
		{
			name:    "missing cluster location",
			cfg:     Config{Region: "us-central1", ClusterLocation: ""},
			wantErr: "--cluster-location is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgCopy := tt.cfg
			err := validateClusterLocation(&cfgCopy)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("got error %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateReleaseChannel(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		want    string
		wantErr string
	}{
		{
			name: "unset leaves the cluster on GKE's default channel",
			cfg:  Config{ReleaseChannel: ""},
		},
		{
			name: "rapid",
			cfg:  Config{ReleaseChannel: "rapid"},
			want: "rapid",
		},
		{
			name: "none is a real choice, not the same as unset",
			cfg:  Config{ReleaseChannel: "none"},
			want: "none",
		},
		{
			name: "case and surrounding space are normalized",
			cfg:  Config{ReleaseChannel: "  Regular "},
			want: "regular",
		},
		{
			name:    "unknown channel",
			cfg:     Config{ReleaseChannel: "nightly"},
			wantErr: `release channel "nightly" is invalid: must be one of extended, none, rapid, regular, stable`,
		},
		{
			name:    "extended cannot carry the beta APIs this tool enables",
			cfg:     Config{ReleaseChannel: "extended"},
			wantErr: "does not allow the Kubernetes beta APIs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfgCopy := tt.cfg
			err := validateReleaseChannel(&cfgCopy)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("got error %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfgCopy.ReleaseChannel != tt.want {
				t.Errorf("got normalized channel %q, want %q", cfgCopy.ReleaseChannel, tt.want)
			}
		})
	}
}

func TestBuildCreateClusterRequest_ReleaseChannel(t *testing.T) {
	tests := []struct {
		name        string
		channel     string
		wantSet     bool
		wantChannel containerpb.ReleaseChannel_Channel
	}{
		{
			// The important case: no channel field at all, so GKE applies its
			// own default. Sending UNSPECIFIED here instead would unenroll the
			// cluster from release channels, which is a different cluster.
			name:    "unset sends no channel",
			channel: "",
			wantSet: false,
		},
		{
			name:        "rapid",
			channel:     "rapid",
			wantSet:     true,
			wantChannel: containerpb.ReleaseChannel_RAPID,
		},
		{
			name:        "regular",
			channel:     "regular",
			wantSet:     true,
			wantChannel: containerpb.ReleaseChannel_REGULAR,
		},
		{
			name:        "stable",
			channel:     "stable",
			wantSet:     true,
			wantChannel: containerpb.ReleaseChannel_STABLE,
		},
		{
			name:        "none unenrolls the cluster explicitly",
			channel:     "none",
			wantSet:     true,
			wantChannel: containerpb.ReleaseChannel_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				ProjectID:       "test-project",
				ClusterName:     "test-cluster",
				ClusterLocation: "us-west1-c",
				MachineType:     "c3-standard-4",
				ReleaseChannel:  tt.channel,
			}
			req := buildCreateClusterRequest("projects/test-project/locations/us-west1-c", cfg)
			got := req.Cluster.ReleaseChannel
			if !tt.wantSet {
				if got != nil {
					t.Fatalf("expected no ReleaseChannel, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected ReleaseChannel to be set, got nil")
			}
			if got.Channel != tt.wantChannel {
				t.Errorf("got channel %v, want %v", got.Channel, tt.wantChannel)
			}
		})
	}
}
