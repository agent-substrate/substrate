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

func TestNodePoolsWithoutPodCertificateProjection(t *testing.T) {
	pool := func(name, version string) *containerpb.NodePool {
		return &containerpb.NodePool{Name: name, Version: version}
	}
	tests := []struct {
		name  string
		pools []*containerpb.NodePool
		want  []string
	}{
		{
			name:  "1.36 pool needs its nodes recreated",
			pools: []*containerpb.NodePool{pool("default-pool", "1.36.4-gke.1247000")},
			want:  []string{"default-pool"},
		},
		{
			name:  "1.37 pool is fine, projection is GA in its kubelet",
			pools: []*containerpb.NodePool{pool("default-pool", "1.37.0-gke.3503000")},
		},
		{
			// A 1.37 control plane does not help 1.36 kubelets, so only the
			// old pool is reported.
			name: "only the pools below 1.37 are reported",
			pools: []*containerpb.NodePool{
				pool("old", "1.36.4-gke.1247000"),
				pool("new", "1.37.0-gke.3503000"),
			},
			want: []string{"old"},
		},
		{
			name:  "an unreadable version is reported rather than assumed fine",
			pools: []*containerpb.NodePool{pool("mystery", "")},
			want:  []string{"mystery"},
		},
		{
			name: "no pools, nothing to report",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, p := range nodePoolsWithoutPodCertificateProjection(&containerpb.Cluster{NodePools: tt.pools}) {
				got = append(got, p.GetName())
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKubernetesMinor(t *testing.T) {
	tests := []struct {
		version string
		want    int
		wantOK  bool
	}{
		{version: "1.36.4-gke.1247000", want: 36, wantOK: true},
		{version: "1.37", want: 37, wantOK: true},
		{version: "1.100.0", want: 100, wantOK: true},
		{version: ""},
		{version: "latest"},
		{version: "2.0.0"},
		{version: "1.x.0"},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			got, ok := kubernetesMinor(tt.version)
			if ok != tt.wantOK || got != tt.want {
				t.Errorf("kubernetesMinor(%q) = %d, %v; want %d, %v", tt.version, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestReplacementNodePoolCommand(t *testing.T) {
	cfg := &Config{ProjectID: "my-project", ClusterName: "substrate-poc", ClusterLocation: "us-west1-c"}
	tests := []struct {
		name string
		pool *containerpb.NodePool
		want string
	}{
		{
			name: "a pool this tool created carries its disk and nested virtualization over",
			pool: &containerpb.NodePool{
				Name:             "substrate-node-pool",
				Version:          "1.36.4-gke.1247000",
				InitialNodeCount: 2,
				Config: &containerpb.NodeConfig{
					MachineType:             "c3-standard-4",
					DiskSizeGb:              500,
					DiskType:                "pd-balanced",
					AdvancedMachineFeatures: &containerpb.AdvancedMachineFeatures{EnableNestedVirtualization: proto.Bool(true)},
				},
			},
			want: "gcloud container node-pools create substrate-node-pool-2 --cluster=substrate-poc --project=my-project --location=us-west1-c --node-version=1.36.4-gke.1247000 --machine-type=c3-standard-4 --num-nodes=2 --disk-size=500 --disk-type=pd-balanced --enable-nested-virtualization",
		},
		{
			// Autoscaled pools can report zero; a pool with no nodes would
			// leave nothing for the workloads to move to.
			name: "defaults stay defaults, and the pool gets at least one node",
			pool: &containerpb.NodePool{
				Name:    "default-pool",
				Version: "1.36.4-gke.1082000",
				Config:  &containerpb.NodeConfig{MachineType: "e2-standard-4"},
			},
			want: "gcloud container node-pools create default-pool-2 --cluster=substrate-poc --project=my-project --location=us-west1-c --node-version=1.36.4-gke.1082000 --machine-type=e2-standard-4 --num-nodes=1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := replacementNodePoolCommand(cfg, tt.pool); got != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestReplacementNodePoolNameFitsGKELimit(t *testing.T) {
	long := strings.Repeat("a", 39) + "-b"
	got := replacementNodePoolName(long)
	if len(got) > 40 {
		t.Errorf("replacementNodePoolName(%q) = %q, %d characters; GKE allows 40", long, got, len(got))
	}
	if got == long {
		t.Errorf("replacement pool name %q collides with the original", got)
	}
}

func TestDeleteNodePoolCommand(t *testing.T) {
	cfg := &Config{ProjectID: "my-project", ClusterName: "substrate-poc", ClusterLocation: "us-west1-c"}
	got := deleteNodePoolCommand(cfg, &containerpb.NodePool{Name: "substrate-node-pool"})
	want := "gcloud container node-pools delete substrate-node-pool --cluster=substrate-poc --project=my-project --location=us-west1-c"
	if got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
}
