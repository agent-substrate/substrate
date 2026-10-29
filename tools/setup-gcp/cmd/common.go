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
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"

	"github.com/spf13/cobra"
)

type Config struct {
	ProjectID     string
	ProjectNumber string
	Region        string

	ClusterName     string
	ClusterLocation string
	ClusterVersion  string

	Network           string
	Subnetwork        string
	EnableDataplaneV2 bool

	NodePoolName               string
	NodePoolVersion            string
	MachineType                string
	EnableNestedVirtualization bool
	BootDiskSizeGB             int32
	BootDiskType               string

	BucketName string

	// TransferGSAName is the name of the snapshot transfer service account
	// (SNAPSHOT_TRANSFER_GSA_NAME), the variable ate-setup also reads.
	TransferGSAName string

	CloudSQLInstance  string
	CloudSQLTier      string
	CloudSQLEdition   string
	CloudSQLStorageGB int64
	CloudSQLGSAName   string

	DashboardDir string
}

func machineTypeFromEnv() (machineType string, deprecated bool) {
	if v := os.Getenv("NODE_MACHINE_TYPE"); v != "" {
		return v, false
	}
	if v := os.Getenv("GVISOR_NODE_MACHINE_TYPE"); v != "" {
		return v, true
	}
	return "c3-standard-4", false
}

func resolveMachineTypeDefault() string {
	machineType, _ := machineTypeFromEnv()
	return machineType
}

func warnDeprecatedMachineTypeEnv(cmd *cobra.Command) {
	if cmd.Flags().Changed("machine-type") {
		return
	}
	if _, deprecated := machineTypeFromEnv(); deprecated {
		slog.Warn("GVISOR_NODE_MACHINE_TYPE is deprecated; use NODE_MACHINE_TYPE instead")
	}
}

type getEnvType interface {
	string | bool | int64 | int32
}

var warnedEnvKeys sync.Map

// getEnv retrieves an environment variable by key and parses it into the specified type.
func getEnv[T getEnvType](key string, fallback T) T {
	val, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	var ret any
	var err error

	switch any(fallback).(type) {
	case string:
		ret = val
	case bool:
		ret, err = strconv.ParseBool(val)
	case int64:
		ret, err = strconv.ParseInt(val, 10, 64)
	case int32:
		var v int64
		v, err = strconv.ParseInt(val, 10, 32)
		ret = int32(v)
	}

	if err != nil {
		if _, warned := warnedEnvKeys.LoadOrStore(key, struct{}{}); !warned {
			slog.Warn("Ignoring unparsable environment variable", "key", key, "value", val, "using", fallback, "error", err)
		}
		return fallback
	}

	return ret.(T)
}

// assetBucketName is the bucket for micro-VM sandbox assets.
// hack/install-microvm-deps.sh derives the same name.
func (c *Config) assetBucketName() string {
	return c.BucketName + "-assets"
}

// workloadIdentityPrincipal is the IAM member for the ate-system Kubernetes
// service account ksa under GKE Workload Identity Federation.
func workloadIdentityPrincipal(cfg *Config, ksa string) string {
	return fmt.Sprintf("principal://iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s.svc.id.goog/subject/ns/ate-system/sa/%s", cfg.ProjectNumber, cfg.ProjectID, ksa)
}
