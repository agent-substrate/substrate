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
	"os"
	"strings"
)

type Environment struct {
	ProjectID             string
	ProjectNumber         string
	ClusterName           string
	ClusterLocation       string
	ClusterVersion        string
	Network               string
	Subnetwork            string
	NodePoolName          string
	NodePoolVersion       string
	GCERegion             string
	BucketName            string
	GVisorNodeMachineType string
}

func loadEnv() (*Environment, error) {
	requiredEnvVars := []string{
		"PROJECT_ID",
		"PROJECT_NUMBER",
		"CLUSTER_NAME",
		"CLUSTER_LOCATION",
		"CLUSTER_VERSION",
		"NETWORK",
		"SUBNETWORK",
		"NODE_POOL_NAME",
		"NODE_POOL_VERSION",
		"GCE_REGION",
		"BUCKET_NAME",
		"GVISOR_NODE_MACHINE_TYPE",
	}

	missing := []string{}
	for _, key := range requiredEnvVars {
		if os.Getenv(key) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	return &Environment{
		ProjectID:             os.Getenv("PROJECT_ID"),
		ProjectNumber:         os.Getenv("PROJECT_NUMBER"),
		ClusterName:           os.Getenv("CLUSTER_NAME"),
		ClusterLocation:       os.Getenv("CLUSTER_LOCATION"),
		ClusterVersion:        os.Getenv("CLUSTER_VERSION"),
		Network:               os.Getenv("NETWORK"),
		Subnetwork:            os.Getenv("SUBNETWORK"),
		NodePoolName:          os.Getenv("NODE_POOL_NAME"),
		NodePoolVersion:       os.Getenv("NODE_POOL_VERSION"),
		GCERegion:             os.Getenv("GCE_REGION"),
		BucketName:            os.Getenv("BUCKET_NAME"),
		GVisorNodeMachineType: os.Getenv("GVISOR_NODE_MACHINE_TYPE"),
	}, nil
}
