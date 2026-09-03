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
	"os"
	"strconv"
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

	NodePoolName    string
	NodePoolVersion string
	MachineType     string

	BucketName string

	CloudSQLInstance  string
	CloudSQLTier      string
	CloudSQLEdition   string
	CloudSQLStorageGB int64
	CloudSQLGSAName   string

	DashboardDir string
}

type getEnvType interface {
	string | bool | int64
}

// getEnv retrieves an environment variable by key and parses it into the specified type.
// If the environment variable is not set or parsing fails, it returns the fallback value.
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
	}

	if err == nil {
		return ret.(T)
	}

	return fallback
}
