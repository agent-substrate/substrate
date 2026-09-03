// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"fmt"
	"os"

	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// CheckEnv checks the list of env vars exist and returns their value.
// If any env var is not set, it returns an error.
func CheckEnv(keys ...string) (map[string]string, error) {
	env := make(map[string]string, len(keys))
	for _, key := range keys {
		value := os.Getenv(key)
		if value == "" {
			return nil, fmt.Errorf("environment variable %s is not set", key)
		}
		env[key] = value
	}
	return env, nil
}

// SystemNamespaceEnv names the namespace the substrate control plane under test
// was installed into. It mirrors the ATE_NAMESPACE hack/install-ate.sh
// installed with, and the --namespace the chart was released into.
const SystemNamespaceEnv = "E2E_SYSTEM_NAMESPACE"

// SystemNamespace returns the namespace the control plane under test runs in,
// falling back to the canonical install namespace.
func SystemNamespace() string {
	if ns := os.Getenv(SystemNamespaceEnv); ns != "" {
		return ns
	}
	return installdefaults.SystemNamespace
}
