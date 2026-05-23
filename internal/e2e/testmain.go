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
	"context"
	"flag"
	"fmt"
	"testing"
)

var (
	RunE2E      bool
	KubeConfig  string
	KubeContext string
)

func bindFlags() {
	flag.BoolVar(&RunE2E, "e2e", false, "run e2e tests")
	flag.BoolVar(&NoColor, "no-color", false, "disable colors in output")
	flag.StringVar(&KubeConfig, "kube-config", "", "Location of the kubeconfig")
	flag.StringVar(&KubeContext, "kube-context", "", "Kubernetes context to use")
}

// RunTestMain should be used to run your e2e test suite.
func RunTestMain(m *testing.M) int {
	bindFlags()
	flag.Parse()

	if !RunE2E {
		fmt.Println(Colorf(`
        <yellow>This is an e2e test suite and does not run by default.
        Run with "go test ./internal/e2e/... -args -e2e"</yellow>`))
		fmt.Println()
		return 0
	}

	return runAndCleanup(m)
}

func runAndCleanup(m *testing.M) int {
	ctx := context.Background()
	clients, err := NewClients(ctx)
	if err != nil {
		fmt.Printf("Failed to initialize E2E clients: %v\n", err)
		return 1
	}
	sharedClients = clients
	defer func() {
		sharedClients.Close()
		sharedClients = nil
	}()

	if err := PreflightChecks(); err != nil {
		fmt.Println(Colorf("<red>Preflight checks FAILED: %v</red>\n", err))
		return 1
	}

	defer CleanupNamespaces()

	return m.Run()
}
