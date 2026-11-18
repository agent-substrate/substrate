// Copyright 2026 The Agent Substrate Authors
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

// Package harness runs ate-api against fakes of everything it talks to, for
// functional tests that drive it only through its public API.
//
// Each test gets its own ate-api, database, fake Kubernetes clients, nodes,
// atelets, object store and volume plugin, so tests run in parallel. A test package
// using the harness calls Main from its TestMain.
package harness

import (
	"log"
	"os"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/preview"
)

// mainStarted records that Main set up the process for the harness.
var mainStarted bool

// Main enables ate-api's preview gates, runs the package's tests, and tears
// the PostgreSQL container down.
func Main(m *testing.M) {
	// Process-wide, like the server flag. Every gate is on, and a test uses a
	// preview feature only through a harness WithPreviewClient, whose clients
	// opt in.
	//
	// TODO: Start ate-api with no gates, as ate-api-server does, and let each
	// test enable the ones it needs when it creates the harness, such as
	// New(t, WithPreviewServer(preview.GateExternalVolumes)). That needs
	// ate-api to take its gates per instance instead of from preview's
	// process-wide state.
	if err := preview.Init(preview.All); err != nil {
		log.Fatalf("enabling preview gates: %v", err)
	}
	mainStarted = true
	code := m.Run()
	storetest.Shutdown()
	os.Exit(code)
}
