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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ProbeName is the name of the probe fixture's WorkerPool and ActorTemplate,
// inside the atespace (and matching k8s namespace) DeployProbe returns.
const ProbeName = "probe"

// probeManifests are the fixture templates DeployProbe deploys: the k8s pool
// half and the substrate ActorTemplate half.
var probeManifests = SubstrateFixtureManifests{
	Pool:     "internal/e2e/fixtures/probe/probe.yaml.tmpl",
	Template: "internal/e2e/fixtures/probe/probe-template.yaml.tmpl",
}

// ProbeOption adjusts what DeployProbe installs.
type ProbeOption func(*probeConfig)

type probeConfig struct{ trustBundle bool }

// WithTrustBundle projects the egress trust bundle into the probe's
// system-info volume, ensuring the cluster-scoped bundle exists first.
//
// Only suites that ASSERT the projection ask for it. The bundle is derived
// from a single cluster-wide Secret, so a suite that merely needs a probe must
// not depend on it: it would then fail whenever the suite that owns the pool
// finishes and takes the bundle with it. For the same reason two suites that
// opt in must not run concurrently — CI runs the egress ones in their own
// step, leaving the identity suite the only opt-in in the standard lanes.
func WithTrustBundle() ProbeOption { return func(c *probeConfig) { c.trustBundle = true } }

// DeployProbe builds the probe fixture image and installs the fixture for the
// sandbox class under test, removing it when the test ends. name
// distinguishes the caller (by convention its suite name): each suite gets
// its own copy of the fixture, so no suite's cleanup can delete the fixture
// out from under another running concurrently. It returns the fixture's
// atespace (which also names the k8s namespace holding the pool) and the
// created ActorTemplate, already golden-snapshotted.
func DeployProbe(t *testing.T, bucket, name string, opts ...ProbeOption) (string, *ateapipb.ActorTemplate) {
	t.Helper()

	var cfg probeConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.trustBundle {
		// Every actor from this template — the fixture's golden boot included —
		// fails closed while the bundle is missing, so it has to exist first.
		EnsureEgressTrustBundle(t, context.Background(), GetClients())
	}

	atespace, templates := DeploySubstrateFixture(t, context.Background(), GetClients(), probeManifests, bucket, name, cfg.trustBundle)
	return atespace, templates[0]
}
