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
)

// ProbeName is the name of the probe fixture's WorkerPool and ActorTemplate,
// inside the namespace DeployProbe returns.
const ProbeName = "probe"

// probeManifest is the fixture template DeployProbe renders.
const probeManifest = "internal/e2e/fixtures/probe/probe.yaml.tmpl"

// trustBundleDataSource fills ${TEMPLATE_TRUST_BUNDLE}, indented to sit in the
// template's dataSources list. The name must be on atelet's supported-bundle
// allowlist.
const trustBundleDataSource = `      - trustBundle:
          name: egress-mitm.ate.dev
          path: trust-bundle.pem`

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

// DeployProbe builds the probe fixture image and applies its manifest for the
// sandbox class under test, removing it when the test ends. name distinguishes
// the caller (by convention its suite name): each suite gets its own copy of
// the fixture, so no suite's cleanup can delete the fixture out from under
// another running concurrently. It returns the fixture's namespace.
func DeployProbe(t *testing.T, bucket, name string, opts ...ProbeOption) string {
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

	// One manifest, rendered for the sandbox class under test, so both apply
	// and delete consume the same file without any shell involved.
	manifest := renderProbeManifest(t, bucket, name, cfg)
	koApply(t, manifest)

	// Unlike the fixtures that live in a namespace CreateNamespace tears down,
	// this one installs into a fixed namespace it shares with nothing, so it has
	// to clean up after itself.
	t.Cleanup(func() {
		// Deletion needs no image build, so go straight to kubectl. `ko delete`
		// rejects this arg shape ("you may not specify resource arguments as
		// well").
		delArgs := []string{"delete", "--ignore-not-found", "-f", manifest}
		if KubeContext != "" {
			delArgs = append([]string{"--context=" + KubeContext}, delArgs...)
		}
		RunCmd(t, "kubectl", delArgs...)
	})

	return FixtureName("ate-e2e-probe") + "-" + name
}

// renderProbeManifest renders the probe fixture for cfg. Split out of
// DeployProbe so the rendering has a unit test that needs no cluster.
func renderProbeManifest(t *testing.T, bucket, name string, cfg probeConfig) string {
	t.Helper()
	inline, blocks := fixtureSubstitutions(bucket, name)
	if cfg.trustBundle {
		blocks["${TEMPLATE_TRUST_BUNDLE}"] = trustBundleDataSource
	}
	return renderManifest(t, probeManifest, inline, blocks)
}
