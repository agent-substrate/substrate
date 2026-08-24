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
	"os"
	"testing"
	"time"
)

// SandboxClassMicroVM is the kata + cloud-hypervisor runtime, spelled as the
// WorkerPool/ActorTemplate spec.sandboxClass field spells it.
const SandboxClassMicroVM = "microvm"

// sandboxClassEnv selects which runtime's fixtures the suites build actors
// from. Unset means gVisor: the default every runtime-agnostic suite ran
// against before the micro-VM lane existed.
const sandboxClassEnv = "E2E_SANDBOX_CLASS"

// SandboxClass returns the sandbox class under test, "" for gVisor. CI sets
// E2E_SANDBOX_CLASS=microvm for the micro-VM lane and leaves it unset for the
// gVisor one, so a single knob repoints every suite's fixtures.
func SandboxClass() string { return os.Getenv(sandboxClassEnv) }

// IsMicroVM reports whether the suites are pointed at the micro-VM fixtures.
// Assertions that only hold for one runtime gate on this.
func IsMicroVM() bool { return SandboxClass() == SandboxClassMicroVM }

// Fixture identifies an installed WorkerPool + ActorTemplate pair (both carry
// the same name) that suites either create Actors from directly or copy the
// resolved runtime — sandbox class, ateom image, container images — out of.
type Fixture struct {
	Namespace string
	Name      string
	// DeployWith is the install flag or script that creates the fixture, so a
	// missing one reports how to fix it rather than just failing.
	DeployWith string
}

// CounterFixture returns the counter demo for the sandbox class under test.
// E2E_TEMPLATE_NAMESPACE / E2E_TEMPLATE_NAME override it, for a cluster that
// installs the fixture somewhere else.
func CounterFixture() Fixture {
	f := Fixture{
		Namespace:  "ate-demo-counter",
		Name:       "counter",
		DeployWith: "hack/install-ate-kind.sh --deploy-demo-counter",
	}
	if IsMicroVM() {
		f = Fixture{
			Namespace:  "ate-demo-counter-microvm",
			Name:       "counter-microvm",
			DeployWith: "hack/run-microvm-demo-kind.sh",
		}
	}
	if v := os.Getenv("E2E_TEMPLATE_NAMESPACE"); v != "" {
		f.Namespace = v
	}
	if v := os.Getenv("E2E_TEMPLATE_NAME"); v != "" {
		f.Name = v
	}
	return f
}

// EgressFixture returns the egress demo for the sandbox class under test.
func EgressFixture() Fixture {
	if IsMicroVM() {
		return Fixture{
			Namespace:  "ate-demo-egress-microvm",
			Name:       "egress-microvm",
			DeployWith: "hack/install-ate-kind.sh --deploy-demo-egress-microvm",
		}
	}
	return Fixture{
		Namespace:  "ate-demo-egress",
		Name:       "egress",
		DeployWith: "hack/install-ate-kind.sh --deploy-demo-egress",
	}
}

// FixtureName suffixes a fixture's name for the sandbox class under test, so
// the gVisor and micro-VM lanes never share one. That matters most for the
// namespaces a suite creates and deletes itself: the two lanes run one after
// the other, and a namespace still Terminating from the previous one would
// fail the next one's apply. ${FIXTURE_SUFFIX} does the same job inside the
// fixture manifests.
func FixtureName(base string) string {
	if IsMicroVM() {
		return base + "-" + SandboxClassMicroVM
	}
	return base
}

// TemplateReadyTimeout is how long to wait for an ActorTemplate's golden
// snapshot. A micro-VM golden (a cloud-hypervisor cold boot plus checkpoint, on
// nested KVM in CI) takes several times what a gVisor one does, so the default
// follows the class under test. E2E_TEMPLATE_READY_TIMEOUT overrides it.
func TemplateReadyTimeout(t *testing.T) time.Duration {
	t.Helper()
	d := 90 * time.Second
	if IsMicroVM() {
		d = 10 * time.Minute
	}
	if v := os.Getenv("E2E_TEMPLATE_READY_TIMEOUT"); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("invalid E2E_TEMPLATE_READY_TIMEOUT %q: %v", v, err)
		}
		d = parsed
	}
	return d
}

// RenderFixtureManifest renders the manifest template at relPath (repo-relative,
// under internal/e2e/fixtures) for the sandbox class under test, writes it into
// the test's temp dir and returns that path. Both an apply and a later delete
// can then consume the same file, with no shell involved.
//
// name distinguishes the caller (by convention its suite name) and is appended
// to ${FIXTURE_SUFFIX}: suite packages run as concurrent processes, so each
// caller must get its own copy of a fixture or one suite's cleanup deletes it
// out from under another.
//
// One template serves both sandbox classes so the two variants of a fixture
// cannot drift apart. See renderManifest for the placeholder kinds a template
// can carry.
func RenderFixtureManifest(t *testing.T, relPath, bucket, name string) string {
	t.Helper()
	inline, blocks := fixtureSubstitutions(bucket, name)
	return renderManifest(t, relPath, inline, blocks)
}

// fixtureSubstitutions is the placeholder set the internal/e2e/fixtures
// manifest templates carry, split into the inline and whole-line-block kinds
// RenderFixtureManifest treats differently.
func fixtureSubstitutions(bucket, name string) (inline, blocks map[string]string) {
	inline = map[string]string{
		"${BUCKET_NAME}": bucket,
		"${ATEOM_IMAGE}": "ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor",
		// The manifest-side half of FixtureName: it suffixes the fixture's
		// namespace, and with it the snapshot prefix underneath.
		"${FIXTURE_SUFFIX}": "-" + name,
	}
	blocks = map[string]string{
		"${WORKERPOOL_RUNTIME}":     "",
		"${TEMPLATE_SANDBOX_CLASS}": "",
		"${TEMPLATE_RESOURCES}":     "",
	}
	if !IsMicroVM() {
		return inline, blocks
	}

	inline["${ATEOM_IMAGE}"] = "ko://github.com/agent-substrate/substrate/cmd/ateom-microvm"
	inline["${FIXTURE_SUFFIX}"] = "-" + SandboxClassMicroVM + "-" + name
	// The cluster-wide SandboxConfig hack/install-microvm-deps.sh installs. A
	// micro-VM WorkerPool has to name it: it is deliberately not the class
	// default, so a missing or stale one fails loudly.
	blocks["${WORKERPOOL_RUNTIME}"] = "  sandboxClass: microvm\n  sandboxConfigName: microvm"
	// Must match the WorkerPool's: a snapshot is not portable across sandbox
	// classes, so only same-class pools are eligible to run these actors.
	blocks["${TEMPLATE_SANDBOX_CLASS}"] = "  sandboxClass: microvm"
	// Only for fixtures that declare no limits of their own. Without them the
	// guest boots at the kata config's default (2GiB), and several of those do
	// not fit beside the demo pools on CI's single kind node. These size the VM
	// itself — see internal/sizing.
	blocks["${TEMPLATE_RESOURCES}"] = "  resources:\n    limits:\n      cpu: \"1\"\n      memory: 512Mi"
	return inline, blocks
}
