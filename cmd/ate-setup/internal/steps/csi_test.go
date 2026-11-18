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

package steps

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
)

// setupCSIWithoutACluster runs SetupCSI against an Env holding no cluster
// client, and reports the nil dereference that any step reaching for one would
// cause as a failure rather than a panicking test binary.
func setupCSIWithoutACluster(t *testing.T, e *Env, driver string) error {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("SetupCSI(%q) reached for the cluster: %v", driver, r)
		}
	}()
	return e.SetupCSI(context.Background(), driver)
}

// SetupCSI installs the ate-system namespace, the pod certificate CAs, and a
// podcertificate controller rollout only when a driver is selected. Running
// them for --setup-csi=none, which `deploy ate-system` passes by default,
// installs a controller nobody asked for. Running them before the Kind check
// or driver validation leaves that controller behind when a request fails.
func TestSetupCSIDecidesBeforeTouchingTheCluster(t *testing.T) {
	for _, tc := range []struct {
		name   string
		driver string
		kind   bool
		error  string
	}{
		{name: "none", driver: "none"},
		{name: "hostpath off Kind", driver: "hostpath", error: "only supported for Kind"},
		{name: "both off Kind", driver: "both", error: "only supported for Kind"},
		{name: "unknown driver", driver: "nfsv4", error: `unknown CSI driver "nfsv4"`},
		{name: "the shell installer's true", driver: "true", error: `unknown CSI driver "true"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{Kind: tc.kind}}

			err := setupCSIWithoutACluster(t, e, tc.driver)
			if tc.error == "" {
				if err != nil {
					t.Fatalf("SetupCSI(%q) = %v, want nil", tc.driver, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("SetupCSI(%q) = nil, want an error containing %q", tc.driver, tc.error)
			}
			if !strings.Contains(err.Error(), tc.error) {
				t.Errorf("SetupCSI(%q) error = %v, want it to contain %q", tc.driver, err, tc.error)
			}
		})
	}
}

// functionBody returns the source of a top-level function in file, from its
// signature to the closing brace in the first column.
func functionBody(t *testing.T, file, signature string) string {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("reading %s: %v", file, err)
	}
	start := strings.Index(string(src), signature)
	if start < 0 {
		t.Fatalf("%s does not contain %q; this test no longer checks anything", file, signature)
	}
	body := string(src)[start:]
	end := strings.Index(body, "\n}\n")
	if end < 0 {
		t.Fatalf("%s: no end found for %q", file, signature)
	}
	return body[:end]
}

// servicednssigner issues the Envoy sidecar's serving certificate from the
// csi-hostpath-controller Service, so a driver deployed before that Service
// exists has nothing to be issued against and never becomes ready. The shell
// installer created the Service first and said why.
//
// The step itself wants a Kind cluster and docker, so the ordering is pinned in
// the source rather than exercised.
func TestTheHostpathControllerServiceIsCreatedBeforeTheDriver(t *testing.T) {
	body := functionBody(t, "csi.go", "func (e *Env) setupCSIHostpath(")

	service := strings.Index(body, "csiHostpathControllerService")
	driver := strings.Index(body, "ApplyTolerant")
	switch {
	case service < 0:
		t.Fatal("setupCSIHostpath no longer applies csiHostpathControllerService")
	case driver < 0:
		t.Fatal("setupCSIHostpath no longer applies the driver bundle with ApplyTolerant")
	case service > driver:
		t.Error("setupCSIHostpath deploys the hostpath driver before creating the " +
			"csi-hostpath-controller Service; the Envoy sidecar cannot be issued a " +
			"certificate until the Service exists")
	}
}

// The e2e tests, the demos and the benchmarks all provision from csi-nfs-sc, so
// both NFS setups apply the one manifest that defines it rather than each
// carrying a copy that can drift. The step wants a cluster, so which file it
// applies is pinned in the source.
func TestTheNFSSetupsApplyTheSharedStorageClass(t *testing.T) {
	const manifest = "hack/third_party/csi-driver-nfs/deploy/example/storageclass-nfs.yaml"
	root := repoRoot(t)

	objs, err := kube.LoadPath(filepath.Join(root, manifest))
	if err != nil {
		t.Fatalf("loading %s: %v", manifest, err)
	}
	if len(objs) != 1 || objs[0].GetKind() != "StorageClass" || objs[0].GetName() != "csi-nfs-sc" {
		t.Fatalf("%s holds %d object(s), want only the csi-nfs-sc StorageClass", manifest, len(objs))
	}
	if p, _, _ := unstructured.NestedString(objs[0].Object, "provisioner"); p != "nfs.csi.k8s.io" {
		t.Errorf("csi-nfs-sc provisioner = %q, want nfs.csi.k8s.io", p)
	}

	goSetup := functionBody(t, "csi.go", "func (e *Env) setupCSINFS(")
	if !strings.Contains(goSetup, `deployDir+"/example/storageclass-nfs.yaml"`) {
		t.Errorf("setupCSINFS no longer applies %s", manifest)
	}
	script, err := os.ReadFile(filepath.Join(root, "hack", "setup-csi-nfs-kind.sh"))
	if err != nil {
		t.Fatalf("reading the shell setup: %v", err)
	}
	if !strings.Contains(string(script), manifest) {
		t.Errorf("hack/setup-csi-nfs-kind.sh no longer applies %s", manifest)
	}
	for name, src := range map[string]string{
		"setupCSINFS":                goSetup,
		"hack/setup-csi-nfs-kind.sh": string(script),
	} {
		if strings.Contains(src, "kind: StorageClass") {
			t.Errorf("%s defines a StorageClass inline; apply %s instead", name, manifest)
		}
	}
}

// Both NFS setups register the canonical csi-nfs CsiDriverConfig from the
// same checked-in manifest so neither carries an inline copy that can drift.
func TestTheNFSSetupsRegisterTheSharedCSIDriverConfig(t *testing.T) {
	const manifest = "hack/csi/csi-driver-config-nfs.yaml"
	root := repoRoot(t)

	raw, err := os.ReadFile(filepath.Join(root, manifest))
	if err != nil {
		t.Fatalf("reading %s: %v", manifest, err)
	}
	cfg, err := CSIDriverConfigFromManifest(raw)
	if err != nil {
		t.Fatalf("parsing %s: %v", manifest, err)
	}
	if cfg.GetMetadata().GetName() != "csi-nfs" {
		t.Errorf("csi-nfs metadata.name = %q, want csi-nfs", cfg.GetMetadata().GetName())
	}
	if cfg.GetDriverName() != "nfs.csi.k8s.io" {
		t.Errorf("csi-nfs driverName = %q, want nfs.csi.k8s.io", cfg.GetDriverName())
	}
	var svc corev1.Service
	decodeYAML(t, "csiNFSControllerService", csiNFSControllerService, &svc)
	if !cfg.GetTls().GetEnabled() || !cfg.GetTls().GetUsePodIdentity() {
		t.Fatalf("csi-nfs tls = %+v, want enabled with pod identity", cfg.GetTls())
	}
	if want := svc.Name + "." + svc.Namespace + ".svc"; cfg.GetTls().GetServerName() != want {
		t.Errorf("csi-nfs tls.serverName = %q, want %q", cfg.GetTls().GetServerName(), want)
	}
	if want := "tcp://" + svc.Name + "." + svc.Namespace + ".svc.cluster.local:" + strconv.Itoa(int(svc.Spec.Ports[0].Port)); cfg.GetControllerEndpoint() != want {
		t.Errorf("csi-nfs controllerEndpoint = %q, want %q", cfg.GetControllerEndpoint(), want)
	}

	goSetup := functionBody(t, "substrate.go", "func csiDriverConfigsForDriver(")
	if !strings.Contains(goSetup, `"csi-driver-config-nfs.yaml"`) {
		t.Errorf("csiDriverConfigsForDriver no longer loads %s", manifest)
	}
	script, err := os.ReadFile(filepath.Join(root, "hack", "setup-csi-nfs-kind.sh"))
	if err != nil {
		t.Fatalf("reading the shell setup: %v", err)
	}
	if !strings.Contains(string(script), manifest) {
		t.Errorf("hack/setup-csi-nfs-kind.sh no longer registers %s", manifest)
	}
	for name, src := range map[string]string{
		"csiDriverConfigsForDriver":  goSetup,
		"hack/setup-csi-nfs-kind.sh": string(script),
	} {
		if strings.Contains(src, "\ndriverName:") {
			t.Errorf("%s defines a CsiDriverConfig inline; register %s instead", name, manifest)
		}
	}
}

// Each --setup-csi value selects the drivers its name says.
func TestCSIDriverSpellings(t *testing.T) {
	for _, tc := range []struct {
		driver string
		want   csiDriverSpec
	}{
		{"nfs", csiDriverSpec{nfs: true}},
		{"hostpath", csiDriverSpec{hostpath: true}},
		{"both", csiDriverSpec{hostpath: true, nfs: true}},
		{"none", csiDriverSpec{}},
	} {
		t.Run(tc.driver, func(t *testing.T) {
			got, ok := csiDrivers[tc.driver]
			if !ok {
				t.Fatalf("csiDrivers has no entry for %q", tc.driver)
			}
			if got != tc.want {
				t.Errorf("csiDrivers[%q] = %+v, want %+v", tc.driver, got, tc.want)
			}
			if got.installsNothing() != (got == csiDriverSpec{}) {
				t.Errorf("csiDrivers[%q].installsNothing() = %v for %+v", tc.driver, got.installsNothing(), got)
			}
		})
	}
}

// decodeYAML unmarshals a manifest defined in csi.go.
func decodeYAML(t *testing.T, name, src string, into any) {
	t.Helper()
	if err := yaml.Unmarshal([]byte(src), into); err != nil {
		t.Fatalf("%s does not parse: %v", name, err)
	}
}

// ateapi verifies tls.serverName, which must be the <service>.<namespace>.svc name servicednssigner issues.
func TestTheNFSDriverConfigEnablesTLSAgainstTheControllerService(t *testing.T) {
	var svc corev1.Service
	decodeYAML(t, "csiNFSControllerService", csiNFSControllerService, &svc)
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "hack", "csi", "csi-driver-config-nfs.yaml"))
	if err != nil {
		t.Fatalf("reading csi-driver-config-nfs.yaml: %v", err)
	}
	cfg, err := CSIDriverConfigFromManifest(raw)
	if err != nil {
		t.Fatalf("parsing csi-driver-config-nfs.yaml: %v", err)
	}

	if !cfg.GetTls().GetEnabled() || !cfg.GetTls().GetUsePodIdentity() {
		t.Fatalf("csi-driver-config-nfs.yaml tls = %+v, want enabled with pod identity", cfg.GetTls())
	}
	if want := svc.Name + "." + svc.Namespace + ".svc"; cfg.GetTls().GetServerName() != want {
		t.Errorf("tls.serverName = %q, want %q, the name servicednssigner issues", cfg.GetTls().GetServerName(), want)
	}
	if want := "tcp://" + svc.Name + "." + svc.Namespace + ".svc.cluster.local:" + strconv.Itoa(int(svc.Spec.Ports[0].Port)); cfg.GetControllerEndpoint() != want {
		t.Errorf("controllerEndpoint = %q, want %q", cfg.GetControllerEndpoint(), want)
	}
}

// hack/setup-csi-nfs-kind.sh is the same install by hand and must keep the same security decisions.
func TestTheNFSShellSetupMatchesTheGoSetup(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "hack", "setup-csi-nfs-kind.sh"))
	if err != nil {
		t.Fatalf("reading the shell setup: %v", err)
	}
	src := string(script)

	for _, forbidden := range []string{"socat:", "privileged: true", "kind: CSIDriverConfig"} {
		if strings.Contains(src, forbidden) {
			t.Errorf("hack/setup-csi-nfs-kind.sh still contains %q", forbidden)
		}
	}
	for _, required := range []string{
		"require_client_certificate: true",
		"/sa/ate-api-server",
		"ExternalVolumes",
		"--preview create csi-driver-config -f",
	} {
		if !strings.Contains(src, required) {
			t.Errorf("hack/setup-csi-nfs-kind.sh no longer contains %q", required)
		}
	}
	if service, driver := strings.Index(src, "name: csi-nfs-controller\n  namespace: kube-system"), strings.Index(src, "csi-nfs-controller.yaml"); service < 0 || driver < 0 || service > driver {
		t.Error("hack/setup-csi-nfs-kind.sh deploys the NFS driver before creating the csi-nfs-controller Service")
	}
	if cdc, restart := strings.Index(src, "csi-driver-config-nfs.yaml"), strings.Index(src, "rollout restart daemonset/atelet"); cdc < 0 || restart < 0 || cdc > restart {
		t.Error("hack/setup-csi-nfs-kind.sh restarts atelet before registering the csi-nfs CsiDriverConfig")
	}
}

// The hostpath proxy needs no more than root to reach the driver's socket.
func TestTheHostpathProxyIsNotPrivileged(t *testing.T) {
	objs, err := kube.LoadPath(filepath.Join(repoRoot(t), "hack", "third_party", "csi-driver-host-path", "deploy", "csi-hostpath-testing.yaml"))
	if err != nil {
		t.Fatalf("loading the hostpath proxy manifest: %v", err)
	}
	var found bool
	for _, obj := range objs {
		if obj.GetKind() != "StatefulSet" {
			continue
		}
		containers, _, _ := unstructured.NestedSlice(obj.Object, "spec", "template", "spec", "containers")
		for _, c := range containers {
			c := c.(map[string]any)
			found = true
			if privileged, _, _ := unstructured.NestedBool(c, "securityContext", "privileged"); privileged {
				t.Errorf("%s container %q is privileged", kube.Describe(obj), c["name"])
			}
		}
	}
	if !found {
		t.Fatal("csi-hostpath-testing.yaml no longer holds a proxy StatefulSet")
	}
}
