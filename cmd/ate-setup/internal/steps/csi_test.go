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

package steps

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
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

// The prerequisites SetupCSI installs -- the CRDs, the ate-system namespace,
// the pod certificate CAs, a podcertificate controller rollout -- are
// prerequisites for a driver, and nothing else needs them. Running them for
// --setup-csi=none, which is what `deploy ate-system` passes by default,
// installs a controller nobody asked for; running them before the Kind check
// leaves that controller behind on the way to refusing a hostpath install on a
// cluster that was never going to support it; and running them before the value
// is understood at all does the same for a typo.
func TestSetupCSIDecidesBeforeTouchingTheCluster(t *testing.T) {
	for _, tc := range []struct {
		name   string
		driver string
		kind   bool
		error  string
	}{
		{name: "none", driver: "none"},
		{name: "the shell installer's false", driver: "false"},
		{name: "unset", driver: ""},
		{name: "hostpath off Kind", driver: "hostpath", error: "only supported for Kind"},
		{name: "both off Kind", driver: "both", error: "only supported for Kind"},
		{name: "the shell installer's true off Kind", driver: "true", error: "only supported for Kind"},
		{name: "unknown driver", driver: "nfsv4", error: `unknown CSI driver "nfsv4"`},
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

// servicednssigner issues the socat sidecar's serving certificate from the
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
			"csi-hostpath-controller Service; the socat sidecar cannot be issued a " +
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

// Both spellings of each driver selection have to keep selecting the same
// thing: --setup-csi was a boolean in the shell installer, and the scripts and
// dev-env files that still pass true or false have to keep working.
func TestCSIDriverSpellings(t *testing.T) {
	for _, tc := range []struct {
		driver string
		want   csiDriverSpec
	}{
		{"nfs", csiDriverSpec{nfs: true}},
		{"hostpath", csiDriverSpec{hostpath: true}},
		{"both", csiDriverSpec{hostpath: true, nfs: true}},
		{"true", csiDriverSpec{hostpath: true, nfs: true}},
		{"none", csiDriverSpec{}},
		{"false", csiDriverSpec{}},
		{"", csiDriverSpec{}},
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
