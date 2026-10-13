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

package main

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/blobstore"
	"sigs.k8s.io/yaml"
)

func TestDeploymentComponentReplacesObjectStorePluginSidecars(t *testing.T) {
	data, err := os.ReadFile("../../config/kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var component struct {
		Patches []struct {
			Target struct {
				Kind string `json:"kind"`
			} `json:"target"`
			Patch string `json:"patch"`
		} `json:"patches"`
	}
	if err := yaml.Unmarshal(data, &component); err != nil {
		t.Fatalf("parsing component: %v", err)
	}

	wantCallers := map[string]string{
		"Deployment": "control",
		"DaemonSet":  "node",
	}
	if len(component.Patches) != len(wantCallers) {
		t.Fatalf("component has %d patches, want %d", len(component.Patches), len(wantCallers))
	}
	for _, patch := range component.Patches {
		mode, ok := wantCallers[patch.Target.Kind]
		if !ok {
			t.Errorf("unexpected patch target kind %q", patch.Target.Kind)
			continue
		}
		assertSidecarPatch(t, patch.Patch, mode)
		delete(wantCallers, patch.Target.Kind)
	}
	for kind := range wantCallers {
		t.Errorf("component has no patch for %s", kind)
	}
}

// TestDeploymentAssetsComponent checks that the assets component only names
// the asset containers of atelet's plugin, and that the main component's node
// arguments serve them once it does.
func TestDeploymentAssetsComponent(t *testing.T) {
	var component struct {
		Patches []struct {
			Target struct {
				Kind          string `json:"kind"`
				LabelSelector string `json:"labelSelector"`
			} `json:"target"`
			Patch string `json:"patch"`
		} `json:"patches"`
	}
	data, err := os.ReadFile("../../config/assets/kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &component); err != nil {
		t.Fatalf("parsing assets component: %v", err)
	}
	if len(component.Patches) != 1 || component.Patches[0].Target.Kind != "DaemonSet" || component.Patches[0].Target.LabelSelector != "app=atelet" {
		t.Fatalf("assets component patches %+v, want one patch of the atelet DaemonSet", component.Patches)
	}
	var patch workloadPatch
	if err := yaml.Unmarshal([]byte(component.Patches[0].Patch), &patch); err != nil {
		t.Fatalf("parsing assets patch: %v", err)
	}
	spec := patch.Spec.Template.Spec
	if len(spec.Containers) != 0 || len(spec.Volumes) != 0 || len(spec.InitContainers) != 1 {
		t.Fatalf("assets patch changes more than one init container: %+v", spec)
	}
	sidecar := spec.InitContainers[0]
	if sidecar.Name != "objectstore-plugin" || sidecar.Args != nil || sidecar.StartupProbe != nil || len(sidecar.Env) != 1 ||
		sidecar.Env[0].Name != "AZURE_STORAGE_ASSET_CONTAINERS" {
		t.Fatalf("assets patch sidecar = %+v, want only AZURE_STORAGE_ASSET_CONTAINERS on objectstore-plugin", sidecar)
	}

	nodeArgs := componentNodeArgs(t)
	t.Setenv("AZURE_STORAGE_ASSET_CONTAINERS", sidecar.Env[0].Value)
	if err := run(t.Context(), nodeArgs, func(string, []string) (map[string]blobstore.Store, error) {
		t.Errorf("node sidecar reached the store factory with the asset container placeholder")
		return nil, errors.New("unexpected")
	}); err == nil || !strings.Contains(err.Error(), "REPLACE_WITH_ASSET_CONTAINERS") {
		t.Errorf("run() with the assets component's placeholder = %v, want it rejected", err)
	}

	t.Setenv("AZURE_STORAGE_ASSET_CONTAINERS", strings.ReplaceAll(sidecar.Env[0].Value, "REPLACE_WITH_ASSET_CONTAINERS", "assets"))
	errStop := errors.New("stop after parsing flags")
	var got []string
	err = run(t.Context(), nodeArgs, func(_ string, containers []string) (map[string]blobstore.Store, error) {
		got = containers
		return nil, errStop
	})
	if want := []string{"snapshots", "assets"}; !errors.Is(err, errStop) || !slices.Equal(got, want) {
		t.Errorf("run() with the assets component = %v with containers %v, want the store factory reached with %v", err, got, want)
	}
}

// componentNodeArgs returns the main component's node sidecar arguments with
// the snapshot container placeholder replaced by "snapshots".
func componentNodeArgs(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("../../config/kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var component struct {
		Patches []struct {
			Target struct {
				Kind string `json:"kind"`
			} `json:"target"`
			Patch string `json:"patch"`
		} `json:"patches"`
	}
	if err := yaml.Unmarshal(data, &component); err != nil {
		t.Fatalf("parsing component: %v", err)
	}
	for _, p := range component.Patches {
		if p.Target.Kind != "DaemonSet" {
			continue
		}
		var patch workloadPatch
		if err := yaml.Unmarshal([]byte(p.Patch), &patch); err != nil {
			t.Fatalf("parsing node patch: %v", err)
		}
		args := slices.Clone(findContainer(t, patch.Spec.Template.Spec.InitContainers, "objectstore-plugin").Args)
		for i, arg := range args {
			args[i] = strings.ReplaceAll(arg, "REPLACE_WITH_CONTAINERS", "snapshots")
		}
		return args
	}
	t.Fatal("component has no DaemonSet patch")
	return nil
}

type workloadPatch struct {
	Spec struct {
		Template struct {
			Metadata struct {
				Labels map[string]string `json:"labels"`
			} `json:"metadata"`
			Spec struct {
				Containers     []container `json:"containers"`
				InitContainers []container `json:"initContainers"`
				Volumes        []struct {
					Name      string `json:"name"`
					Projected *struct {
						Sources []struct {
							ServiceAccountToken *struct {
								Audience string `json:"audience"`
								Path     string `json:"path"`
							} `json:"serviceAccountToken"`
						} `json:"sources"`
					} `json:"projected"`
				} `json:"volumes"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

type container struct {
	Name         string   `json:"name"`
	Args         []string `json:"args"`
	StartupProbe *struct {
		Exec *struct {
			Command []string `json:"command"`
		} `json:"exec"`
	} `json:"startupProbe"`
	Env []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
		Patch string `json:"$patch"`
	} `json:"env"`
	VolumeMounts []struct {
		Name      string `json:"name"`
		MountPath string `json:"mountPath"`
	} `json:"volumeMounts"`
}

// assertSidecarPatch checks that a patch replaces only the base
// objectstore-plugin sidecar: its image, arguments, startup probe, and Azure
// workload identity, without touching the caller container.
func assertSidecarPatch(t *testing.T, raw, mode string) {
	t.Helper()

	var patch workloadPatch
	if err := yaml.Unmarshal([]byte(raw), &patch); err != nil {
		t.Fatalf("parsing %s patch: %v", mode, err)
	}
	if _, ok := patch.Spec.Template.Metadata.Labels["azure.workload.identity/use"]; ok {
		t.Errorf("%s patch triggers whole-Pod workload identity mutation", mode)
	}
	if len(patch.Spec.Template.Spec.Containers) != 0 {
		t.Errorf("%s patch changes caller containers %v", mode, patch.Spec.Template.Spec.Containers)
	}

	sidecar := findContainer(t, patch.Spec.Template.Spec.InitContainers, "objectstore-plugin")
	socket := "--socket=/run/objectstore-plugin/" + mode + ".sock"
	if len(sidecar.Args) < 2 || sidecar.Args[0] != mode || sidecar.Args[1] != socket {
		t.Errorf("%s sidecar args = %v, want %s %s ...", mode, sidecar.Args, mode, socket)
	}
	// An unreplaced container placeholder must fail at startup.
	if err := run(t.Context(), sidecar.Args, func(string, []string) (map[string]blobstore.Store, error) {
		t.Errorf("%s sidecar reached the store factory with the container placeholder", mode)
		return nil, errors.New("unexpected")
	}); err == nil || !strings.Contains(err.Error(), "REPLACE_WITH_CONTAINERS") {
		t.Errorf("%s sidecar args %v: run() = %v, want the container placeholder rejected", mode, sidecar.Args, err)
	}
	args := slices.Clone(sidecar.Args)
	for i, arg := range args {
		args[i] = strings.ReplaceAll(arg, "REPLACE_WITH_CONTAINERS", "snapshots,other")
	}
	// Snapshot-only installs use this component as it is: no asset
	// container flag to fill in or delete.
	wantContainers := []string{"snapshots", "other"}
	if mode == "node" {
		if slices.ContainsFunc(args, func(arg string) bool { return strings.HasPrefix(arg, "--asset-containers") }) {
			t.Errorf("node sidecar args %v set --asset-containers; only the assets component may", args)
		}
		// The base manifest's asset-staging volume, which only this
		// container mounts, and its capacity.
		for _, want := range []string{
			"--asset-staging-dir=/var/lib/objectstore-plugin/asset-staging",
			"--asset-staging-capacity=8Gi",
		} {
			if !slices.Contains(args, want) {
				t.Errorf("node sidecar args %v do not include %s, as the base manifest does", args, want)
			}
		}
	}
	// Once replaced, the component's arguments parse with this binary's flags.
	errStop := errors.New("stop after parsing flags")
	var gotContainers []string
	err := run(t.Context(), args, func(_ string, containers []string) (map[string]blobstore.Store, error) {
		gotContainers = containers
		return nil, errStop
	})
	if !errors.Is(err, errStop) || !slices.Equal(gotContainers, wantContainers) {
		t.Errorf("%s sidecar args %v: run() = %v with containers %v, want the store factory reached with %v", mode, args, err, gotContainers, wantContainers)
	}
	wantProbe := []string{"/ko-app/azure-blob-objectstore-plugin", "healthcheck", socket}
	if sidecar.StartupProbe == nil || sidecar.StartupProbe.Exec == nil || !slices.Equal(sidecar.StartupProbe.Exec.Command, wantProbe) {
		t.Errorf("%s sidecar startup probe does not run %v", mode, wantProbe)
	}
	assertEnvNames(t, sidecar, []string{
		"AZURE_CLIENT_ID",
		"AZURE_TENANT_ID",
		"AZURE_FEDERATED_TOKEN_FILE",
	}, []string{"ATE_STORAGE_BACKEND"})
	assertMounts(t, sidecar, map[string]string{"azure-identity-token": "/var/run/secrets/azure/tokens"})

	var foundToken bool
	for _, volume := range patch.Spec.Template.Spec.Volumes {
		if volume.Name != "azure-identity-token" || volume.Projected == nil {
			continue
		}
		for _, source := range volume.Projected.Sources {
			token := source.ServiceAccountToken
			if token != nil &&
				token.Audience == "api://AzureADTokenExchange" &&
				token.Path == "azure-identity-token" {
				foundToken = true
			}
		}
	}
	if !foundToken {
		t.Errorf("%s patch has no Azure federated service-account token projection", mode)
	}
}

func findContainer(t *testing.T, containers []container, name string) container {
	t.Helper()
	for _, container := range containers {
		if container.Name == name {
			return container
		}
	}
	t.Fatalf("container %q not found", name)
	return container{}
}

// assertEnvNames checks that container sets exactly the want variables and
// deletes exactly the deleted ones from the base.
func assertEnvNames(t *testing.T, container container, want, deleted []string) {
	t.Helper()
	var set, removed []string
	for _, env := range container.Env {
		if env.Patch == "delete" {
			removed = append(removed, env.Name)
		} else {
			set = append(set, env.Name)
		}
	}
	slices.Sort(set)
	slices.Sort(removed)
	want, deleted = slices.Sorted(slices.Values(want)), slices.Sorted(slices.Values(deleted))
	if !slices.Equal(set, want) {
		t.Errorf("%s sets environment variables %v, want %v", container.Name, set, want)
	}
	if !slices.Equal(removed, deleted) {
		t.Errorf("%s deletes environment variables %v, want %v", container.Name, removed, deleted)
	}
}

func assertMounts(t *testing.T, container container, want map[string]string) {
	t.Helper()
	if len(container.VolumeMounts) != len(want) {
		t.Errorf("%s has volume mounts %v, want %v", container.Name, container.VolumeMounts, want)
		return
	}
	for name, path := range want {
		assertHasMount(t, container, name, path)
	}
}

func assertHasMount(t *testing.T, container container, name, path string) {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		if mount.Name == name && mount.MountPath == path {
			return
		}
	}
	t.Errorf("%s has no %s mount at %s", container.Name, name, path)
}
