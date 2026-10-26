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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
)

// sizing holds the fields the sizing Components set on a workload: replicas,
// container resources, and the PodDisruptionBudget's budget.
type sizing struct {
	Replicas  *int
	Resources map[string]map[string]map[string]string
	PDB       map[string]any
}

// sizingOf collects the sizing of every workload and disruption budget in a
// manifest, by name.
func sizingOf(t *testing.T, manifest []byte) map[string]sizing {
	t.Helper()
	out := map[string]sizing{}
	for _, doc := range strings.Split(string(manifest), "\n---\n") {
		var obj struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Replicas       *int `json:"replicas"`
				MaxUnavailable any  `json:"maxUnavailable"`
				MinAvailable   any  `json:"minAvailable"`
				Template       struct {
					Spec struct {
						InitContainers []container `json:"initContainers"`
						Containers     []container `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("rendered document is not valid YAML: %v", err)
		}
		s := out[obj.Metadata.Name]
		switch obj.Kind {
		case "Deployment", "StatefulSet", "DaemonSet":
			s.Replicas = obj.Spec.Replicas
			for _, c := range slices.Concat(obj.Spec.Template.Spec.InitContainers, obj.Spec.Template.Spec.Containers) {
				if c.Resources == nil {
					continue
				}
				if s.Resources == nil {
					s.Resources = map[string]map[string]map[string]string{}
				}
				s.Resources[c.Name] = c.Resources
			}
		case "PodDisruptionBudget":
			s.PDB = map[string]any{}
			if obj.Spec.MaxUnavailable != nil {
				s.PDB["maxUnavailable"] = obj.Spec.MaxUnavailable
			}
			if obj.Spec.MinAvailable != nil {
				s.PDB["minAvailable"] = obj.Spec.MinAvailable
			}
		default:
			continue
		}
		out[obj.Metadata.Name] = s
	}
	return out
}

type container struct {
	Name      string                       `json:"name"`
	Resources map[string]map[string]string `json:"resources"`
}

func replicas(n int) *int { return &n }

// defaultSizing is the sizing components/sizing-default gives each workload in
// the install manifests.
var defaultSizing = map[string]sizing{
	"ate-api-server": {Replicas: replicas(2), PDB: map[string]any{"maxUnavailable": float64(1)}},
	"ate-idp-server": {Replicas: replicas(2), PDB: map[string]any{"maxUnavailable": float64(1)}},
	"ate-controller": {Replicas: replicas(1)},
	"atelet": {Resources: map[string]map[string]map[string]string{
		"objectstore-plugin": {"requests": {"cpu": "50m", "memory": "128Mi"}},
		"atelet":             {"requests": {"cpu": "50m", "memory": "128Mi"}},
	}},
	"atenet-router": {Replicas: replicas(1)},
	"atenet-egress": {Replicas: replicas(1), Resources: map[string]map[string]map[string]string{
		"sdsmint": {"requests": {"cpu": "200m", "memory": "512Mi"}, "limits": {"memory": "768Mi"}},
	}},
	"podcertificate-controller": {Replicas: replicas(1)},
	"postgres": {Replicas: replicas(1), Resources: map[string]map[string]map[string]string{
		"tls-reloader": {"requests": {"cpu": "10m", "memory": "32Mi"}},
		"postgres":     {"requests": {"cpu": "2", "memory": "1Gi"}, "limits": {"cpu": "16", "memory": "2Gi"}},
	}},
}

// The sizing components reproduce the sizing the base manifests carried before
// they moved out: today's values by default, and today's large profile under
// --cluster-size=size10.
func TestRenderSizing(t *testing.T) {
	withPostgres := func(resources map[string]map[string]string) map[string]sizing {
		out := map[string]sizing{}
		for name, s := range defaultSizing {
			out[name] = s
		}
		pg := out["postgres"]
		pg.Resources = map[string]map[string]map[string]string{
			"tls-reloader": defaultSizing["postgres"].Resources["tls-reloader"],
			"postgres":     resources,
		}
		out["postgres"] = pg
		return out
	}

	for _, tc := range []struct {
		name string
		cfg  config.Config
		want map[string]sizing
		// wantQPS says whether podcertificate-controller gets the large
		// profile's client-go rate limits.
		wantQPS bool
	}{
		{name: "default", want: defaultSizing},
		{
			name: "size10",
			cfg:  config.Config{ClusterSize: config.ClusterSizeSize10},
			want: withPostgres(map[string]map[string]string{
				"requests": {"cpu": "80", "memory": "140Gi"},
				"limits":   {"memory": "140Gi"},
			}),
			wantQPS: true,
		},
		{
			name: "kind",
			cfg:  config.Config{Kind: true},
			want: withPostgres(map[string]map[string]string{
				"requests": {"cpu": "250m", "memory": "1Gi"},
				"limits":   {"cpu": "2", "memory": "2Gi"},
			}),
		},
		{
			// The agentgateway egress drops sdsmint, so sizing-default must
			// not add it back.
			name: "agentgateway",
			cfg:  config.Config{Router: config.RouterAgentgateway},
			want: func() map[string]sizing {
				out := map[string]sizing{}
				for name, s := range defaultSizing {
					out[name] = s
				}
				out["atenet-egress"] = sizing{Replicas: replicas(1)}
				return out
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Root = repoRoot(t)
			if cfg.Router == "" {
				cfg.Router = config.RouterEnvoy
			}
			e := &Env{Cfg: &cfg}

			egress := e.atenetEgressManifestPath()
			if cfg.Router == config.RouterAgentgateway {
				egress = e.Cfg.Path(installDir + "/agentgateway-egress")
			}
			got := map[string]sizing{}
			var podcert []byte
			for _, path := range []string{
				e.Cfg.Path(SystemOverlay(e.Cfg)),
				egress,
				e.Cfg.Manifest("pod-certificate-controller.yaml"),
				e.postgresManifestPath(),
			} {
				rendered, err := e.render(path)
				if err != nil {
					t.Fatalf("render(%s): %v", path, err)
				}
				for name, s := range sizingOf(t, rendered) {
					got[name] = s
				}
				if strings.HasSuffix(path, "pod-certificate-controller.yaml") {
					podcert = rendered
				}
			}
			// The kind bundle also renders dev tools that take no sizing. Any
			// other workload must be in want, so the diff reports one these
			// paths render but defaultSizing lacks.
			for _, name := range []string{"jaeger", "opentelemetry-collector", "prometheus", "rustfs"} {
				delete(got, name)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("sizing mismatch (-want +got):\n%s", diff)
			}
			for _, flag := range []string{"--kube-api-qps=100", "--kube-api-burst=200"} {
				if strings.Contains(string(podcert), flag) != tc.wantQPS {
					t.Errorf("podcertificate-controller has %s = %v, want %v", flag, !tc.wantQPS, tc.wantQPS)
				}
			}
		})
	}
}

// The base manifests leave every sizing field to the sizing components, so a
// field no sizing input sets is not rendered and another manager can own it.
// Every workload they define needs default sizing. The kind dev tools under
// kind/ are not install manifests.
func TestBaseManifestsSetNoSizing(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	files, err := filepath.Glob(cfg.Manifest("*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	postgres, err := filepath.Glob(cfg.Manifest("postgres", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, file := range append(files, postgres...) {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for name, s := range sizingOf(t, raw) {
			found[name] = true
			if s.Replicas != nil || s.Resources != nil || len(s.PDB) > 0 {
				t.Errorf("%s sets sizing on %s: %+v", file, name, s)
			}
			if _, ok := defaultSizing[name]; !ok {
				t.Errorf("%s defines %s with no default sizing; add a patch to components/sizing-default and an entry to defaultSizing", file, name)
			}
		}
	}
	for name := range defaultSizing {
		if !found[name] {
			t.Errorf("defaultSizing has %s, which no install manifest defines", name)
		}
	}
}
