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
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// containerArgs maps "<kind>/<name>/<container>" to the container's args.
func containerArgs(t *testing.T, manifest []byte) map[string][]string {
	t.Helper()
	nodes, err := (&kio.ByteReader{Reader: bytes.NewReader(manifest), OmitReaderAnnotations: true}).Read()
	if err != nil {
		t.Fatalf("parsing manifest: %v", err)
	}
	out := map[string][]string{}
	for _, node := range nodes {
		for _, path := range containerListPaths {
			containers, err := node.Pipe(yaml.Lookup(path...))
			if err != nil || containers == nil {
				continue
			}
			elements, err := containers.Elements()
			if err != nil {
				t.Fatalf("listing containers: %v", err)
			}
			for _, c := range elements {
				name, _ := c.GetString("name")
				image, _ := c.GetString("image")
				key := node.GetKind() + "/" + node.GetName() + "/" + name
				var args []string
				if list, _ := c.Pipe(yaml.Lookup("args")); list != nil {
					for _, item := range list.YNode().Content {
						args = append(args, item.Value)
					}
				}
				// Tag the substrate containers so callers can tell them apart
				// without repeating the prefix check.
				if strings.HasPrefix(image, substrateImagePrefix) {
					key = "substrate:" + key
				}
				out[key] = args
			}
		}
	}
	return out
}

const previewTestManifest = `# leading comment
apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
data:
  image: ko://github.com/agent-substrate/substrate/cmd/ateapi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  template:
    spec:
      initContainers:
      - name: sidecar
        image: ko://github.com/agent-substrate/substrate/cmd/atenet
        restartPolicy: Always
        args:
        - "sdsmint"
      containers:
      - name: api
        image: ko://github.com/agent-substrate/substrate/cmd/ateapi
        args:
        - --port=8080  # keep me
        - --preview=Stale
      - name: bare
        image: ko://github.com/agent-substrate/substrate/cmd/atelet
      - name: envoy
        image: ${ENVOY_DATAPLANE_IMAGE}
        args:
        - -c
---
apiVersion: v1
kind: Pod
metadata:
  name: pod
spec:
  containers:
  - name: ctl
    image: ko://github.com/agent-substrate/substrate/cmd/atecontroller
`

func TestInjectPreviewArg(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		arg     string
	}{
		{enabled: true, arg: "--preview=*"},
		{enabled: false, arg: "--preview="},
	} {
		t.Run(tc.arg, func(t *testing.T) {
			out, err := injectPreviewArg([]byte(previewTestManifest), tc.enabled)
			if err != nil {
				t.Fatalf("injectPreviewArg: %v", err)
			}
			got := containerArgs(t, out)
			want := map[string][]string{
				"substrate:Deployment/api/sidecar": {"sdsmint", tc.arg},
				"substrate:Deployment/api/api":     {"--port=8080", tc.arg},
				"substrate:Deployment/api/bare":    {tc.arg},
				"Deployment/api/envoy":             {"-c"},
				"substrate:Pod/pod/ctl":            {tc.arg},
			}
			for key, wantArgs := range want {
				if !slices.Equal(got[key], wantArgs) {
					t.Errorf("%s args = %q, want %q", key, got[key], wantArgs)
				}
			}
			if len(got) != len(want) {
				t.Errorf("got containers %v, want %d", got, len(want))
			}
			// Everything else survives: comments, placeholders, and the
			// ConfigMap that merely mentions a substrate image.
			for _, s := range []string{"# keep me", "${ENVOY_DATAPLANE_IMAGE}", "kind: ConfigMap", "image: ko://github.com/agent-substrate/substrate/cmd/ateapi\n---"} {
				if !strings.Contains(string(out), s) {
					t.Errorf("output lost %q:\n%s", s, out)
				}
			}
		})
	}
}

func TestInjectPreviewArgLeavesOtherManifestsAlone(t *testing.T) {
	in := []byte("apiVersion: v1\nkind: Pod\nmetadata:   {name: x}\nspec:\n  containers:\n  - {name: c, image: busybox}\n")
	for _, enabled := range []bool{true, false} {
		out, err := injectPreviewArg(in, enabled)
		if err != nil {
			t.Fatalf("injectPreviewArg: %v", err)
		}
		if !bytes.Equal(out, in) {
			t.Errorf("enabled=%v: manifest changed:\n%s", enabled, out)
		}
	}
}

type recordingResolver struct{ got []byte }

func (r *recordingResolver) ResolveBytes(_ context.Context, manifest []byte) ([]byte, error) {
	r.got = manifest
	return manifest, nil
}

// Every component manifest ate-setup ships, rendered the way it applies it,
// gets --preview on each substrate container, and only once.
func TestResolveManifestInjectsPreview(t *testing.T) {
	root := repoRoot(t)
	sources := []struct {
		name string
		cfg  config.Config
		path func(e *Env) string
		// direct resolves path without rendering it, as DeployAtelet does.
		direct bool
		// want names substrate containers the rendered manifest must carry.
		want []string
	}{
		{
			name: "base bundle",
			cfg:  config.Config{Router: config.RouterEnvoy},
			path: func(e *Env) string { return e.Cfg.Path(SystemOverlay(e.Cfg)) },
			want: []string{"ate-api-server", "ate-controller", "atenet-router", "atelet-${SUBSTRATE_VERSION_SUFFIX}"},
		},
		{
			name: "kind bundle",
			cfg:  config.Config{Router: config.RouterEnvoy, Kind: true},
			path: func(e *Env) string { return e.Cfg.Path(SystemOverlay(e.Cfg)) },
			want: []string{"ate-api-server", "ate-controller", "atenet-router", "atelet-${SUBSTRATE_VERSION_SUFFIX}"},
		},
		{
			name: "agentgateway bundle",
			cfg:  config.Config{Router: config.RouterAgentgateway},
			path: func(e *Env) string { return e.Cfg.Path(SystemOverlay(e.Cfg)) },
			want: []string{"ate-api-server", "ate-controller"},
		},
		{
			name:   "atelet file",
			path:   func(e *Env) string { return e.Cfg.Manifest("atelet.yaml") },
			direct: true,
			want:   []string{"atelet-${SUBSTRATE_VERSION_SUFFIX}"},
		},
		{
			name: "podcert file",
			path: func(e *Env) string { return e.Cfg.Manifest("pod-certificate-controller.yaml") },
			want: []string{"podcertificate-controller"},
		},
		{
			name: "podcert size10 overlay",
			cfg:  config.Config{ClusterSize: config.ClusterSizeSize10},
			path: func(e *Env) string { return e.Cfg.Manifest("podcert-size10") },
			want: []string{"podcertificate-controller"},
		},
		{
			name: "router file",
			path: func(e *Env) string { return e.Cfg.Manifest("atenet-router.yaml") },
			want: []string{"atenet-router"},
		},
		{
			name: "egress file",
			path: func(e *Env) string { return e.atenetEgressManifestPath() },
			want: []string{"atenet-egress"},
		},
		{
			name: "egress sdsmint file",
			cfg:  config.Config{ExperimentalUseSDSMint: true},
			path: func(e *Env) string { return e.atenetEgressManifestPath() },
			want: []string{"atenet-egress"},
		},
		{
			name: "credential provider file",
			path: func(e *Env) string {
				return e.Cfg.Path("manifests", "egress-credential-injection", "k8s-credential-provider.yaml")
			},
			want: []string{"k8s-credential-provider"},
		},
	}
	for _, src := range sources {
		for _, enabled := range []bool{true, false} {
			arg := previewArg(enabled)
			t.Run(src.name+"/"+arg, func(t *testing.T) {
				cfg := src.cfg
				cfg.Root = root
				cfg.EnablePreview = enabled
				resolver := &recordingResolver{}
				e := &Env{Cfg: &cfg, resolver: resolver}

				path := src.path(e)
				var err error
				if src.direct {
					_, err = e.ResolveManifest(context.Background(), path)
				} else {
					_, err = e.renderResolve(context.Background(), path)
				}
				if err != nil {
					t.Fatalf("resolving %s: %v", path, err)
				}

				var original []byte
				if src.direct {
					original, err = kube.ReadPath(path)
				} else {
					original, err = e.render(path)
				}
				if err != nil {
					t.Fatalf("rendering %s: %v", path, err)
				}
				if diff := cmp.Diff(decodeAll(t, original), withoutPreview(decodeAll(t, resolver.got))); diff != "" {
					t.Errorf("injection changed more than --preview (-before +after):\n%s", diff)
				}

				found := map[string]bool{}
				for key, args := range containerArgs(t, resolver.got) {
					previews := 0
					for _, a := range args {
						if strings.HasPrefix(a, previewFlag) {
							previews++
						}
					}
					if !strings.HasPrefix(key, "substrate:") {
						if previews != 0 {
							t.Errorf("%s is not a substrate container but got %q", key, args)
						}
						continue
					}
					if previews != 1 || !slices.Contains(args, arg) {
						t.Errorf("%s args = %q, want exactly one %s", key, args, arg)
					}
					found[strings.Split(key, "/")[1]] = true
				}
				for _, name := range src.want {
					if !found[name] {
						t.Errorf("no substrate container in workload %s (found %v)", name, found)
					}
				}
			})
		}
	}
}

// decodeAll decodes every non-empty document in a YAML stream.
func decodeAll(t *testing.T, stream []byte) []any {
	t.Helper()
	var docs []any
	dec := yamlv3.NewDecoder(bytes.NewReader(stream))
	for {
		var doc any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return docs
		}
		if err != nil {
			t.Fatalf("decoding manifest: %v", err)
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
}

// withoutPreview strips every --preview argument, and any args list left empty
// by doing so, from decoded YAML.
func withoutPreview(v any) any {
	switch v := v.(type) {
	case []any:
		out := make([]any, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && strings.HasPrefix(s, previewFlag+"=") {
				continue
			}
			out = append(out, withoutPreview(item))
		}
		return out
	case map[string]any:
		for k, item := range v {
			v[k] = withoutPreview(item)
		}
		if args, ok := v["args"].([]any); ok && len(args) == 0 {
			delete(v, "args")
		}
		return v
	default:
		return v
	}
}
