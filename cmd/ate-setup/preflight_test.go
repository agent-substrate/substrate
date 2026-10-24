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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/ate-setup/internal/cmd"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("git rev-parse --show-toplevel: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// Each invocation gets its own command tree and flags, including invocations
// made by the real shell shims. The test executable runs the installer without
// rebuilding it or contacting a real cluster.
func TestInstallerPreflightProcess(t *testing.T) {
	if os.Getenv("ATE_SETUP_PREFLIGHT_TEST") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			cmd.Root().SetArgs(os.Args[i+1:])
			cmd.Execute()
			os.Exit(0)
		}
	}
	t.Fatal("missing installer arguments")
}

func runPreflightInstaller(t *testing.T, kubeconfig, dockerStatus string, args ...string) (string, int) {
	t.Helper()
	bin := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for name, source := range map[string]string{
		"docker": "#!/bin/sh\necho called >> \"$PREFLIGHT_DOCKER_LOG\"\nexit \"$PREFLIGHT_DOCKER_STATUS\"\n",
		"go": `#!/bin/sh
if [ "$*" = "env GOARCH" ]; then
  echo amd64
elif [ "$1" = run ] && [ "$2" = ./cmd/ate-setup ]; then
  shift 2
  exec "$PREFLIGHT_EXECUTABLE" -test.run='^TestInstallerPreflightProcess$' -- "$@"
else
  echo "unexpected go invocation: $*" >&2
  exit 1
fi
`,
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(source), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	process := exec.CommandContext(ctx, executable, append([]string{"-test.run=^TestInstallerPreflightProcess$", "--"}, args...)...)
	process.Dir = repoRoot(t)
	process.Env = []string{
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"HOME=" + t.TempDir(),
		"KUBECONFIG=" + kubeconfig,
		"NO_DEV_ENV=true",
		"ATE_NO_REPORT_DEFAULTS=1",
		"ATE_SETUP_PREFLIGHT_TEST=1",
		"PREFLIGHT_EXECUTABLE=" + executable,
		"PREFLIGHT_DOCKER_STATUS=" + dockerStatus,
		"PREFLIGHT_DOCKER_LOG=" + filepath.Join(bin, "docker.log"),
	}
	output, err := process.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("installer did not fail promptly: %v\n%s", ctx.Err(), output)
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return string(output), exitErr.ExitCode()
	}
	if err != nil {
		t.Fatalf("running installer: %v\n%s", err, output)
	}
	return string(output), 0
}

func preflightKubeconfig(t *testing.T, server string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config")
	contents := "apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []\n"
	if server != "" {
		contents = fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: local
  cluster:
    server: %s
contexts:
- name: kind-kind
  context:
    cluster: local
    user: local
users:
- name: local
  user: {}
current-context: kind-kind
`, server)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestKindInstallerMissingContext(t *testing.T) {
	args := []string{"--kind", "deploy", "demo", "counter"}
	out, code := runPreflightInstaller(t, preflightKubeconfig(t, ""), "0", args...)
	if code == 0 || !strings.Contains(out, "create-kind-cluster.sh") || !strings.Contains(out, "export kubeconfig") {
		t.Fatalf("exit=%d, want missing-context recovery guidance\n%s", code, out)
	}
}

func TestKindInstallerDockerUnavailable(t *testing.T) {
	out, code := runPreflightInstaller(t, preflightKubeconfig(t, "http://127.0.0.1:1"), "1",
		"--kind", "deploy", "demo", "counter")
	if code == 0 || !strings.Contains(out, "Docker") || !strings.Contains(out, "docker info") {
		t.Fatalf("exit=%d, want Docker readiness guidance\n%s", code, out)
	}
}

// A source build of the envoy dataplane needs docker buildx. Without it the
// install has to stop before it touches the cluster, which here is an address
// nothing listens on.
func TestInstallerRequiresBuildxBeforeTouchingCluster(t *testing.T) {
	out, code := runPreflightInstaller(t, preflightKubeconfig(t, "http://127.0.0.1:1"), "1",
		"deploy", "ate-system",
		"--context=kind-kind",
		`--credential-provider={"name":"k8s.io"}`,
		"--ingress-allowed-spiffe-ids=spiffe://example.invalid/client",
		"--gcp-project-id=example-invalid",
		"--snapshot-transfer-gsa-name=ate-snapshot-transfer",
		"--snapshot-location=gs://example-invalid",
		"--ko-docker-repo=example.invalid/repo")
	if code == 0 || !strings.Contains(out, "docker buildx plugin") || !strings.Contains(out, "--image-repo") {
		t.Fatalf("exit=%d, want docker buildx guidance\n%s", code, out)
	}
	if strings.Contains(out, "127.0.0.1:1") {
		t.Fatalf("installer contacted the cluster before checking for docker buildx\n%s", out)
	}
}

func TestInstallerRequiresControlPlaneBeforeDemoOrBenchmark(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"native demo", []string{"deploy", "demo", "counter"}},
		{"native benchmark", []string{"deploy", "benchmarks"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"deployments.apps ate-controller not found"}`)
			}))
			defer server.Close()
			out, code := runPreflightInstaller(t, preflightKubeconfig(t, server.URL), "0", tc.args...)
			if code == 0 || !strings.Contains(out, "deploy ate-system") || !strings.Contains(out, "credential-provider") {
				t.Errorf("exit=%d, want control-plane install guidance\n%s", code, out)
			}
			mu.Lock()
			defer mu.Unlock()
			want := "GET /apis/apps/v1/namespaces/ate-system/deployments/ate-controller"
			if len(requests) != 1 || requests[0] != want {
				t.Errorf("requests=%v, want only %q before refusing deployment", requests, want)
			}
		})
	}
}

func TestKindInstallerHelpNeedsNoCluster(t *testing.T) {
	args := []string{"--kind", "deploy", "demo", "counter", "--help"}
	out, code := runPreflightInstaller(t, preflightKubeconfig(t, ""), "1", args...)
	if code != 0 || !strings.Contains(out, "Usage:") {
		t.Fatalf("exit=%d, want help without prerequisites\n%s", code, out)
	}
}

func TestInstallerControlPlaneCheckPreservesErrorsAndAllowsInstalledSystem(t *testing.T) {
	for _, installed := range []bool{false, true} {
		t.Run(fmt.Sprintf("installed=%t", installed), func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				requests = append(requests, r.Method+" "+r.URL.Path)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				if installed && strings.HasSuffix(r.URL.Path, "/deployments/ate-controller") {
					_, _ = fmt.Fprint(w, `{"kind":"Deployment","apiVersion":"apps/v1","metadata":{"name":"ate-controller","namespace":"ate-system"}}`)
					return
				}
				w.WriteHeader(http.StatusForbidden)
				_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"fixture access denied"}`)
			}))
			defer server.Close()
			out, code := runPreflightInstaller(t, preflightKubeconfig(t, server.URL), "0", "deploy", "demo", "counter")
			want := "fixture access denied"
			if installed {
				want = "demo-counter_deploy"
			}
			if code == 0 || !strings.Contains(out, want) || strings.Contains(out, "Deploy the control plane first") {
				t.Errorf("exit=%d, want original API error without missing-install guidance\n%s", code, out)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(requests) == 0 || requests[0] != "GET /apis/apps/v1/namespaces/ate-system/deployments/ate-controller" {
				t.Fatalf("requests=%v, want control-plane check first", requests)
			}
			if installed && len(requests) < 2 {
				t.Errorf("requests=%v, want demo to proceed after the control-plane check", requests)
			}
			if !installed && len(requests) != 1 {
				t.Errorf("requests=%v, want permission error to stop the install", requests)
			}
			for _, request := range requests {
				if !strings.HasPrefix(request, "GET ") {
					t.Errorf("unexpected mutation: %s", request)
				}
			}
		})
	}
}

func TestKindInstallerUsesResolvedContextAndClusterName(t *testing.T) {
	document := filepath.Join(t.TempDir(), "install.yaml")
	if err := os.WriteFile(document, []byte("apiVersion: install.ate.dev/v1alpha1\nkind: SubstrateInstall\ncontext: selected-by-config\nkindCluster:\n  name: local-dev\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, code := runPreflightInstaller(t, preflightKubeconfig(t, "http://127.0.0.1:1"), "0",
		"--kind", "--config", document, "deploy", "demo", "counter")
	for _, want := range []string{`context "selected-by-config"`, `export kubeconfig --name="local-dev"`, `KIND_CLUSTER_NAME="local-dev"`, "replaces an existing cluster"} {
		if code == 0 || !strings.Contains(out, want) {
			t.Errorf("exit=%d, want %q in resolved-context guidance\n%s", code, want, out)
		}
	}
}

func TestKindInstallerMissingOrMalformedKubeconfig(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "config")
		want := "create-kind-cluster.sh"
		if malformed {
			if err := os.WriteFile(path, []byte("contexts: [\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			want = "while reading kubeconfig"
		}
		out, code := runPreflightInstaller(t, path, "0", "--kind", "deploy", "demo", "counter")
		if code == 0 || !strings.Contains(out, want) {
			t.Errorf("malformed=%t exit=%d, want %q\n%s", malformed, code, want, out)
		}
		if malformed && strings.Contains(out, "create-kind-cluster.sh") {
			t.Errorf("malformed kubeconfig was misreported as a missing cluster\n%s", out)
		}
	}
}
