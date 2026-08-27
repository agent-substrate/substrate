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

package config

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// loadEnv isolates Load from the ambient environment.
//
// Load deliberately reads the developer's environment, so a test that sets only
// what it cares about is at the mercy of whatever the shell or CI job happens
// to export: an ambient PROJECT_ID or ATE_ATENET_ROUTER quietly changes the
// result. Every variable Load consults is blanked here -- empty reads as unset,
// which is what these tests mean by "not configured" -- and NO_DEV_ENV keeps
// .ate-dev-env.sh out of it. Tests then set back only what they exercise.
func loadEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NO_DEV_ENV", "1")
	for _, name := range []string{
		"ANTHROPIC_API_KEY",
		"ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE",
		"ATE_API_POSTGRES_CONNECTION_STRING",
		"ATE_ATENET_ROUTER",
		"ATE_EXPERIMENTAL_USE_SDSMINT",
		"ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER",
		"ATE_INSTALL_ROLLOUT_TIMEOUT",
		"ATE_OTLP_ENDPOINT",
		"BENCHMARK_ACTOR_MEMORY",
		"BUCKET_NAME",
		"CLUSTER_LOCATION",
		"CLUSTER_NAME",
		"KIND_CLUSTER_NAME",
		"KO_DEFAULTPLATFORMS",
		"KO_DOCKER_REPO",
		"KUBECONFIG",
		"KUBECTL_CONTEXT",
		"MEMORYSTORE_INSTANCE",
		"PROJECT_ID",
	} {
		t.Setenv(name, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	loadEnv(t)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Router != RouterEnvoy {
		t.Errorf("Router = %q, want %q", cfg.Router, RouterEnvoy)
	}
	if cfg.PostgresConnString() != DefaultPostgresConnectionString {
		t.Errorf("PostgresConnString() = %q, want %q", cfg.PostgresConnString(), DefaultPostgresConnectionString)
	}
	if cfg.RolloutTimeout != DefaultRolloutTimeout {
		t.Errorf("RolloutTimeout = %v, want %v", cfg.RolloutTimeout, DefaultRolloutTimeout)
	}
}

func TestLoadFlagsBeatEnvironment(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_ATENET_ROUTER", RouterEnvoy)
	t.Setenv("ATE_INSTALL_ROLLOUT_TIMEOUT", "30s")

	cfg, err := Load(Options{Router: RouterAgentgateway, RolloutTimeout: "120s"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Router != RouterAgentgateway {
		t.Errorf("Router = %q, want %q", cfg.Router, RouterAgentgateway)
	}
	if want := 120 * time.Second; cfg.RolloutTimeout != want {
		t.Errorf("RolloutTimeout = %v, want %v", cfg.RolloutTimeout, want)
	}
}

// ATE_API_POSTGRES_CONNECTION_STRING is how a developer points the apiserver at
// their own database, the same override the shell installer honored.
func TestLoadPostgresConnectionStringOverride(t *testing.T) {
	loadEnv(t)
	const dsn = "postgresql://someone@db.example:5432/atepg?sslmode=disable"
	t.Setenv("ATE_API_POSTGRES_CONNECTION_STRING", dsn)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresConnString() != dsn {
		t.Errorf("PostgresConnString() = %q, want %q", cfg.PostgresConnString(), dsn)
	}
}

// ate-setup's own client resolves $KUBECONFIG through the client-go loading
// rules, so the value has to reach ScriptEnv as well. Otherwise a developer who
// exports KUBECONFIG without passing --kubeconfig gets an install split across
// two clusters: ate-setup writes to the exported one while the shell scripts it
// delegates to fall back to ~/.kube/config.
func TestLoadKubeconfigFallsBackToEnvironment(t *testing.T) {
	loadEnv(t)
	t.Setenv("KUBECONFIG", "/home/dev/clusters/dev.yaml")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "/home/dev/clusters/dev.yaml"; cfg.Kubeconfig != want {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Kubeconfig, want)
	}
	if !slices.Contains(cfg.ScriptEnv(), "KUBECONFIG=/home/dev/clusters/dev.yaml") {
		t.Errorf("ScriptEnv() does not carry KUBECONFIG: %v", cfg.ScriptEnv())
	}

	// The flag still wins over the environment.
	cfg, err = Load(Options{Kubeconfig: "/tmp/explicit.yaml"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "/tmp/explicit.yaml"; cfg.Kubeconfig != want {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Kubeconfig, want)
	}
}

// The shell installer left the podcertificate-controller and CSI waits at 120s
// and pointed --rollout-timeout only at the ate-system rollouts. WaitTimeout
// lets the flag reach every wait without its 60s default shortening the slow
// bootstrap ones.
func TestWaitTimeout(t *testing.T) {
	loadEnv(t)
	const historical = 120 * time.Second

	for _, tc := range []struct {
		name string
		opts Options
		env  string
		want time.Duration
	}{
		{"default leaves the historical timeout alone", Options{}, "", historical},
		{"a longer flag raises it", Options{RolloutTimeout: "5m"}, "", 5 * time.Minute},
		{"a shorter flag lowers it", Options{RolloutTimeout: "30s"}, "", 30 * time.Second},
		{"the environment counts as asking too", Options{}, "10m", 10 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ATE_INSTALL_ROLLOUT_TIMEOUT", tc.env)

			cfg, err := Load(tc.opts)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got := cfg.WaitTimeout(historical); got != tc.want {
				t.Errorf("WaitTimeout(%v) = %v, want %v", historical, got, tc.want)
			}
		})
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	loadEnv(t)

	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"router", Options{Router: "nginx"}},
		{"rollout timeout", Options{RolloutTimeout: "invalid"}},
		{"podcert workers", Options{PodcertWorkersPerSigner: -1}},
		{"extproc missing sdsmint", Options{AdditionalEgressExtprocService: "ate-system/extproc:50051"}},
		{"extproc invalid format", Options{ExperimentalUseSDSMint: true, AdditionalEgressExtprocService: "extproc:50051"}},
		{"extproc agentgateway", Options{ExperimentalUseSDSMint: true, Router: RouterAgentgateway, AdditionalEgressExtprocService: "ate-system/extproc:50051"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(tc.opts); err == nil {
				t.Fatal("Load() succeeded, want an error")
			}
		})
	}
}

func TestLoadRejectsInvalidPodcertWorkersEnv(t *testing.T) {
	loadEnv(t)
	for _, val := range []string{"0", "-1", "abc"} {
		t.Run(val, func(t *testing.T) {
			t.Setenv("ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER", val)
			if _, err := Load(Options{}); err == nil {
				t.Fatalf("Load() with env %q succeeded, want an error", val)
			}
		})
	}
}

// --kind must reproduce the exports in the shell kind installer, including
// clearing the GKE coordinates and isolating the snapshot bucket name.
func TestLoadKindProfile(t *testing.T) {
	loadEnv(t)
	t.Setenv("PROJECT_ID", "some-gke-project")
	t.Setenv("CLUSTER_LOCATION", "us-central1-c")
	t.Setenv("BUCKET_NAME", "ambient-cloud-bucket")

	cfg, err := Load(Options{Kind: true})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Context != "kind-kind" {
		t.Errorf("Context = %q, want kind-kind", cfg.Context)
	}
	if cfg.ProjectID != "" {
		t.Errorf("ProjectID = %q, want it cleared for kind", cfg.ProjectID)
	}
	if cfg.ClusterLocation != "" {
		t.Errorf("ClusterLocation = %q, want it cleared for kind", cfg.ClusterLocation)
	}
	if cfg.KODockerRepo != "localhost:5001" {
		t.Errorf("KODockerRepo = %q, want localhost:5001", cfg.KODockerRepo)
	}
	if want := "linux/" + runtime.GOARCH; cfg.KODefaultPlatforms != want {
		t.Errorf("KODefaultPlatforms = %q, want %q", cfg.KODefaultPlatforms, want)
	}
	if cfg.BucketName != "ate-snapshots" {
		t.Errorf("BucketName = %q, want ate-snapshots (must not inherit ambient)", cfg.BucketName)
	}
}

// scriptEnvMap turns ScriptEnv's KEY=VALUE slice back into a map. Absence and
// emptiness are distinct: the shell scripts use ${VAR:-default} fallbacks, so a
// variable that is set but empty behaves differently from an unset one.
func scriptEnvMap(t *testing.T, cfg *Config) map[string]string {
	t.Helper()
	env := make(map[string]string)
	for _, kv := range cfg.ScriptEnv() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("ScriptEnv() entry %q is not KEY=VALUE", kv)
		}
		env[name] = value
	}
	return env
}

func TestScriptEnvCarriesResolvedConfig(t *testing.T) {
	loadEnv(t)
	t.Setenv("BUCKET_NAME", "from-environment")

	cfg, err := Load(Options{Context: "gke_demo", Kubeconfig: "/tmp/kubeconfig"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	env := scriptEnvMap(t, cfg)
	for name, want := range map[string]string{
		"KUBECTL_CONTEXT": "gke_demo",
		"KUBECONFIG":      "/tmp/kubeconfig",
		"BUCKET_NAME":     "from-environment",
	} {
		if env[name] != want {
			t.Errorf("ScriptEnv()[%s] = %q, want %q", name, env[name], want)
		}
	}
	// Nothing configured a project, so the variable must not be exported at
	// all rather than exported empty.
	if _, ok := env["PROJECT_ID"]; ok {
		t.Errorf("ScriptEnv() exports PROJECT_ID = %q, want it absent", env["PROJECT_ID"])
	}
}

func TestScriptEnvKindProfile(t *testing.T) {
	loadEnv(t)
	t.Setenv("PROJECT_ID", "some-gke-project")
	t.Setenv("CLUSTER_LOCATION", "us-central1-c")
	t.Setenv("MEMORYSTORE_INSTANCE", "some-instance")

	cfg, err := Load(Options{Kind: true})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	env := scriptEnvMap(t, cfg)
	for _, name := range []string{"PROJECT_ID", "CLUSTER_LOCATION", "MEMORYSTORE_INSTANCE"} {
		if _, ok := env[name]; ok {
			t.Errorf("ScriptEnv() exports %s = %q for a kind install, want it absent", name, env[name])
		}
	}
	if env["ATE_INSTALL_KIND"] != "true" {
		t.Errorf("ScriptEnv()[ATE_INSTALL_KIND] = %q, want true", env["ATE_INSTALL_KIND"])
	}
	if env["KO_DOCKER_REPO"] != "localhost:5001" {
		t.Errorf("ScriptEnv()[KO_DOCKER_REPO] = %q, want localhost:5001", env["KO_DOCKER_REPO"])
	}
}

func TestSourceShellEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.sh")
	script := "export PROJECT_ID=demo-project\n" +
		"export BUCKET_NAME=snapshots-${PROJECT_ID}\n"
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	env, err := sourceShellEnv(path, dir)
	if err != nil {
		t.Fatalf("sourceShellEnv() error = %v", err)
	}
	if env["PROJECT_ID"] != "demo-project" {
		t.Errorf("PROJECT_ID = %q, want demo-project", env["PROJECT_ID"])
	}
	// Shell interpolation has to keep working: developer files build values
	// out of each other and out of ${USER}.
	if env["BUCKET_NAME"] != "snapshots-demo-project" {
		t.Errorf("BUCKET_NAME = %q, want snapshots-demo-project", env["BUCKET_NAME"])
	}
}

func TestSourceShellEnvReportsFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.sh")
	if err := os.WriteFile(path, []byte("exit 3\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := sourceShellEnv(path, dir); err == nil {
		t.Fatal("sourceShellEnv() succeeded, want an error")
	}
}
