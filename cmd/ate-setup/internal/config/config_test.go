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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// loadEnv isolates Load from the ambient environment.
//
// Load deliberately reads the developer's environment, so a test that sets only
// what it cares about is at the mercy of whatever the shell or CI job happens
// to export: an ambient PROJECT_ID or ATE_ATENET_DATAPLANE quietly changes the
// result. Every variable Load consults is blanked here -- empty reads as unset,
// which is what these tests mean by "not configured" -- and NO_DEV_ENV keeps
// .ate-dev-env.sh out of it. Tests then set back only what they exercise.
func loadEnv(t *testing.T) {
	t.Helper()
	t.Setenv("NO_DEV_ENV", "1")
	for _, name := range []string{
		"ANTHROPIC_API_KEY",
		"ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE",
		"ATE_API_POSTGRES_CLOUDSQL_GSA",
		"ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH",
		"ATE_API_POSTGRES_CLOUDSQL_IP_TYPE",
		"ATE_API_POSTGRES_CONNECTION_STRING",
		"ATE_API_POSTGRES_POOL_MAX_CONNS",
		"ATE_API_POSTGRES_SCHEMA",
		"ATE_API_POSTGRES_SERVER_CA_FILE",
		"ATE_ATENET_DATAPLANE",
		"ATE_CREDENTIAL_INJECTION_ENABLED",
		"ATE_CREDENTIAL_PROVIDER_ADDRESS",
		"ATE_CREDENTIAL_PROVIDER_NAME",
		"ATE_EXPERIMENTAL_USE_SDSMINT",
		"ATE_IMAGE_REPO",
		"ATE_IMAGE_TAG",
		"ATE_INSTALL_CLUSTER_SIZE",
		"ATE_INSTALL_CORDON_CONTROL_PLANE",
		"ATE_INSTALL_KIND",
		"ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER",
		"ATE_INSTALL_ROLLOUT_TIMEOUT",
		"ATE_OTLP_ENDPOINT",
		"BENCHMARK_ACTOR_MEMORY",
		"BUCKET_NAME",
		"CLUSTER_LOCATION",
		"CLUSTER_NAME",
		"EXPECTED_JWT_ISSUER",
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
	// Blanking this one would not read as unset: an exported but empty
	// instance is the explicit "remove Cloud SQL" request.
	unsetEnv(t, "ATE_API_POSTGRES_CLOUDSQL_INSTANCE")
}

// unsetEnv removes a variable for the duration of the test. t.Setenv first, so
// that its cleanup restores whatever the caller's environment had.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	if err := os.Unsetenv(name); err != nil {
		t.Fatalf("os.Unsetenv(%s) = %v", name, err)
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
	if cfg.ClusterSize != ClusterSizeSize0 {
		t.Errorf("ClusterSize = %q, want %q", cfg.ClusterSize, ClusterSizeSize0)
	}
	if cfg.CordonControlPlane {
		t.Error("CordonControlPlane = true, want false")
	}
}

// --cluster-size=size10 pins the apiserver's pool on the default connection
// string only. An explicit ATE_API_POSTGRES_CONNECTION_STRING names a database
// the installer did not size, so it is passed through as written.
func TestLoadClusterSize(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     Options
		env      map[string]string
		wantSize string
		wantDSN  string
	}{
		{
			name:     "flag",
			opts:     Options{ClusterSize: ClusterSizeSize10},
			wantSize: ClusterSizeSize10,
			wantDSN:  DefaultPostgresConnectionString + Size10PostgresPoolParams,
		},
		{
			name:     "environment",
			env:      map[string]string{"ATE_INSTALL_CLUSTER_SIZE": ClusterSizeSize10},
			wantSize: ClusterSizeSize10,
			wantDSN:  DefaultPostgresConnectionString + Size10PostgresPoolParams,
		},
		{
			name:     "flag beats the environment",
			opts:     Options{ClusterSize: ClusterSizeSize0},
			env:      map[string]string{"ATE_INSTALL_CLUSTER_SIZE": ClusterSizeSize10},
			wantSize: ClusterSizeSize0,
			wantDSN:  DefaultPostgresConnectionString,
		},
		{
			name:     "explicit connection string is untouched",
			opts:     Options{ClusterSize: ClusterSizeSize10},
			env:      map[string]string{"ATE_API_POSTGRES_CONNECTION_STRING": "postgresql://someone@db.example:5432/atepg"},
			wantSize: ClusterSizeSize10,
			wantDSN:  "postgresql://someone@db.example:5432/atepg",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			cfg, err := Load(tc.opts)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.ClusterSize != tc.wantSize {
				t.Errorf("ClusterSize = %q, want %q", cfg.ClusterSize, tc.wantSize)
			}
			if cfg.Size10() != (tc.wantSize == ClusterSizeSize10) {
				t.Errorf("Size10() = %v, want %v", cfg.Size10(), tc.wantSize == ClusterSizeSize10)
			}
			if got := cfg.PostgresConnString(); got != tc.wantDSN {
				t.Errorf("PostgresConnString() = %q, want %q", got, tc.wantDSN)
			}
			env := scriptEnvMap(t, cfg)
			if tc.wantSize == ClusterSizeSize10 {
				if env["ATE_INSTALL_CLUSTER_SIZE"] != ClusterSizeSize10 {
					t.Errorf("ScriptEnv()[ATE_INSTALL_CLUSTER_SIZE] = %q, want size10", env["ATE_INSTALL_CLUSTER_SIZE"])
				}
			} else if _, ok := env["ATE_INSTALL_CLUSTER_SIZE"]; ok {
				t.Errorf("ScriptEnv() exports ATE_INSTALL_CLUSTER_SIZE = %q for the default profile, want it absent", env["ATE_INSTALL_CLUSTER_SIZE"])
			}
		})
	}
}

func TestLoadCordonControlPlane(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
		env  string
		want bool
	}{
		{name: "flag", opts: Options{CordonControlPlane: true}, want: true},
		{name: "environment true", env: "true", want: true},
		{name: "environment false", env: "false", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			t.Setenv("ATE_INSTALL_CORDON_CONTROL_PLANE", tc.env)
			cfg, err := Load(tc.opts)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.CordonControlPlane != tc.want {
				t.Errorf("CordonControlPlane = %v, want %v", cfg.CordonControlPlane, tc.want)
			}
			_, exported := scriptEnvMap(t, cfg)["ATE_INSTALL_CORDON_CONTROL_PLANE"]
			if exported != tc.want {
				t.Errorf("ScriptEnv() exports ATE_INSTALL_CORDON_CONTROL_PLANE = %v, want %v", exported, tc.want)
			}
		})
	}
}

func TestLoadFlagsBeatEnvironment(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_ATENET_DATAPLANE", RouterEnvoy)
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

// ATE_API_POSTGRES_SCHEMA defaults to public, as in the shell installer, and
// an explicit value wins.
func TestLoadPostgresSchema(t *testing.T) {
	loadEnv(t)
	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresSchemaName() != DefaultPostgresSchema {
		t.Errorf("PostgresSchemaName() = %q, want %q", cfg.PostgresSchemaName(), DefaultPostgresSchema)
	}

	t.Setenv("ATE_API_POSTGRES_SCHEMA", "substrate")
	cfg, err = Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresSchemaName() != "substrate" {
		t.Errorf("PostgresSchemaName() = %q, want %q", cfg.PostgresSchemaName(), "substrate")
	}
}

// The apiserver reads its pool size and its server CA out of the DSN and a
// mounted file respectively, neither of which the shell installer synthesizes.
func TestLoadPostgresTuning(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_API_POSTGRES_POOL_MAX_CONNS", "50")
	t.Setenv("ATE_API_POSTGRES_SERVER_CA_FILE", "/etc/ssl/server-ca.pem")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresPoolMaxConns != "50" {
		t.Errorf("PostgresPoolMaxConns = %q, want 50", cfg.PostgresPoolMaxConns)
	}
	if want := "/etc/ssl/server-ca.pem"; cfg.PostgresServerCAFile != want {
		t.Errorf("PostgresServerCAFile = %q, want %q", cfg.PostgresServerCAFile, want)
	}
}

// ATE_API_POSTGRES_CLOUDSQL_INSTANCE is three-way, and Load is where the
// distinction is made: everything downstream sees only Instance and
// InstanceSet.
func TestLoadCloudSQL(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		loadEnv(t)
		cfg, err := Load(Options{})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.CloudSQL.InstanceSet {
			t.Errorf("CloudSQL = %+v, want InstanceSet false so the cluster's record is adopted", cfg.CloudSQL)
		}
	})

	t.Run("exported but empty removes Cloud SQL", func(t *testing.T) {
		loadEnv(t)
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_INSTANCE", "")
		cfg, err := Load(Options{})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg.CloudSQL.InstanceSet || cfg.CloudSQL.Instance != "" {
			t.Errorf("CloudSQL = %+v, want an explicitly empty instance", cfg.CloudSQL)
		}
	})

	t.Run("fully specified", func(t *testing.T) {
		loadEnv(t)
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_INSTANCE", "p:r:i")
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_GSA", "ate@p.iam.gserviceaccount.com")
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH", "false")
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_IP_TYPE", CloudSQLIPTypePSC)

		cfg, err := Load(Options{})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		want := CloudSQLConfig{
			Instance:    "p:r:i",
			InstanceSet: true,
			GSA:         "ate@p.iam.gserviceaccount.com",
			IAMAuth:     "false",
			IPType:      CloudSQLIPTypePSC,
		}
		if cfg.CloudSQL != want {
			t.Errorf("CloudSQL = %+v, want %+v", cfg.CloudSQL, want)
		}
	})

	// An unrecognized IP type reaches the proxy as an unset flag, which silently
	// dials the public address instead of the private one that was meant.
	t.Run("rejects an unknown IP type", func(t *testing.T) {
		loadEnv(t)
		t.Setenv("ATE_API_POSTGRES_CLOUDSQL_IP_TYPE", "internal")
		if _, err := Load(Options{}); err == nil || !strings.Contains(err.Error(), "ATE_API_POSTGRES_CLOUDSQL_IP_TYPE") {
			t.Fatalf("Load() error = %v, want it to name the invalid IP type", err)
		}
	})
}

// EXPECTED_JWT_ISSUER overrides the issuer derived from the GKE coordinates,
// which is how a cluster authenticating against something other than its own
// OIDC discovery document is installed.
func TestLoadExpectedJWTIssuer(t *testing.T) {
	loadEnv(t)
	const issuer = "https://issuer.example.com"
	t.Setenv("EXPECTED_JWT_ISSUER", issuer)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ExpectedJWTIssuer != issuer {
		t.Errorf("ExpectedJWTIssuer = %q, want %q", cfg.ExpectedJWTIssuer, issuer)
	}
}

// The endpoint has to reach both the Go steps and the shell scripts ate-setup
// still delegates to, or the two halves of an install export different
// collectors.
func TestLoadOtlpEndpoint(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_OTLP_ENDPOINT", "http://from-environment:4317")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "http://from-environment:4317"; cfg.OtlpEndpoint != want {
		t.Errorf("OtlpEndpoint = %q, want %q", cfg.OtlpEndpoint, want)
	}

	cfg, err = Load(Options{OtlpEndpoint: "http://from-flag:4317"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "http://from-flag:4317"; cfg.OtlpEndpoint != want {
		t.Errorf("OtlpEndpoint = %q, want %q", cfg.OtlpEndpoint, want)
	}
	if got := scriptEnvMap(t, cfg)["ATE_OTLP_ENDPOINT"]; got != "http://from-flag:4317" {
		t.Errorf("ScriptEnv()[ATE_OTLP_ENDPOINT] = %q, want the flag's value", got)
	}
}

// hack/install-ate-kind.sh exports ATE_INSTALL_KIND rather than passing a flag,
// so the environment has to select the Kind profile as completely as --kind
// does; a Kind install that only half-applied would push images to the wrong
// registry.
func TestLoadKindFromEnvironment(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_INSTALL_KIND", "true")
	t.Setenv("PROJECT_ID", "some-project")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !cfg.Kind {
		t.Error("Kind = false, want true")
	}
	if cfg.KODockerRepo != "localhost:5001" {
		t.Errorf("KODockerRepo = %q, want the Kind default", cfg.KODockerRepo)
	}
	if cfg.ProjectID != "" {
		t.Errorf("ProjectID = %q, want it cleared by the Kind profile", cfg.ProjectID)
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

// A developer who juggles clusters sets KUBECONFIG to a list. client-go's
// explicit path is one file, so forwarding the list makes every command fail
// with "stat /a.yaml:/b.yaml: no such file or directory"; the loading rules
// read and merge the list themselves when no explicit path is given.
func TestLoadKubeconfigListStaysWithTheLoadingRules(t *testing.T) {
	loadEnv(t)
	list := strings.Join([]string{"/home/dev/a.yaml", "/home/dev/b.yaml"}, string(os.PathListSeparator))
	t.Setenv("KUBECONFIG", list)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Kubeconfig != "" {
		t.Errorf("Kubeconfig = %q, want it empty so the client-go loading rules merge the list", cfg.Kubeconfig)
	}
	// kubectl understands the list, so the scripts still get it verbatim.
	if !slices.Contains(cfg.ScriptEnv(), "KUBECONFIG="+list) {
		t.Errorf("ScriptEnv() does not carry KUBECONFIG=%s: %v", list, cfg.ScriptEnv())
	}

	// An explicit --kubeconfig is one file by definition and still wins.
	cfg, err = Load(Options{Kubeconfig: "/tmp/explicit.yaml"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "/tmp/explicit.yaml"; cfg.Kubeconfig != want {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Kubeconfig, want)
	}
	if !slices.Contains(cfg.ScriptEnv(), "KUBECONFIG=/tmp/explicit.yaml") {
		t.Errorf("ScriptEnv() does not carry the --kubeconfig value: %v", cfg.ScriptEnv())
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
		// Both parse. Accepted, they would turn every wait into a single probe
		// that fails against a workload which has not started yet.
		{"zero rollout timeout", Options{RolloutTimeout: "0s"}},
		{"negative rollout timeout", Options{RolloutTimeout: "-30s"}},
		{"podcert workers", Options{PodcertWorkersPerSigner: -1}},
		{"cluster size", Options{ClusterSize: "size5"}},
		{"extproc missing sdsmint", Options{AdditionalEgressExtprocService: "ate-system/extproc:50051"}},
		{"extproc invalid format", Options{ExperimentalUseSDSMint: true, AdditionalEgressExtprocService: "extproc:50051"}},
		{"extproc agentgateway", Options{ExperimentalUseSDSMint: true, Router: RouterAgentgateway, AdditionalEgressExtprocService: "ate-system/extproc:50051"}},
		{"injection missing sdsmint", Options{ExperimentalEgressCredentialInjection: true}},
		{"injection agentgateway", Options{ExperimentalUseSDSMint: true, Router: RouterAgentgateway, ExperimentalEgressCredentialInjection: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(tc.opts); err == nil {
				t.Fatal("Load() succeeded, want an error")
			}
		})
	}
}

func TestLoadRejectsInvalidRolloutTimeoutEnv(t *testing.T) {
	loadEnv(t)
	for _, val := range []string{"0", "0s", "-1m", "forever"} {
		t.Run(val, func(t *testing.T) {
			t.Setenv("ATE_INSTALL_ROLLOUT_TIMEOUT", val)
			if _, err := Load(Options{}); err == nil {
				t.Fatalf("Load() with env %q succeeded, want an error", val)
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

// Developer env files print: they call gcloud, they announce the project they
// picked. That output must not land in the record stream, where it would be
// glued onto the first variable printed and lose it.
func TestSourceShellEnvIgnoresScriptOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.sh")
	script := "echo 'Using project demo-project'\n" +
		"printf 'no trailing newline either'\n" +
		"export AAA_FIRST=one\n" +
		"export PROJECT_ID=demo-project\n"
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	env, err := sourceShellEnv(path, dir)
	if err != nil {
		t.Fatalf("sourceShellEnv() error = %v", err)
	}
	if env["AAA_FIRST"] != "one" {
		t.Errorf("AAA_FIRST = %q, want one", env["AAA_FIRST"])
	}
	if env["PROJECT_ID"] != "demo-project" {
		t.Errorf("PROJECT_ID = %q, want demo-project", env["PROJECT_ID"])
	}
	for name := range env {
		if strings.Contains(name, "Using project") {
			t.Errorf("script output was parsed as a variable name: %q", name)
		}
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

func TestLoadImageSource(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		opts      Options
		want      images.Source
		wantError string
	}{
		{
			name: "unset builds from source",
			want: images.Source{},
		},
		{
			name: "flags",
			opts: Options{ImageRepo: "example.com/substrate", ImageTag: "v1.2.3"},
			want: images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"},
		},
		{
			name: "environment",
			env:  map[string]string{"ATE_IMAGE_REPO": "example.com/env", "ATE_IMAGE_TAG": "v9"},
			want: images.Source{Repo: "example.com/env", Tag: "v9"},
		},
		{
			name: "flags beat the environment",
			env:  map[string]string{"ATE_IMAGE_REPO": "example.com/env", "ATE_IMAGE_TAG": "v9"},
			opts: Options{ImageRepo: "example.com/flag", ImageTag: "v1"},
			want: images.Source{Repo: "example.com/flag", Tag: "v1"},
		},
		{
			// A repo pasted from a registry UI often carries one, and it would
			// otherwise produce a double slash in every reference.
			name: "trailing slash is trimmed",
			opts: Options{ImageRepo: "example.com/substrate/", ImageTag: "v1"},
			want: images.Source{Repo: "example.com/substrate", Tag: "v1"},
		},
		{
			name:      "a repo needs a tag",
			opts:      Options{ImageRepo: "example.com/substrate"},
			wantError: "--image-repo (or ATE_IMAGE_REPO) requires --image-tag",
		},
		{
			name:      "a tag needs a repo",
			opts:      Options{ImageTag: "v1"},
			wantError: "--image-tag (or ATE_IMAGE_TAG) requires --image-repo",
		},
		{
			// The environment reaches validation the same way the flags do.
			name:      "a tag from the environment needs a repo",
			env:       map[string]string{"ATE_IMAGE_TAG": "v1"},
			wantError: "--image-tag (or ATE_IMAGE_TAG) requires --image-repo",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}

			cfg, err := Load(tc.opts)
			if tc.wantError != "" {
				if err == nil {
					t.Fatalf("Load() = nil error, want one containing %q", tc.wantError)
				}
				if !strings.Contains(err.Error(), tc.wantError) {
					t.Errorf("Load() error = %v, want it to contain %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Images.Repo != tc.want.Repo {
				t.Errorf("Images.Repo = %q, want %q", cfg.Images.Repo, tc.want.Repo)
			}
			if cfg.Images.Tag != tc.want.Tag {
				t.Errorf("Images.Tag = %q, want %q", cfg.Images.Tag, tc.want.Tag)
			}
			if cfg.Images.IsPrebuilt() != (tc.want.Repo != "") {
				t.Errorf("Images.IsPrebuilt() = %v, want %v", cfg.Images.IsPrebuilt(), tc.want.Repo != "")
			}
		})
	}
}
