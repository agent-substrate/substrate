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

// Package config resolves the environment ate-setup installs into. It layers
// the developer's .ate-dev-env.sh (sourced through bash, so existing setups
// keep working), the ambient process environment, and command line flags into
// a single typed Config.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// Enumerated values for the install-shaping flags.
const (
	RouterEnvoy        = "envoy"
	RouterAgentgateway = "agentgateway"

	SandboxClassGvisor  = "gvisor"
	SandboxClassMicrovm = "microvm"
)

// DefaultRolloutTimeout is the default wait timeout for workload rollouts.
const DefaultRolloutTimeout = 60 * time.Second

// DefaultPostgresConnectionString mirrors default_postgres_connection_string in
// the shell installer: the apiserver reaches PostgreSQL over mTLS using the
// podcertificate controller's projected servicedns trust bundle and its own
// podidentity credential bundle.
const DefaultPostgresConnectionString = "postgresql://postgres@postgres.ate-system.svc:5432/atepg?sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem&sslcert=/run/podidentity.podcert.ate.dev/credential-bundle.pem&sslkey=/run/podidentity.podcert.ate.dev/credential-bundle.pem"

// DefaultPostgresSchema mirrors the shell installer's default for
// ATE_API_POSTGRES_SCHEMA, the PostgreSQL schema holding the Substrate tables.
const DefaultPostgresSchema = "public"

// devEnvFile is the optional per-developer environment script at the repo root.
const devEnvFile = ".ate-dev-env.sh"

// Config is the fully resolved installation environment. Fields sourced from
// the developer environment keep their shell names in the comments so the
// mapping back to .ate-dev-env.sh stays obvious.
type Config struct {
	// Root is the repository root. All manifest paths are relative to it.
	Root string

	// Kind selects the local Kind install profile (ATE_INSTALL_KIND).
	Kind bool

	// Kubeconfig and Context select the target cluster. Empty Context means
	// "use the current context" (the KUBECTL_CONTEXT convention).
	//
	// Kubeconfig falls back to $KUBECONFIG rather than staying empty. The
	// client-go loading rules consult that variable on their own, so leaving
	// it out here would point ate-setup at one cluster while the shell scripts
	// it delegates to, which are handed this value through ScriptEnv, used
	// another.
	Kubeconfig string
	Context    string

	// GKE cluster coordinates, used to fetch credentials and to derive the
	// service account JWT issuer.
	ProjectID       string
	ClusterName     string
	ClusterLocation string

	// BucketName is the snapshot bucket demos are templated with.
	BucketName string

	// KODockerRepo is where ko pushes images (KO_DOCKER_REPO).
	KODockerRepo string
	// KODefaultPlatforms constrains ko's build platforms.
	KODefaultPlatforms string

	// Images selects where container images come from. Its zero value builds
	// them from source with ko, which is what a developer install does.
	Images images.Source

	// Router selects the atenet router dataplane.
	Router string
	// PostgresConnectionString is the apiserver's store connection string.
	// Empty means use DefaultPostgresConnectionString.
	PostgresConnectionString string
	// PostgresSchema is the PostgreSQL schema for the Substrate tables
	// (ATE_API_POSTGRES_SCHEMA). Empty means DefaultPostgresSchema.
	PostgresSchema string

	// RolloutTimeout is the timeout duration for rollout status checks.
	RolloutTimeout time.Duration
	// rolloutTimeoutSet records whether RolloutTimeout was asked for rather
	// than defaulted. See WaitTimeout.
	rolloutTimeoutSet bool

	// PodcertWorkersPerSigner overrides WORKERS_PER_SIGNER on podcertificate-controller.
	PodcertWorkersPerSigner int

	// ExperimentalUseSDSMint enables per-SNI dynamic cert minting on atenet-egress.
	ExperimentalUseSDSMint bool

	// AdditionalEgressExtprocService is the optional NS/SVC:PORT external processor filter.
	AdditionalEgressExtprocService string

	// AnthropicAPIKey is required only by the claude-code-multiplex demo.
	AnthropicAPIKey string

	// OtlpEndpoint is where the control plane ships telemetry
	// (ATE_OTLP_ENDPOINT). Benchmark actors are pointed at it too.
	OtlpEndpoint string
	// BenchmarkActorMemory is the memory limit for benchmark actors
	// (BENCHMARK_ACTOR_MEMORY). Empty leaves the workload default in place.
	BenchmarkActorMemory string

	// shellEnv is the process environment layered over .ate-dev-env.sh. It is
	// kept so that the shell scripts ate-setup still shells out to see the same
	// variables the shell installer would have exported to them.
	shellEnv map[string]string
}

// Options carries the raw flag values the root command collects, before
// defaulting and validation.
type Options struct {
	Kind                           bool
	Kubeconfig                     string
	Context                        string
	Router                         string
	RolloutTimeout                 string
	PodcertWorkersPerSigner        int
	ExperimentalUseSDSMint         bool
	AdditionalEgressExtprocService string

	// Image source selection.
	ImageRepo string
	ImageTag  string

	// NoDevEnv skips sourcing .ate-dev-env.sh even when it exists.
	NoDevEnv bool
}

// Load resolves the effective configuration. Precedence, lowest to highest:
// .ate-dev-env.sh, the process environment, then flags.
func Load(opts Options) (*Config, error) {
	root, err := RepoRoot()
	if err != nil {
		return nil, err
	}

	env := environ()

	// Sourcing is skipped for Kind installs the same way the shell kind installer
	// exports NO_DEV_ENV: the GKE-shaped variables in a developer's file would
	// otherwise point a local install at a cloud project.
	if !opts.NoDevEnv && !opts.Kind && os.Getenv("NO_DEV_ENV") == "" {
		path := filepath.Join(root, devEnvFile)
		if _, statErr := os.Stat(path); statErr == nil {
			sourced, srcErr := sourceShellEnv(path, root)
			if srcErr != nil {
				return nil, fmt.Errorf("while sourcing %s: %w", devEnvFile, srcErr)
			}
			// The process environment still wins: an explicitly exported
			// variable is a deliberate override of the file.
			for k, v := range sourced {
				if _, ok := env[k]; !ok {
					env[k] = v
				}
			}
		}
	}

	timeoutStr := firstNonEmpty(opts.RolloutTimeout, env["ATE_INSTALL_ROLLOUT_TIMEOUT"])
	rolloutTimeout := DefaultRolloutTimeout
	if timeoutStr != "" {
		d, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid --rollout-timeout %q (must be a duration like 60s, 5m): %w", timeoutStr, err)
		}
		rolloutTimeout = d
	}

	podcertWorkers := opts.PodcertWorkersPerSigner
	if podcertWorkers == 0 && env["ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER"] != "" {
		val := env["ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER"]
		w, err := strconv.Atoi(val)
		if err != nil || w < 1 {
			return nil, fmt.Errorf("--podcert-workers-per-signer must be a positive integer, got %q", val)
		}
		podcertWorkers = w
	}

	sdsmint := opts.ExperimentalUseSDSMint || env["ATE_EXPERIMENTAL_USE_SDSMINT"] == "true"
	extproc := firstNonEmpty(opts.AdditionalEgressExtprocService, env["ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE"])

	cfg := &Config{
		Root:                           root,
		Kind:                           opts.Kind,
		Kubeconfig:                     firstNonEmpty(opts.Kubeconfig, env["KUBECONFIG"]),
		Context:                        firstNonEmpty(opts.Context, env["KUBECTL_CONTEXT"]),
		ProjectID:                      env["PROJECT_ID"],
		ClusterName:                    env["CLUSTER_NAME"],
		ClusterLocation:                env["CLUSTER_LOCATION"],
		BucketName:                     env["BUCKET_NAME"],
		KODockerRepo:                   env["KO_DOCKER_REPO"],
		KODefaultPlatforms:             env["KO_DEFAULTPLATFORMS"],
		Images:                         loadImageSource(opts, env),
		PostgresConnectionString:       env["ATE_API_POSTGRES_CONNECTION_STRING"],
		PostgresSchema:                 env["ATE_API_POSTGRES_SCHEMA"],
		RolloutTimeout:                 rolloutTimeout,
		rolloutTimeoutSet:              timeoutStr != "",
		PodcertWorkersPerSigner:        podcertWorkers,
		ExperimentalUseSDSMint:         sdsmint,
		AdditionalEgressExtprocService: extproc,
		AnthropicAPIKey:                env["ANTHROPIC_API_KEY"],
		OtlpEndpoint:                   env["ATE_OTLP_ENDPOINT"],
		BenchmarkActorMemory:           env["BENCHMARK_ACTOR_MEMORY"],
		shellEnv:                       env,
	}

	if opts.Kind {
		applyKindDefaults(cfg)
	}

	cfg.Router = firstNonEmpty(opts.Router, env["ATE_ATENET_ROUTER"], RouterEnvoy)

	if err := validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// loadImageSource resolves where images come from.
func loadImageSource(opts Options, env map[string]string) images.Source {
	return images.Source{
		Repo: strings.TrimSuffix(firstNonEmpty(opts.ImageRepo, env["ATE_IMAGE_REPO"]), "/"),
		Tag:  firstNonEmpty(opts.ImageTag, env["ATE_IMAGE_TAG"]),
	}
}

func applyKindDefaults(cfg *Config) {
	cfg.ProjectID = ""
	cfg.ClusterLocation = ""
	kindClusterName := firstNonEmpty(cfg.shellEnv["KIND_CLUSTER_NAME"], "kind")
	cfg.Context = firstNonEmpty(cfg.Context, "kind-"+kindClusterName)
	cfg.KODockerRepo = firstNonEmpty(cfg.KODockerRepo, "localhost:5001")
	cfg.KODefaultPlatforms = "linux/" + runtime.GOARCH
	cfg.BucketName = "ate-snapshots"
}

func validate(cfg *Config) error {
	if err := cfg.Images.Validate(); err != nil {
		return err
	}
	switch cfg.Router {
	case RouterEnvoy, RouterAgentgateway:
	default:
		return fmt.Errorf("atenet router must be %s or %s, got %q", RouterEnvoy, RouterAgentgateway, cfg.Router)
	}
	if cfg.PodcertWorkersPerSigner < 0 {
		return fmt.Errorf("--podcert-workers-per-signer must be a positive integer, got %d", cfg.PodcertWorkersPerSigner)
	}
	if cfg.AdditionalEgressExtprocService != "" {
		if err := validateExtprocService(cfg.AdditionalEgressExtprocService); err != nil {
			return err
		}
		if !cfg.ExperimentalUseSDSMint {
			return fmt.Errorf("--experimental-additional-egress-extproc-service requires --experimental-use-sdsmint")
		}
		if cfg.Router != RouterEnvoy {
			return fmt.Errorf("--experimental-additional-egress-extproc-service requires --atenet-router=envoy")
		}
	}
	return nil
}

func validateExtprocService(spec string) error {
	parts := strings.Split(spec, "/")
	if len(parts) != 2 {
		return fmt.Errorf("--experimental-additional-egress-extproc-service must be <namespace>/<service>:<port>, got %q", spec)
	}
	namespace := parts[0]
	svcPort := strings.Split(parts[1], ":")
	if len(svcPort) != 2 {
		return fmt.Errorf("--experimental-additional-egress-extproc-service must be <namespace>/<service>:<port>, got %q", spec)
	}
	service := svcPort[0]
	portStr := svcPort[1]
	if namespace == "" || service == "" {
		return fmt.Errorf("--experimental-additional-egress-extproc-service must be <namespace>/<service>:<port>, got %q", spec)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("--experimental-additional-egress-extproc-service port must be 1-65535, got %q", portStr)
	}
	return nil
}

// PostgresConnString returns the configured connection string, falling back to
// the in-cluster default.
func (c *Config) PostgresConnString() string {
	if c.PostgresConnectionString != "" {
		return c.PostgresConnectionString
	}
	return DefaultPostgresConnectionString
}

// PostgresSchemaName returns the configured schema, falling back to the
// shell installer's default. ate-api-server rejects an empty value.
func (c *Config) PostgresSchemaName() string {
	if c.PostgresSchema != "" {
		return c.PostgresSchema
	}
	return DefaultPostgresSchema
}

// WaitTimeout returns how long to wait for a workload whose historical timeout
// was historical.
//
// The shell installer applied --rollout-timeout to the ate-system rollouts
// only; the podcertificate-controller wait and the CSI driver waits were fixed
// at 120s because they are the slow bootstrap paths — an image pull on a cold
// cluster, in the CSI case a driver being torn down and reinstalled. Honoring
// the flag everywhere would let its 60s default halve exactly those waits.
//
// So the flag now reaches every wait, but only when someone asked for it. Left
// alone, each site keeps the timeout the scripts gave it.
func (c *Config) WaitTimeout(historical time.Duration) time.Duration {
	if c.rolloutTimeoutSet {
		return c.RolloutTimeout
	}
	return historical
}

// Manifest resolves a path under manifests/ate-install.
func (c *Config) Manifest(parts ...string) string {
	return filepath.Join(append([]string{c.Root, "manifests", "ate-install"}, parts...)...)
}

// Path resolves a repo-relative path.
func (c *Config) Path(parts ...string) string {
	return filepath.Join(append([]string{c.Root}, parts...)...)
}

// kindUnsetVars are the GKE-specific variables the shell kind installer unset
// before delegating, so that a developer who had already sourced
// .ate-dev-env.sh into their shell did not end up pointing a local install at a
// cloud project.
var kindUnsetVars = []string{
	"GCE_REGION", "CLUSTER_LOCATION", "NETWORK", "SUBNETWORK",
	"MEMORYSTORE_INSTANCE", "PROJECT_ID",
}

// ScriptEnv returns the environment for the shell scripts ate-setup still
// delegates to (the benchmark and micro-VM helpers).
//
// Those scripts read the same variables the shell installer exported to them,
// so this reproduces that environment: .ate-dev-env.sh under the process
// environment, with the resolved configuration layered on top so that flags
// such as --context and --kind reach them.
func (c *Config) ScriptEnv() []string {
	merged := make(map[string]string, len(c.shellEnv)+len(kindUnsetVars)+12)
	for k, v := range c.shellEnv {
		merged[k] = v
	}

	if c.Kind {
		for _, name := range kindUnsetVars {
			delete(merged, name)
		}
		merged["ATE_INSTALL_KIND"] = "true"
		merged["NO_DEV_ENV"] = "true"
	}

	// The resolved values win: they already account for flags, the dev env,
	// and the kind profile.
	for name, value := range map[string]string{
		"KUBECTL_CONTEXT":     c.Context,
		"KUBECONFIG":          c.Kubeconfig,
		"BUCKET_NAME":         c.BucketName,
		"KO_DOCKER_REPO":      c.KODockerRepo,
		"KO_DEFAULTPLATFORMS": c.KODefaultPlatforms,
		"PROJECT_ID":          c.ProjectID,
		"CLUSTER_NAME":        c.ClusterName,
		"CLUSTER_LOCATION":    c.ClusterLocation,
	} {
		if value == "" {
			// An empty value means "not configured". Leaving the variable set
			// but empty would defeat the ${VAR:-default} fallbacks the scripts
			// rely on.
			delete(merged, name)
			continue
		}
		merged[name] = value
	}

	if c.RolloutTimeout > 0 {
		merged["ATE_INSTALL_ROLLOUT_TIMEOUT"] = c.RolloutTimeout.String()
	}
	if c.PodcertWorkersPerSigner > 0 {
		merged["ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER"] = strconv.Itoa(c.PodcertWorkersPerSigner)
	}
	if c.ExperimentalUseSDSMint {
		merged["ATE_EXPERIMENTAL_USE_SDSMINT"] = "true"
	}
	if c.AdditionalEgressExtprocService != "" {
		merged["ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE"] = c.AdditionalEgressExtprocService
	}

	env := make([]string, 0, len(merged))
	for k, v := range merged {
		env = append(env, k+"="+v)
	}
	sort.Strings(env)
	return env
}

// KoEnv returns the environment overrides ko needs for this configuration.
func (c *Config) KoEnv() []string {
	var env []string
	if c.KODockerRepo != "" {
		env = append(env, "KO_DOCKER_REPO="+c.KODockerRepo)
	}
	if c.KODefaultPlatforms != "" {
		env = append(env, "KO_DEFAULTPLATFORMS="+c.KODefaultPlatforms)
	}
	return env
}

func environ() map[string]string {
	env := make(map[string]string)
	for _, kv := range os.Environ() {
		if name, value, ok := strings.Cut(kv, "="); ok {
			env[name] = value
		}
	}
	return env
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
