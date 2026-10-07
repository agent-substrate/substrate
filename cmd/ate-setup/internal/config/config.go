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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/pkg/postgressetup"
)

// Enumerated values for the install-shaping flags.
const (
	RouterEnvoy        = "envoy"
	RouterAgentgateway = "agentgateway"

	SandboxClassGvisor  = "gvisor"
	SandboxClassMicrovm = "microvm"

	// Cluster size profiles. size0 is the shipped footprint; size10 assumes a
	// dedicated node for PostgreSQL and raises the store, its client pool, and
	// the podcertificate controller's API rate limits to match.
	ClusterSizeSize0  = "size0"
	ClusterSizeSize10 = "size10"
)

// DefaultRolloutTimeout is the default wait timeout for workload rollouts.
const DefaultRolloutTimeout = 60 * time.Second

// Size10PostgresPoolParams is appended to the bundled read/write connection on
// size10 clusters. pgxpool defaults MaxConns to max(4, runtime.NumCPU()), which
// under-uses the size10 server's raised max_connections; pinning the pool makes
// the client side open the sockets the server is provisioned for.
const Size10PostgresPoolParams = "&pool_max_conns=64&pool_min_conns=4"

const (
	DefaultPostgresSchema        = postgressetup.Schema
	DefaultPostgresReadWriteRole = postgressetup.ReadWriteRole
	DefaultPostgresOwnerRole     = postgressetup.OwnerRole
)

// Cloud SQL Auth Proxy IP types, the values ATE_API_POSTGRES_CLOUDSQL_IP_TYPE
// accepts.
const (
	CloudSQLIPTypePrivate = "private"
	CloudSQLIPTypePublic  = "public"
	CloudSQLIPTypePSC     = "psc"
)

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

	// Namespace is the namespace the control plane is installed into, from
	// ATE_NAMESPACE. It defaults to the canonical installdefaults.SystemNamespace,
	// so an install that does not set it is unaffected. The checked-in manifests
	// under manifests/ate-install/ name that namespace literally, so the
	// manifest-applying steps refuse any other value; see Env.RequireCanonicalNamespace.
	Namespace string

	// Kubeconfig and Context select the target cluster. Empty Context means
	// "use the current context" (the KUBECTL_CONTEXT convention).
	//
	// Kubeconfig is a single file, handed to client-go as its explicit path. It
	// falls back to $KUBECONFIG rather than staying empty so that ate-setup and
	// the shell scripts it delegates to agree on the cluster — but only when
	// that variable names one file. See loadKubeconfig.
	Kubeconfig string
	Context    string

	// GKE cluster coordinates, used to fetch credentials and to derive the
	// service account JWT issuer.
	ProjectID       string
	ClusterName     string
	ClusterLocation string

	// ExpectedJWTIssuer is the service account token issuer ate-api-server
	// trusts (EXPECTED_JWT_ISSUER). It overrides both the GKE derivation from
	// the coordinates above and OpenID discovery, for clusters whose issuer
	// follows neither form.
	ExpectedJWTIssuer string

	// ActorJWTAlgorithm is the signing algorithm of the key in a new actor JWT
	// pool (ACTOR_JWT_ALGORITHM): ES256 or RS256.
	ActorJWTAlgorithm string

	// BucketName is the snapshot bucket demos are templated with.
	BucketName string

	// KODockerRepo is where ko pushes images (KO_DOCKER_REPO).
	KODockerRepo string
	// KODefaultPlatforms constrains ko's build platforms.
	KODefaultPlatforms string
	// DockerBuildFlags are extra docker buildx build flags for the images ko
	// cannot build (DOCKER_BUILD_FLAGS, whitespace-separated).
	DockerBuildFlags []string

	// Images selects where container images come from. Its zero value builds
	// them from source with ko, which is what a developer install does.
	Images images.Source

	// Router selects the atenet router dataplane.
	Router string
	// The read/write and owner connections can use different login identities.
	// With one configured connection, both pools use it. Both empty selects
	// bundled PostgreSQL.
	PostgresReadWriteConnectionString string
	PostgresOwnerConnectionString     string
	PostgresReadWriteRole             string
	PostgresOwnerRole                 string
	// These distinguish an explicit role override from the default when
	// adopting an existing Cloud SQL installation.
	PostgresReadWriteRoleSet bool
	PostgresOwnerRoleSet     bool
	// PostgresSchema is the PostgreSQL schema for the Substrate tables
	// (ATE_API_POSTGRES_SCHEMA). Empty means DefaultPostgresSchema.
	PostgresSchema string
	// PostgresPoolMaxConns sizes the apiserver's read/write pool
	// (ATE_API_POSTGRES_POOL_MAX_CONNS). Empty leaves the DSN or pgxpool default.
	PostgresPoolMaxConns string
	// PostgresServerCAFile is a local PEM file holding the server CA of an
	// external PostgreSQL (ATE_API_POSTGRES_SERVER_CA_FILE). Its contents are
	// published as the postgres-server-ca Secret, which ate-api-server mounts
	// at /run/postgres-server-ca/server-ca.pem for sslmode=verify-ca DSNs.
	PostgresServerCAFile string
	// CloudSQL points the apiserver at a Cloud SQL instance through the Auth
	// Proxy sidecar instead of a directly reachable PostgreSQL.
	CloudSQL CloudSQLConfig

	// RolloutTimeout is the timeout duration for rollout status checks.
	RolloutTimeout time.Duration
	// rolloutTimeoutSet records whether RolloutTimeout was asked for rather
	// than defaulted. See WaitTimeout.
	rolloutTimeoutSet bool

	// PodcertWorkersPerSigner overrides WORKERS_PER_SIGNER on podcertificate-controller.
	PodcertWorkersPerSigner int

	// ClusterSize is the footprint profile (ATE_INSTALL_CLUSTER_SIZE): size0
	// or size10.
	ClusterSize string

	// CordonControlPlane keeps the control plane off the worker nodes
	// (ATE_INSTALL_CORDON_CONTROL_PLANE). It assumes a small shared pool
	// labeled and tainted ate.dev/workloadType=ate-control-plane:NoSchedule,
	// and a one-node pool labeled and tainted
	// ate.dev/workloadType=ate-postgres:NoSchedule for postgres alone.
	CordonControlPlane bool

	// AdditionalEgressExtprocService is the optional NS/SVC:PORT external processor filter.
	AdditionalEgressExtprocService string

	// CredentialProviderJSON is the --credential-provider value; see
	// CredentialProvider for its schema.
	CredentialProviderJSON string

	// AnthropicAPIKey is required only by the claude-code-multiplex demo.
	AnthropicAPIKey string

	// OtlpEndpoint is where the control plane ships telemetry
	// (ATE_OTLP_ENDPOINT). Benchmark actors are pointed at it too.
	OtlpEndpoint string
	// BenchmarkActorMemory is the memory limit for benchmark actors
	// (BENCHMARK_ACTOR_MEMORY). Empty leaves the workload default in place.
	BenchmarkActorMemory string
	// EnvoyConcurrency overrides the Envoy egress gateway worker thread count
	// (E2E_ENVOY_CONCURRENCY). Empty leaves Envoy's default concurrency in place.
	EnvoyConcurrency string

	// kubeconfigEnv is what ScriptEnv exports as $KUBECONFIG. Unlike Kubeconfig
	// it may be a PATH-style list of files, which kubectl understands and
	// client-go's explicit path does not.
	kubeconfigEnv string

	// shellEnv is the process environment layered over .ate-dev-env.sh. It is
	// kept so that the shell scripts ate-setup still shells out to see the same
	// variables the shell installer would have exported to them.
	shellEnv map[string]string

	// resolved is every setting with the channel that supplied it. Commands
	// read their own scoped settings from it; Config itself carries only the
	// settings the installer resolves for every command.
	resolved *Resolved
}

// Resolved returns every setting and its origin. Command-scoped settings are
// not fields on Config: a command reads its own by key.
//
// A Config built directly rather than by Load has resolved nothing, which is
// what tests do. Reporting declared defaults keeps every lookup working
// instead of panicking on a nil map.
func (c *Config) Resolved() *Resolved {
	if c.resolved == nil {
		c.resolved = defaultsOnly()
	}
	return c.resolved
}

// SetResolved attaches resolved settings to a Config built directly. Load
// does this itself; the callers here are tests and the demo test harness.
func (c *Config) SetResolved(r *Resolved) { c.resolved = r }

// defaultsOnly is every setting on its declared default. Computed on first use
// rather than at init: settings register from command packages, whose init
// runs after this one.
func defaultsOnly() *Resolved {
	r, err := Resolve(nil, ResolveOptions{})
	if err != nil {
		// Unreachable: TestRegistryDefaultsParse covers every declared default.
		panic("config: declared defaults do not parse: " + err.Error())
	}
	return r
}

// CloudSQLConfig is the operator's Cloud SQL intent, as expressed by the
// ATE_API_POSTGRES_CLOUDSQL_* variables.
//
// Every field is empty-means-unspecified except Instance, which is three-way:
// a non-empty instance selects Cloud SQL, an explicitly empty one removes it,
// and an unset one (InstanceSet false) adopts whatever the target cluster
// already records. Without that distinction a redeploy from a shell that
// simply never exported the variable would tear the proxy sidecar out from
// under a working installation.
type CloudSQLConfig struct {
	// Instance is the instance connection name, PROJECT:REGION:INSTANCE.
	Instance string
	// InstanceSet records whether ATE_API_POSTGRES_CLOUDSQL_INSTANCE was
	// present in the environment at all, empty value included.
	InstanceSet bool

	// GSA is the Google service account the proxy authenticates as, and whose
	// email (minus the .gserviceaccount.com suffix) is the IAM database user.
	GSA string
	// IAMAuth enables automatic IAM database authentication ("true" or
	// "false"). Empty defaults to enabled.
	IAMAuth string
	// IPType selects which instance address the proxy dials: one of the
	// CloudSQLIPType constants. Empty defaults to private.
	IPType string
}

// extprocServiceForm and extprocServicePort describe the accepted value, for
// the two ways it can be wrong.
const (
	extprocServiceForm = "<namespace>/<service>:<port>"
	extprocServicePort = extprocServiceForm + ", with a port of 1-65535"
)

// validateExtprocService checks the service reference. It takes the resolved
// value rather than the bare string so the error names the channel that
// supplied it: the setting reaches a flag, an environment variable and a
// configuration file, and naming only the flag sends a reader who used one of
// the other two to the wrong place.
func validateExtprocService(v Value) error {
	spec := v.Raw
	parts := strings.Split(spec, "/")
	if len(parts) != 2 {
		return &InvalidError{Value: v, Want: extprocServiceForm}
	}
	namespace := parts[0]
	svcPort := strings.Split(parts[1], ":")
	if len(svcPort) != 2 {
		return &InvalidError{Value: v, Want: extprocServiceForm}
	}
	service := svcPort[0]
	portStr := svcPort[1]
	if namespace == "" || service == "" {
		return &InvalidError{Value: v, Want: extprocServiceForm}
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return &InvalidError{Value: v, Want: extprocServicePort}
	}
	return nil
}

// Size10 reports whether the size10 footprint profile is selected.
func (c *Config) Size10() bool {
	return c.ClusterSize == ClusterSizeSize10
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
		"KUBECONFIG":          c.kubeconfigEnv,
		"BUCKET_NAME":         c.BucketName,
		"KO_DOCKER_REPO":      c.KODockerRepo,
		"KO_DEFAULTPLATFORMS": c.KODefaultPlatforms,
		"PROJECT_ID":          c.ProjectID,
		"CLUSTER_NAME":        c.ClusterName,
		"CLUSTER_LOCATION":    c.ClusterLocation,
		"ATE_OTLP_ENDPOINT":   c.OtlpEndpoint,
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
	// Resolved values again, so a flag overrides whatever the environment
	// carried rather than layering under it.
	delete(merged, "ATE_INSTALL_CLUSTER_SIZE")
	if c.ClusterSize != "" && c.ClusterSize != ClusterSizeSize0 {
		merged["ATE_INSTALL_CLUSTER_SIZE"] = c.ClusterSize
	}
	delete(merged, "ATE_INSTALL_CORDON_CONTROL_PLANE")
	if c.CordonControlPlane {
		merged["ATE_INSTALL_CORDON_CONTROL_PLANE"] = "true"
	}
	if c.AdditionalEgressExtprocService != "" {
		merged["ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE"] = c.AdditionalEgressExtprocService
	}
	if c.CredentialProviderJSON != "" {
		merged["ATE_CREDENTIAL_PROVIDER"] = c.CredentialProviderJSON
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

// The bundled Kubernetes Secrets provider, which the installer deploys with a
// NetworkPolicy that admits only the egress gateway. Its default address is
// the Service in
// manifests/egress-credential-injection/k8s-credential-provider.yaml.
const (
	K8sCredentialProviderName    = "k8s.io"
	K8sCredentialProviderAddress = "k8s-credential-provider." + installdefaults.SystemNamespace + ".svc:50051"
)

// credentialProviderKey is the setting the provider JSON arrives on.
const credentialProviderKey = "atenet.egress.credentialProvider"

// credentialProviderUsage lists the accepted --credential-provider values.
const credentialProviderUsage = `{"name":"k8s.io"} for the bundled Kubernetes Secrets provider, ` +
	`{"enabled":false} to turn egress credential injection off, ` +
	`or {"name":"<provider>","address":"<host>:<port>"} for a provider you deploy`

// credentialProviderSpec is the schema of the --credential-provider JSON.
type credentialProviderSpec struct {
	// Enabled defaults to true when absent.
	Enabled *bool  `json:"enabled"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

// CredentialProvider is the credential provider the egress gateway is pointed
// at. The zero value means injection is off.
type CredentialProvider struct {
	// Name is the host of the ate-secret:// credential URIs the provider
	// serves, e.g. k8s.io.
	Name string
	// Address is the host:port the gateway dials the provider at.
	Address string
}

// Enabled reports whether the gateway is given a provider.
func (p CredentialProvider) Enabled() bool { return p.Name != "" }

// Kubernetes reports whether the provider is the bundled one.
func (p CredentialProvider) Kubernetes() bool { return p.Name == K8sCredentialProviderName }

// ServerName is the SAN the gateway expects on the provider's serving
// certificate: the address without its port.
func (p CredentialProvider) ServerName() string {
	if i := strings.LastIndex(p.Address, ":"); i >= 0 {
		return p.Address[:i]
	}
	return p.Address
}

// CredentialProvider parses --credential-provider, a JSON object that either
// turns injection off with enabled false or names a provider and its address.
// Only the bundled provider has a default address. A missing value is reported here rather than in validate
// because only the deploys that render the egress gateway need it.
func (c *Config) CredentialProvider() (CredentialProvider, error) {
	raw := c.CredentialProviderJSON
	if raw == "" {
		// Every channel is named, not just the flag: a provider set in the
		// configuration document is the documented way to do it, and a reader
		// told only about the flag goes looking in the wrong place.
		return CredentialProvider{}, fmt.Errorf("a credential provider is required; set one of %s: %s",
			channelsFor(credentialProviderKey), credentialProviderUsage)
	}
	invalid := func(format string, args ...any) (CredentialProvider, error) {
		return CredentialProvider{}, fmt.Errorf("invalid --credential-provider '%s': %s", raw, fmt.Sprintf(format, args...))
	}

	var spec credentialProviderSpec
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return invalid("%v; want %s", err, credentialProviderUsage)
	}
	if _, err := dec.Token(); err != io.EOF {
		return invalid("want a single JSON object")
	}

	if spec.Enabled != nil && !*spec.Enabled {
		if spec.Name != "" || spec.Address != "" {
			return invalid("enabled false takes no name or address")
		}
		return CredentialProvider{}, nil
	}
	if spec.Name == "" {
		return invalid("name is required; want %s", credentialProviderUsage)
	}
	// The name is the host of the credential URIs the provider serves.
	if errs := validation.IsDNS1123Subdomain(spec.Name); len(errs) > 0 {
		return invalid("name %q is not a valid DNS name: %s", spec.Name, strings.Join(errs, "; "))
	}
	if spec.Address == "" {
		if spec.Name != K8sCredentialProviderName {
			return invalid("a provider you deploy needs an address as host:port")
		}
		spec.Address = K8sCredentialProviderAddress
	} else if host, port, err := net.SplitHostPort(spec.Address); err != nil || host == "" || port == "" {
		return invalid("address must be host:port, got %q", spec.Address)
	}
	return CredentialProvider{Name: spec.Name, Address: spec.Address}, nil
}
